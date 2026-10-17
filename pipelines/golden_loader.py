"""
D：中间态 → golden（目前只有日线：不复权价 + 复权因子）

三步，都可以重复执行（没有变化就不写）：
    1. load_prices(batch_id)：一个不复权日线批次的中间态 parquet（health / warning）+ 人工放行的隔离行 → golden.price_daily
         · 代码 → security_id：和补数据用同一套对应（backfill.build_universe）
         · 跳过 config/golden_skip.csv 里存续期待核对的证券
         · 按证券存续期截断：首个交易日之前、最后交易日之后的行不写（代码被别的公司重复使用时，数据会串到别家）
         · 和 golden 当前版本比对：新的写 version 1；数值或 quality_status 变了写新版本；没变跳过
    2. build_adjustments()：事件库的拆股、现金分红 → golden.adjustment_events（复权因子）
         · 拆股：price_factor = 1 / 比例；现金分红：price_factor = 1 − 每股金额 / 除息前一天不复权收盘价
         · 分拆（只来自人工修正，事件库没有这类事件）：和现金分红同一算法，value = 每股分到的价值
         · 只算落在这只证券 golden 日线区间里的事件（区间外的事件不影响任何已存价格）
         · 去重（改过代码的证券新旧代码各拉过一遍）→ 人工修正（config/adjustment_overrides.csv）
         · 事件修订（revision 变了）或因子变了写新版本；事件没了（被删 / 人工删掉）写 is_active = 0 的版本撤回
    3. check_against_fmp()：复权后的每日收益率 vs FMP 复权价的每日收益率，相差 > 2% 记问题（GD001 v2，warning）
         按收益率比：漏一个事件只影响事件那一天的收益率；按价格水平比会让之前几千天都算偏差

命令行：
    python -m pipelines.golden_loader all            # 还没写进 golden 的不复权日线批次 → golden，再算复权因子、核对
    python -m pipelines.golden_loader prices <batch_id>
    python -m pipelines.golden_loader adjustments
    python -m pipelines.golden_loader check
"""
import argparse
import csv
import logging
import os
import re
from datetime import date

import numpy as np
import pandas as pd

from config.paths import BASE_DIR
from db import ClickHouseStore, MetaStore
from db.object_store import IntermediateStore
from pipelines.backfill import build_universe

log = logging.getLogger(__name__)

GOLDEN_DB = "golden"
PRICE_ENDPOINT = "price_volume_unadjusted"
SKIP_PATH = BASE_DIR / "config" / "golden_skip.csv"
OVERRIDES_PATH = BASE_DIR / "config" / "adjustment_overrides.csv"
SOURCE_TO_GOLDEN = {"adjOpen": "open", "adjHigh": "high", "adjLow": "low", "adjClose": "close", "volume": "volume"}
VALUE_COLS = list(SOURCE_TO_GOLDEN.values())
CHANGE_REL = 1e-6                  # 相对变化超过它算"有变化"（基本等于有变化就算）
FMP_TOLERANCE = 0.02               # 每日收益率和 FMP 复权价算出的收益率差超过 2% 记问题

# v2：改为按每日收益率比（v1 按价格水平比：漏一个事件，之前几千天都算偏差；22.9 万天里真正影响收益率的只有约 120 天）
GD001 = {"rule_id": "GD001", "rule_version": 2, "endpoint": "golden_price_daily", "severity": "warning",
         "description": "复权后的每日收益率与 FMP 复权价的每日收益率相差 > 2%（通常是这一天的拆股 / 分红 / 分拆事件有问题）；"
                        "两边重叠的最后一天不比（两次拉取时间不同，当天数据可能还在更新）",
         "params": {"tolerance": FMP_TOLERANCE}, "impl": "golden_loader.check_against_fmp"}


def load_skip(path=None) -> dict[str, str]:
    path = path or SKIP_PATH
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8-sig") as f:      # 兼容带 BOM（Excel 打开不乱码）
        return {r["symbol"].strip().upper(): r["reason"] for r in csv.DictReader(f)}


def read_config_csv(path) -> list[dict]:
    """
    读人工配置 CSV（UTF-8，可带 BOM）。读不了时给出能看懂的原因：
    最常见的是在 Excel 里打开后直接保存——Excel 会按 GBK 读、存回去时中文变成 ?，日期改成 28/04/2011
    """
    try:
        with open(path, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
    except UnicodeDecodeError as e:
        raise ValueError(f"{path} 不是 UTF-8 编码（{e.reason}）：可能在 Excel 里打开后被保存过。"
                         f"请用 git 或备份恢复；以后用 VS Code 编辑，或在 Excel 里另存为「CSV UTF-8」") from None
    bad_dates = [r for r in rows if r.get("ex_date") and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", r["ex_date"].strip())]
    if bad_dates:
        raise ValueError(f"{path} 有日期不是 YYYY-MM-DD 格式（如 {bad_dates[0]['ex_date']}）：可能被 Excel 改过格式")
    return rows


def load_overrides(path=None) -> list[dict]:
    """人工修正：action = drop（删除事件）/ set_value（改金额或比例）/ add（补事件）"""
    path = path or OVERRIDES_PATH
    if not os.path.exists(path):
        return []
    rows = read_config_csv(path)
    for r in rows:
        if r["action"] not in ("drop", "set_value", "add") or r["kind"] not in ("split", "dividend", "spinoff"):
            raise ValueError(f"adjustment_overrides.csv 有无法识别的行：{r}")
        r["symbol"] = r["symbol"].strip().upper()
        r["ex_date"] = pd.Timestamp(r["ex_date"])
        r["value"] = float(r["value"]) if r.get("value") else None
    return rows


def apply_overrides(ev: pd.DataFrame, overrides: list[dict], security_of: dict[str, int]) -> tuple[pd.DataFrame, dict]:
    """按人工修正删除 / 改值 / 补事件；找不到对应事件的修正记日志（可能事件库已经改正了，这条修正可以删掉）"""
    stats = {"dropped": 0, "set_value": 0, "added": 0, "unmatched": 0}
    for o in overrides:
        sid = security_of.get(o["symbol"])
        hit = (ev["security_id"] == sid) & (ev["kind"] == o["kind"]) & (ev["ex_date"] == o["ex_date"])
        if o["action"] == "add" and sid is not None:
            ev = pd.concat([ev, pd.DataFrame([{
                "event_id": 0, "security_id": sid, "ex_date": o["ex_date"], "value": o["value"], "event_revision": 0,
                "known_at": pd.Timestamp(o["decided_on"], tz="UTC"), "kind": o["kind"]}])], ignore_index=True)
            stats["added"] += 1
        elif sid is None or not hit.any():
            stats["unmatched"] += 1
            log.warning(f"[golden] 人工修正找不到对应事件：{o['action']} {o['symbol']} {o['kind']} {o['ex_date'].date()}")
        elif o["action"] == "drop":
            ev = ev[~hit]
            stats["dropped"] += int(hit.sum())
        else:
            ev.loc[hit, "value"] = o["value"]
            stats["set_value"] += int(hit.sum())
    return ev, stats


def _changed(new: pd.DataFrame, cur: pd.DataFrame) -> pd.Series:
    """新旧两组价格（列对齐）是否有变化：任一数值列相对变化 > CHANGE_REL、空值状态不同，或 quality_status 不同"""
    out = new["quality_status"].values != cur["quality_status"].values
    for c in VALUE_COLS:
        a, b = new[c].astype(float).values, cur[c].astype(float).values
        both = ~np.isnan(a) & ~np.isnan(b)
        diff = np.zeros(len(a), dtype=bool)
        diff[both] = np.abs(a[both] - b[both]) > CHANGE_REL * np.maximum(np.abs(a[both]), np.abs(b[both]))
        out |= diff | (np.isnan(a) != np.isnan(b))
    return pd.Series(out, index=new.index)


# =========================
# 1. 日线
# =========================
def load_prices(batch_id: str, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
                intermediate: IntermediateStore | None = None, db: str = GOLDEN_DB, today: date | None = None) -> dict:
    """today：代码 → 证券对应用哪一天的代码表（每日流程传交易日；不传用今天）"""
    store, ch, intermediate = store or MetaStore(), ch or ClickHouseStore(), intermediate or IntermediateStore()
    ing = store.get_ingestion(batch_id)
    if ing is None or ing["endpoint"] != PRICE_ENDPOINT:
        raise ValueError(f"批次 {batch_id} 不是 {PRICE_ENDPOINT} 的批次（目前 golden 只接不复权日线）")
    loads = [l for l in store.get_loads(batch_id) if l["layer"] == "intermediate" and l["status"] == "success"]
    if not loads:
        raise ValueError(f"批次 {batch_id} 还没有清洗出中间态")

    df = intermediate.read(loads[-1]["location"])
    released = [r for r in store.pending_outbox(PRICE_ENDPOINT, limit=100000) if r["batch_id"] == batch_id]
    if released:
        rel = pd.DataFrame([r["record"] for r in released])
        rel["quality_status"] = "released"
        df = pd.concat([df, rel], ignore_index=True)
    df = df.rename(columns=SOURCE_TO_GOLDEN)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    stats = {"rows_in": len(df)}

    # 代码 → 证券；跳过名单
    universe = build_universe(store.conn, today=today)
    skip = load_skip()
    df["security_id"] = df["symbol"].map(universe.security_of)
    stats["unmapped"] = int(df["security_id"].isna().sum())
    stats["skipped"] = int(df["symbol"].isin(list(skip)).sum())
    df = df[df["security_id"].notna() & ~df["symbol"].isin(list(skip))].copy()
    df["security_id"] = df["security_id"].astype("uint64")

    # 存续期截断
    sids = sorted(df["security_id"].unique().tolist())
    life = pd.DataFrame(store.conn.execute(
        "SELECT security_id, first_trade_date, last_trade_date FROM ref.securities WHERE security_id = ANY(%s)",
        ([int(s) for s in sids],)).fetchall(), columns=["security_id", "first", "last"])
    life["security_id"] = life["security_id"].astype("uint64")
    df = df.merge(life, on="security_id", how="left")
    inside = (df["first"].isna() | (df["date"] >= df["first"])) & (df["last"].isna() | (df["date"] <= df["last"]))
    stats["outside_life"] = int((~inside).sum())
    df = df[inside].drop(columns=["first", "last"])

    # 和 golden 当前版本比对
    empty_cur = pd.DataFrame(columns=["security_id", "date", *VALUE_COLS, "quality_status", "version"])
    cur = empty_cur if df.empty else ch.query_df(
        f"SELECT security_id, date, {', '.join(VALUE_COLS)}, quality_status, version "
        f"FROM {db}.v_price_daily WHERE security_id IN {{ids:Array(UInt64)}}", {"ids": sids})
    if cur.empty:                                   # 空结果不带列名
        cur = empty_cur
    cur = cur.astype({"security_id": "uint64"})
    cur["date"] = pd.to_datetime(cur["date"]).dt.date
    m = df.merge(cur, on=["security_id", "date"], how="left", suffixes=("", "_cur"))
    is_new = m["version"].isna()
    cur_side = m[[f"{c}_cur" for c in VALUE_COLS] + ["quality_status_cur"]].rename(columns=lambda c: c[:-4])
    changed = ~is_new & _changed(m, cur_side)
    out = m[is_new | changed].copy()
    out["version"] = (pd.to_numeric(out["version"]).fillna(0) + 1).astype("uint32")      # 新的从 1 开始，变了的 +1
    stats.update(new=int(is_new.sum()), changed=int(changed.sum()), unchanged=int((~is_new & ~changed).sum()))

    out = out.assign(source="fmp", source_symbol=out["symbol"], batch_id=batch_id,
                     source_logic_version=out["source_logic_version"].fillna(1).astype("uint16"))
    cols = ["security_id", "date", *VALUE_COLS, "quality_status", "source", "source_symbol", "batch_id",
            "source_logic_version", "version"]
    with store.load_step(batch_id, "golden", f"{db}.price_daily") as step:
        step.rows = ch.insert_df(f"{db}.price_daily", out[cols]) if not out.empty else 0
    if released:
        store.mark_written([r["quarantine_id"] for r in released])
    stats["written"] = len(out)
    log.info(f"[golden] 批次 {batch_id}：{stats}")
    return stats


# =========================
# 2. 复权因子
# =========================
def build_adjustments(*, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
                      db: str = GOLDEN_DB, today: date | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    today = today or date.today()
    # on_symbol：事件来源代码是不是除权日当天这只证券正在用的代码（去重时优先保留）
    cols = ["event_id", "security_id", "ex_date", "value", "event_revision", "known_at", "on_symbol"]
    splits = pd.DataFrame(store.conn.execute("""
        SELECT event_id, security_id, effective_date, numerator::float8 / denominator::float8, revision, known_at,
               ref.resolve_security('symbol', symbol, effective_date) IS NOT DISTINCT FROM security_id
        FROM events.v_splits_current
        WHERE security_id IS NOT NULL AND split_type <> 'adjustment' AND numerator > 0 AND denominator > 0""").fetchall(),
        columns=cols).assign(kind="split")
    divs = pd.DataFrame(store.conn.execute("""
        SELECT event_id, security_id, ex_date, amount::float8, revision, known_at,
               ref.resolve_security('symbol', symbol, ex_date) IS NOT DISTINCT FROM security_id
        FROM events.v_dividends_current WHERE security_id IS NOT NULL AND amount > 0""").fetchall(),
        columns=cols).assign(kind="dividend")
    ev = pd.concat([splits, divs], ignore_index=True)
    ev["ex_date"] = pd.to_datetime(ev["ex_date"])
    ev = ev[ev["ex_date"].dt.date <= today]
    stats = {"events_in": len(ev)}

    # 去重：改过代码的证券，新旧代码各拉过一遍全部历史（FMP 按现在的代码给历史），同一笔分红 / 拆股会出现两次
    #   同一证券、同一天、同一类事件只留一条：优先当天正在用的代码，其次事件编号小的
    #   （实测同一天同一代码的常规 + 特别分红为 0 例，所以按天去重不会误删）
    ev = ev.sort_values(["security_id", "kind", "ex_date", "on_symbol", "event_id"], ascending=[True, True, True, False, True])
    dup = ev.duplicated(["security_id", "kind", "ex_date"])
    stats["duplicates_dropped"] = int(dup.sum())
    ev = ev[~dup].drop(columns="on_symbol")
    ev["security_id"] = ev["security_id"].astype("uint64")
    ev, ostats = apply_overrides(ev, load_overrides(), build_universe(store.conn).security_of)
    stats["overrides"] = ostats

    px = ch.query_df(f"SELECT security_id, date, close FROM {db}.v_price_daily WHERE close > 0 ORDER BY date")
    if px.empty:
        return {**stats, "written": 0, "note": "golden 里还没有日线"}
    px["date"] = pd.to_datetime(px["date"])
    rng = px.groupby("security_id")["date"].agg(["min", "max"])
    ev["security_id"] = ev["security_id"].astype("uint64")
    ev = ev.join(rng, on="security_id", how="inner")
    ev = ev[(ev["ex_date"] > ev["min"]) & (ev["ex_date"] <= ev["max"])].drop(columns=["min", "max"])   # 只算影响已存价格的
    stats["in_price_range"] = len(ev)

    # 除权前一个交易日的不复权收盘价
    ev = pd.merge_asof(ev.sort_values("ex_date"), px.rename(columns={"date": "prev_date", "close": "prev_close"}),
                       left_on="ex_date", right_on="prev_date", by="security_id",
                       direction="backward", allow_exact_matches=False)
    # 现金分红和分拆同一算法：分拆就是把子公司股票发给股东，value = 每股分到的价值
    is_split = ev["kind"] == "split"
    ev["price_factor"] = np.where(is_split, 1 / ev["value"], 1 - ev["value"] / ev["prev_close"])
    ev["volume_factor"] = np.where(is_split, ev["value"], 1.0)
    # 因子必须 > 0（缺前收时是 NaN，也算无效）；分红 / 分拆因子还必须 < 1（金额 >= 前收说明数据有错）
    bad = ~(ev["price_factor"] > 0) | ((~is_split) & ~(ev["price_factor"] < 1))
    stats["bad_factor"] = int(bad.sum())
    if bad.any():
        log.warning(f"[golden] {int(bad.sum())} 个事件因子无效（分红金额 >= 前收或缺前收），已跳过："
                    f"{ev.loc[bad, ['security_id', 'ex_date', 'kind', 'value', 'prev_close']].head(5).to_dict('records')}")
    ev = ev[~bad]

    # 和 golden 当前版本比对（含已撤回的）：新的写 v1；修订 / 因子变了 / 重新出现的写新版本；
    # golden 里有效、但这次已经没有的（事件被删、被人工修正删掉）写一个 is_active = 0 的新版本撤回
    key = ["security_id", "ex_date", "kind", "event_id"]
    cur = ch.query_df(f"""
        SELECT security_id, ex_date, kind, event_id, max(version) AS v,
               argMax(value, version) AS value_cur, argMax(prev_close, version) AS prev_close_cur,
               argMax(price_factor, version) AS pf_cur, argMax(volume_factor, version) AS vf_cur,
               argMax(event_revision, version) AS rev_cur, argMax(known_at, version) AS known_at_cur,
               argMax(is_active, version) AS active_cur
        FROM {db}.adjustment_events GROUP BY security_id, ex_date, kind, event_id""")
    if cur.empty:
        cur = pd.DataFrame(columns=key + ["v", "value_cur", "prev_close_cur", "pf_cur", "vf_cur", "rev_cur",
                                          "known_at_cur", "active_cur"])
    cur = cur.astype({"security_id": "uint64", "event_id": "uint64"})
    cur["ex_date"] = pd.to_datetime(cur["ex_date"])
    ev["event_id"] = ev["event_id"].astype("uint64")

    m = ev.merge(cur, on=key, how="left")
    is_new = m["v"].isna()
    changed = ~is_new & ((m["active_cur"] != 1) | (m["event_revision"] != m["rev_cur"])
                         | (np.abs(m["price_factor"] - m["pf_cur"]) > 1e-9))
    out = m[is_new | changed].copy()
    out["version"] = (pd.to_numeric(out["v"]).fillna(0) + 1).astype("uint32")
    out["is_active"] = 1

    gone = cur[cur["active_cur"] == 1].merge(ev[key], on=key, how="left", indicator=True)
    gone = gone[gone["_merge"] == "left_only"]
    retract = pd.DataFrame({
        **{k: gone[k] for k in key}, "value": gone["value_cur"], "prev_close": gone["prev_close_cur"],
        "price_factor": gone["pf_cur"], "volume_factor": gone["vf_cur"], "event_revision": gone["rev_cur"],
        "known_at": gone["known_at_cur"], "version": pd.to_numeric(gone["v"]) + 1, "is_active": 0})
    stats.update(new=int(is_new.sum()), changed=int(changed.sum()), retracted=len(retract))

    cols = ["security_id", "ex_date", "kind", "value", "prev_close", "price_factor", "volume_factor", "is_active",
            "event_id", "event_revision", "known_at", "version"]
    write = pd.concat([out[cols], retract[cols]], ignore_index=True) if len(retract) else out[cols]
    if not write.empty:
        write = write.astype({"prev_close": float, "event_id": "uint64", "event_revision": "uint32",
                              "is_active": "uint8", "version": "uint32"})
        ch.insert_df(f"{db}.adjustment_events", write)
    stats["written"] = len(write)
    log.info(f"[golden] 复权因子：{stats}")
    return stats


# =========================
# 3. 和 FMP 复权价核对
# =========================
def check_against_fmp(*, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
                      db: str = GOLDEN_DB, raw_table: str = "raw.price_volume_daily") -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    store.sync_rules([GD001])
    # 每次都是全量核对：先清掉上一轮的结果，避免已经不偏差的段一直留着
    store.conn.execute("DELETE FROM meta.quality_issues WHERE rule_id = 'GD001'")
    ours = ch.query_df(f"""
        SELECT a.security_id, a.date, a.adj_close, a.close, p.source_symbol, p.batch_id
        FROM {db}.v_price_daily_adjusted AS a
        INNER JOIN {db}.v_price_daily AS p ON a.security_id = p.security_id AND a.date = p.date
        WHERE a.adj_close > 0""")
    if ours.empty:
        return {"compared": 0, "issues": 0}
    fmp = ch.query_df(f"""
        SELECT symbol AS source_symbol, date, argMax(adjClose, ingested_at) AS fmp_adj_close,
               argMax(batch_id, ingested_at) AS fmp_batch
        FROM {raw_table} WHERE symbol IN {{syms:Array(String)}} AND adjClose > 0 GROUP BY symbol, date""",
        {"syms": sorted(ours["source_symbol"].unique().tolist())})
    d = ours.merge(fmp, on=["source_symbol", "date"], how="inner").sort_values(["security_id", "date"])
    # 两边重叠的最后一天不比：两次拉取时间不同，当天收盘数据可能还在更新
    d = d[d["date"] < d.groupby("security_id")["date"].transform("max")].copy()
    g = d.groupby("security_id")
    d["r_ours"] = g["adj_close"].pct_change()
    d["r_fmp"] = g["fmp_adj_close"].pct_change()
    # FMP 复权价每次分红都会改写整段历史：相邻两天来自不同的拉取批次时口径可能不同，不比
    #（每日增量只拉最近几天，跨批次的那一天会出现假偏差）
    d.loc[g["fmp_batch"].shift() != d["fmp_batch"], "r_fmp"] = np.nan
    d["r_unadj"] = g["close"].pct_change()
    d = d.dropna(subset=["r_ours", "r_fmp"]).copy()
    d["diff"] = d["r_ours"] - d["r_fmp"]
    bad = d[d["diff"].abs() > FMP_TOLERANCE]
    for batch_id, gg in bad.groupby("batch_id"):
        store.record_issues(batch_id, [
            {"rule_id": "GD001", "rule_version": GD001["rule_version"], "severity": "warning", "symbol": r.source_symbol,
             "date": r.date, "detail": {"security_id": int(r.security_id), "r_ours": round(r.r_ours, 6),
                                        "r_fmp": round(r.r_fmp, 6), "r_unadjusted": round(r.r_unadj, 6)}}
            for r in gg.itertuples()])
    stats = {"compared": len(d), "issues": len(bad), "securities": int(bad["security_id"].nunique()),
             "over_10pct": int((bad["diff"].abs() > 0.10).sum())}
    log.info(f"[golden] 和 FMP 复权收益率核对：{stats}")
    return stats


def pending_batches(store: MetaStore, db: str = GOLDEN_DB) -> list[str]:
    """已经清洗出中间态、但还没成功写进 golden 的不复权日线批次"""
    return [r[0] for r in store.conn.execute("""
        SELECT DISTINCT i.batch_id FROM meta.ingestion i
        JOIN meta.load_log l ON l.batch_id = i.batch_id AND l.layer = 'intermediate' AND l.status = 'success'
        WHERE i.endpoint = %s
          AND NOT EXISTS (SELECT 1 FROM meta.load_log g WHERE g.batch_id = i.batch_id AND g.layer = 'golden'
                          AND g.status = 'success' AND g.target_table = %s)
        ORDER BY 1""", (PRICE_ENDPOINT, f"{db}.price_daily")).fetchall()]


def load_all(*, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
             today: date | None = None) -> dict:
    """还没写进 golden 的批次 → golden；再重算复权因子、和 FMP 核对"""
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    batches = pending_batches(store)
    totals = {}
    for b in batches:
        for k, v in load_prices(b, store=store, ch=ch, today=today).items():
            totals[k] = totals.get(k, 0) + v
    return {"batches": len(batches), "prices": totals,
            "adjustments": build_adjustments(store=store, ch=ch, today=today),
            "check": check_against_fmp(store=store, ch=ch)}


def main():
    parser = argparse.ArgumentParser(description="中间态 → golden（日线：不复权价 + 复权因子）")
    parser.add_argument("command", choices=["all", "prices", "adjustments", "check"])
    parser.add_argument("batch_id", nargs="?")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("db").setLevel(logging.WARNING)
    if args.command == "all":
        print(load_all())
    elif args.command == "prices":
        if not args.batch_id:
            parser.error("prices 需要 batch_id")
        print(load_prices(args.batch_id))
    elif args.command == "adjustments":
        print(build_adjustments())
    else:
        print(check_against_fmp())


if __name__ == "__main__":
    main()
