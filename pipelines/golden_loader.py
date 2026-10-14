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
         · 只算落在这只证券 golden 日线区间里的事件（区间外的事件不影响任何已存价格）
         · 事件修订（revision 变了）或因子变了写新版本
    3. check_against_fmp()：我们算的复权收盘价 vs FMP 复权收盘价，偏差 > 0.5% 记问题（GD001，warning）
         一段连续偏差的日子只记一条（记在这段的第一天），避免一个因子算错刷出上千条

命令行：
    python -m pipelines.golden_loader all            # 所有不复权日线批次 → golden，再算复权因子、核对
    python -m pipelines.golden_loader prices <batch_id>
    python -m pipelines.golden_loader adjustments
    python -m pipelines.golden_loader check
"""
import argparse
import csv
import logging
import os
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
SOURCE_TO_GOLDEN = {"adjOpen": "open", "adjHigh": "high", "adjLow": "low", "adjClose": "close", "volume": "volume"}
VALUE_COLS = list(SOURCE_TO_GOLDEN.values())
CHANGE_REL = 1e-6                  # 相对变化超过它算"有变化"（基本等于有变化就算）
FMP_TOLERANCE = 0.005              # 复权价和 FMP 复权价偏差超过 0.5% 记问题

GD001 = {"rule_id": "GD001", "rule_version": 1, "endpoint": "golden_price_daily", "severity": "warning",
         "description": "我们算的复权收盘价（不复权价 × 复权因子）与 FMP 复权收盘价偏差 > 0.5%；一段连续偏差记一条（这段的第一天）",
         "params": {"tolerance": FMP_TOLERANCE}, "impl": "golden_loader.check_against_fmp"}


def load_skip(path=None) -> dict[str, str]:
    path = path or SKIP_PATH
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return {r["symbol"].strip().upper(): r["reason"] for r in csv.DictReader(f)}


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
                intermediate: IntermediateStore | None = None, db: str = GOLDEN_DB) -> dict:
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
    universe = build_universe(store.conn)
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
    is_split = ev["kind"] == "split"
    ev["price_factor"] = np.where(is_split, 1 / ev["value"], 1 - ev["value"] / ev["prev_close"])
    ev["volume_factor"] = np.where(is_split, ev["value"], 1.0)
    # 因子必须 > 0（缺前收时是 NaN，也算无效）；分红因子还必须 < 1（金额 >= 前收说明数据有错）
    bad = ~(ev["price_factor"] > 0) | ((~is_split) & ~(ev["price_factor"] < 1))
    stats["bad_factor"] = int(bad.sum())
    if bad.any():
        log.warning(f"[golden] {int(bad.sum())} 个事件因子无效（分红金额 >= 前收或缺前收），已跳过："
                    f"{ev.loc[bad, ['security_id', 'ex_date', 'kind', 'value', 'prev_close']].head(5).to_dict('records')}")
    ev = ev[~bad]

    cur = ch.query_df(f"""
        SELECT security_id, ex_date, kind, event_id, max(version) AS v,
               argMax(event_revision, version) AS rev_cur, argMax(price_factor, version) AS pf_cur
        FROM {db}.adjustment_events GROUP BY security_id, ex_date, kind, event_id""").rename(columns={"v": "version"})
    if not cur.empty:
        cur["ex_date"] = pd.to_datetime(cur["ex_date"])
    m = ev.merge(cur, on=["security_id", "ex_date", "kind", "event_id"], how="left") if not cur.empty else ev.assign(
        version=np.nan, rev_cur=np.nan, pf_cur=np.nan)
    is_new = m["version"].isna()
    changed = ~is_new & ((m["event_revision"] != m["rev_cur"]) | (np.abs(m["price_factor"] - m["pf_cur"]) > 1e-9))
    out = m[is_new | changed].copy()
    out["version"] = (pd.to_numeric(out["version"]).fillna(0) + 1).astype("uint32")
    stats.update(new=int(is_new.sum()), changed=int(changed.sum()))
    if not out.empty:
        out["prev_close"] = out["prev_close"].astype(float)
        out["event_id"] = out["event_id"].astype("uint64")
        out["event_revision"] = out["event_revision"].astype("uint32")
        ch.insert_df(f"{db}.adjustment_events",
                     out[["security_id", "ex_date", "kind", "value", "prev_close", "price_factor", "volume_factor",
                          "event_id", "event_revision", "known_at", "version"]])
    stats["written"] = len(out)
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
        SELECT a.security_id, a.date, a.adj_close, p.source_symbol, p.batch_id
        FROM {db}.v_price_daily_adjusted AS a
        INNER JOIN {db}.v_price_daily AS p ON a.security_id = p.security_id AND a.date = p.date
        WHERE a.adj_close > 0""")
    if ours.empty:
        return {"compared": 0, "issues": 0}
    fmp = ch.query_df(f"""
        SELECT symbol AS source_symbol, date, argMax(adjClose, ingested_at) AS fmp_adj_close
        FROM {raw_table} WHERE symbol IN {{syms:Array(String)}} GROUP BY symbol, date""",
        {"syms": sorted(ours["source_symbol"].unique().tolist())})
    d = ours.merge(fmp, on=["source_symbol", "date"], how="inner").sort_values(["security_id", "date"])
    d["dev"] = d["adj_close"] / d["fmp_adj_close"] - 1
    d["bad"] = d["dev"].abs() > FMP_TOLERANCE
    # 一段连续偏差（中间没有恢复正常的日子）记一条
    d["seg"] = (d["bad"] != d.groupby("security_id")["bad"].shift()).cumsum()
    segs = d[d["bad"]].groupby("seg").agg(
        security_id=("security_id", "first"), symbol=("source_symbol", "first"), batch_id=("batch_id", "first"),
        start=("date", "min"), end=("date", "max"), days=("date", "size"),
        max_dev=("dev", lambda s: float(s.abs().max())), first_ours=("adj_close", "first"),
        first_fmp=("fmp_adj_close", "first"))
    issues = []
    for batch_id, g in segs.groupby("batch_id"):
        issues = [{"rule_id": "GD001", "rule_version": 1, "severity": "warning", "symbol": r.symbol, "date": r.start,
                   "detail": {"security_id": int(r.security_id), "end": str(r.end), "days": int(r.days),
                              "max_deviation": round(r.max_dev, 6), "ours": float(r.first_ours), "fmp": float(r.first_fmp)}}
                  for r in g.itertuples()]
        store.record_issues(batch_id, issues)
    stats = {"compared": len(d), "deviating_days": int(d["bad"].sum()), "segments": len(segs),
             "securities": int(segs["security_id"].nunique()) if len(segs) else 0,
             "median_abs_dev": float(d["dev"].abs().median())}
    log.info(f"[golden] 和 FMP 复权价核对：{stats}")
    return stats


def load_all(*, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    batches = [r[0] for r in store.conn.execute("""
        SELECT DISTINCT i.batch_id FROM meta.ingestion i
        JOIN meta.load_log l ON l.batch_id = i.batch_id AND l.layer = 'intermediate' AND l.status = 'success'
        WHERE i.endpoint = %s ORDER BY 1""", (PRICE_ENDPOINT,)).fetchall()]
    totals = {}
    for b in batches:
        for k, v in load_prices(b, store=store, ch=ch).items():
            totals[k] = totals.get(k, 0) + v
    return {"batches": len(batches), "prices": totals,
            "adjustments": build_adjustments(store=store, ch=ch), "check": check_against_fmp(store=store, ch=ch)}


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
