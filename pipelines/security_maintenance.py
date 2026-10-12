"""
证券主表增量维护：改名、新调入、退市

事件回填 security_id 以后，主表不能再清空重建，只能在原来的证券上改。三类维护：
    1. 改名：还没对应上的代码变更 X → Y（日期 d）。d 前一天用 X 的是主表里的证券、且 X 现在还没结束 →
             X 在 d 结束、从 d 开始用 Y（记上 event_id），回填这条事件；Y 那天已被别的证券占着 → 不改，进报告
             config/symbol_change_fixes.csv 里 ignore 的跳过
    2. 新调入：2010 年以后、还没对应上的 S&P 500 调入事件，调入日主表里没人用这个代码 → 新建证券
             （拉 profile / SEC CIK，用和首次生成同一套 build_record 组装），回填这条事件
    3. 退市：仍是 active、但已不在指数里的证券，拉最近 60 天日线：
             最后一天早于今天 10 天以上 → 退市：最后交易日 = 日线最后一天；收购方记录里是被收购方 → acquired，
             否则退市原因留空、标 needs_review；所有还没结束的代码在最后交易日的下一天结束
             一条日线都拉不到 → 不自动退市，进报告（可能代码已变、数据源缺失）

默认只试算（拉外部数据、出报告，不写库），加 --apply 才写库；每个动作一个事务，可以重复执行。
每天的顺序：event_loader 导入事件 → security_maintenance --apply → event_security_backfill --apply（补分红、拆股等）

命令行：
    python -m pipelines.security_maintenance                  # 试算
    python -m pipelines.security_maintenance --apply          # 写库
    python -m pipelines.security_maintenance --skip-delist    # 不做退市检查（不拉日线）
"""
import argparse
import csv
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg

from config.paths import DATA_DIR
from db.meta_store import MetaStore
from fetch_data.events.common import to_date
from fetch_data.fetch import FetchData
from pipelines.security_master import (
    STALE_PRICE_DAYS, UNIVERSE_START, Candidate, Interval, build_record, fetch_profiles, fetch_sec_ciks,
    insert_record, load_change_fixes, load_overrides,
)

log = logging.getLogger(__name__)

REPORT_PATH = DATA_DIR / "security_maintenance_report.csv"
PRICE_LOOKBACK_DAYS = 60
ACQUISITION_WINDOW_DAYS = 730          # 收购文件日最早可以比最后交易日早多久（审批常常拖一年以上）


@dataclass
class Action:
    kind: str                          # rename / new_security / delist / skip
    symbol: str
    detail: str
    security_id: int | None = None
    event_id: int | None = None
    payload: dict = field(default_factory=dict)


# =========================
# 1. 改名
# =========================
def plan_renames(conn, ignore: set[tuple]) -> list[Action]:
    rows = conn.execute(
        "SELECT event_id, old_symbol, new_symbol, change_date FROM events.v_symbol_changes_current "
        "WHERE security_id IS NULL ORDER BY change_date, event_id").fetchall()
    actions = []
    for eid, old, new, d in rows:
        if (old, new, d) in ignore:
            continue
        hit = conn.execute(
            "SELECT identifier_id, security_id FROM ref.security_identifiers "
            "WHERE id_type = 'symbol' AND id_value = %s AND valid_from < %s AND valid_to IS NULL",
            (old, d)).fetchone()
        if hit is None:                # 旧代码不在主表里、或者已经结束（历史上的改名，首次生成时已处理）
            continue
        ident_id, sid = hit
        taken = conn.execute("SELECT ref.resolve_security('symbol', %s, %s)", (new, d)).fetchone()[0]
        if taken is not None and taken != sid:
            actions.append(Action("skip", old, f"改名 {old} → {new}（{d}）：新代码那天已被证券 {taken} 使用，需人工处理",
                                  sid, eid))
            continue
        actions.append(Action("rename", old, f"{old} → {new}（{d}）", sid, eid,
                              {"identifier_id": ident_id, "old": old, "new": new, "date": d}))
    return actions


def apply_rename(conn, a: Action) -> None:
    p = a.payload
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("UPDATE ref.security_identifiers SET valid_to = %s WHERE identifier_id = %s AND valid_to IS NULL",
                    (p["date"], p["identifier_id"]))
        cur.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source, event_id) "
                    "VALUES (%s, 'symbol', %s, %s, 'fmp_symbol_change', %s)", (a.security_id, p["new"], p["date"], a.event_id))
        cur.execute("UPDATE events.corporate_events SET security_id = %s WHERE event_id = %s AND security_id IS NULL",
                    (a.security_id, a.event_id))
        cur.execute("UPDATE ref.securities SET notes = concat_ws('；', notes, %s::text) WHERE security_id = %s",
                    (f"{p['date']} 代码 {p['old']} → {p['new']}（自动维护）", a.security_id))


# =========================
# 2. 新调入
# =========================
def plan_new_securities(conn, meta: MetaStore, today: date, *, profiles: dict | None = None,
                        sec_ciks: dict | None = None, fetcher_cls=FetchData) -> list[Action]:
    rows = conn.execute(
        "SELECT event_id, symbol, effective_date FROM events.v_sp500_index_changes_current "
        "WHERE security_id IS NULL AND action = 'add' AND effective_date >= %s "
        "AND ref.resolve_security('symbol', symbol, effective_date) IS NULL ORDER BY effective_date",
        (UNIVERSE_START,)).fetchall()
    if not rows:
        return []
    symbols = sorted({r[1] for r in rows})
    profiles = profiles if profiles is not None else fetch_profiles(meta, symbols, fetcher_cls)
    sec = sec_ciks if sec_ciks is not None else fetch_sec_ciks()
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        changes = cur.execute("SELECT old_symbol, new_symbol, change_date, company_name "
                              "FROM events.v_symbol_changes_current ORDER BY change_date").fetchall()
    _, ignore = load_change_fixes()
    changes = [c for c in changes if (c["old_symbol"], c["new_symbol"], c["change_date"]) not in ignore]
    overrides = load_overrides()
    actions = []
    for eid, sym, d in rows:
        cand = Candidate([Interval(sym, d, None, eid)])
        rec = build_record(cand, profile=profiles.get(sym), sec_cik=sec.get(sym), last_price_date=None,
                           acquisitions=[], symbol_changes=changes, overrides=overrides, today=today)
        actions.append(Action("new_security", sym, f"{rec.name}（{d} 调入，{rec.review_status}）", None, eid, {"record": rec}))
    return actions


def apply_new_security(conn, a: Action) -> int:
    with conn.transaction(), conn.cursor() as cur:
        sid, _ = insert_record(conn, cur, a.payload["record"])
        cur.execute("UPDATE events.corporate_events SET security_id = %s WHERE event_id = %s AND security_id IS NULL",
                    (sid, a.event_id))
    a.security_id = sid
    return sid


# =========================
# 3. 退市
# =========================
def active_not_in_index(conn) -> list[tuple[int, str]]:
    """仍是 active、最近一次成分事件是调出（或没有对应上的成分事件）的证券，和它现在的代码"""
    return conn.execute("""
        WITH last_index AS (
            SELECT DISTINCT ON (security_id) security_id, action
            FROM events.v_sp500_index_changes_current WHERE security_id IS NOT NULL
            ORDER BY security_id, effective_date DESC, (action = 'add') DESC   -- 同一天调出又调入算在指数里
        )
        SELECT s.security_id, i.id_value
        FROM ref.securities s
        JOIN ref.security_identifiers i ON i.security_id = s.security_id AND i.id_type = 'symbol' AND i.valid_to IS NULL
        LEFT JOIN last_index l ON l.security_id = s.security_id
        WHERE s.status = 'active' AND coalesce(l.action, 'remove') = 'remove'
        ORDER BY s.security_id""").fetchall()


def fetch_last_prices(meta: MetaStore, symbols: list[str], today: date, fetcher_cls=FetchData) -> dict[str, date | None]:
    out = {}
    if not symbols:
        return out
    params = {"from_date": today - timedelta(days=PRICE_LOOKBACK_DAYS), "to_date": today}
    with meta.ingestion_run("price_volume", symbols, {"purpose": "security_maintenance_delist_check"}) as run:
        failed_all = []
        for sym in symbols:
            with fetcher_cls([sym], "price_volume", params) as fetcher:
                df, _, _, failed = fetcher.fetch_fmp_batch()
            failed_all += failed
            out[sym] = max(to_date(x) for x in df["date"]) if not df.empty else None
        n_ok = sum(v is not None for v in out.values())
        run.finish({"success": n_ok, "missing": len(out) - n_ok, "failed": len(failed_all)}, failed_all)
    return out


def plan_delistings(conn, today: date, last_prices: dict[str, date | None]) -> list[Action]:
    actions = []
    for sid, sym in active_not_in_index(conn):
        if sym not in last_prices:
            continue
        last = last_prices[sym]
        if last is None:
            actions.append(Action("skip", sym, f"最近 {PRICE_LOOKBACK_DAYS} 天拉不到日线，没有自动退市，需人工核对", sid))
            continue
        if last >= today - timedelta(days=STALE_PRICE_DAYS):
            continue
        acq = conn.execute(
            "SELECT acquirer_name, filing_date FROM events.v_acquisitions_current "
            "WHERE target_security_id = %s AND filing_date BETWEEN %s AND %s ORDER BY filing_date DESC LIMIT 1",
            (sid, last - timedelta(days=ACQUISITION_WINDOW_DAYS), last + timedelta(days=30))).fetchone()
        reason = "acquired" if acq else None
        detail = f"最后交易日 {last}" + (f"，被 {acq[0]} 收购（文件日 {acq[1]}）" if acq else "，退市原因不明")
        actions.append(Action("delist", sym, detail, sid, None, {"last_trade_date": last, "reason": reason}))
    return actions


def apply_delisting(conn, a: Action) -> None:
    last, reason = a.payload["last_trade_date"], a.payload["reason"]
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("UPDATE ref.security_identifiers SET valid_to = greatest(%s, valid_from + 1) "
                    "WHERE security_id = %s AND valid_to IS NULL", (last + timedelta(days=1), a.security_id))
        cur.execute(
            "UPDATE ref.securities SET status = 'delisted', last_trade_date = %s, delisting_reason = %s::text, "
            "primary_exchange = NULL, "
            "review_status = CASE WHEN %s::text IS NULL THEN 'needs_review' ELSE review_status END, "
            "notes = concat_ws('；', notes, %s::text) WHERE security_id = %s",
            (last, reason, reason, f"{date.today()} 自动退市：{a.detail}", a.security_id))


# =========================
# 一键维护
# =========================
APPLY = {"rename": apply_rename, "new_security": apply_new_security, "delist": apply_delisting}


def run(*, meta: MetaStore | None = None, apply: bool = False, delist: bool = True, today: date | None = None,
        fetcher_cls=FetchData) -> list[Action]:
    meta = meta or MetaStore()
    today = today or date.today()
    conn = meta.conn
    _, ignore = load_change_fixes()

    # 顺序：先改名（新代码可能紧接着出现在调入事件里），再新建，最后退市
    actions = plan_renames(conn, ignore)
    if apply:
        for a in actions:
            if a.kind == "rename":
                apply_rename(conn, a)
    new = plan_new_securities(conn, meta, today, fetcher_cls=fetcher_cls)
    if apply:
        for a in new:
            apply_new_security(conn, a)
    actions += new
    if delist:
        symbols = [sym for _, sym in active_not_in_index(conn)]
        dl = plan_delistings(conn, today, fetch_last_prices(meta, symbols, today, fetcher_cls))
        if apply:
            for a in dl:
                if a.kind == "delist":
                    apply_delisting(conn, a)
        actions += dl
    write_report(actions, applied=apply)
    return actions


def write_report(actions: list[Action], applied: bool, path=REPORT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["kind", "symbol", "security_id", "event_id", "detail", "applied"])
        for a in actions:
            w.writerow([a.kind, a.symbol, a.security_id, a.event_id, a.detail, applied and a.kind != "skip"])


def main():
    parser = argparse.ArgumentParser(description="证券主表增量维护：改名、新调入、退市（默认试算）")
    parser.add_argument("--apply", action="store_true", help="写库")
    parser.add_argument("--skip-delist", action="store_true", help="不做退市检查（不拉日线）")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("fetch_data.fetch").setLevel(logging.ERROR)
    actions = run(apply=args.apply, delist=not args.skip_delist)
    from collections import Counter
    print("动作：", dict(Counter(a.kind for a in actions)))
    for a in actions:
        print(f"  {a.kind:<13} {a.symbol:<8} {a.security_id or '':<6} {a.detail}")
    print(f"明细报告：{REPORT_PATH}")
    print("已写库" if args.apply else "试算完成，没有写库；确认后加 --apply")


if __name__ == "__main__":
    main()
