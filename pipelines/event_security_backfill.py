"""
事件回填 security_id：把 events.corporate_events.security_id、events.acquisitions.acquirer_security_id 从空填上

⚠ 只能做一次（不可逆）：回填后 security_id 不能再改，证券主表也不能再清空重建（security_master build --rebuild 会拒绝）。
  所以默认只试算、出报告，加 --apply 才写库。只填空的，已经填过的不动，可以重复执行（补新事件）。

按事件类型对应（日期都用明细表里的业务日期，总表的 event_date 不一定是业务日期）：
    分红 / 拆股（FMP 按代码拉的全部历史）：
        FMP 按"现在的代码"给历史（META 2022 年以前的分红也记在 META 下），所以：
        1. 业务日期那天谁用这个代码 → 就是它（GOOG 2014 年以前 → A 类股）
        2. 没人用：这个代码是某只证券最后使用的代码，且业务日期在它的存续期内 → 它（META 改名前、FB 时期）
        3. 代码现在被主表以外的公司使用（FMP profile 仍在交易、主表里现在没人用，如 STI → Solidion）：
           只填新公司上市日之前的（那时只可能是老公司的数据），且只按第 1 条；之后的不填
    S&P 500 成分变更：调入日 / 调出日前一天谁用这个代码 → 它；
        查不到（社区数据集把代码回溯成了后来的代码，如 2006 年的 GOOGL）→ 用生成主表时的成分区间对应；
        两种方法都有结果但不一致（IR、UA）→ 不填
    原则：拿不准的不填。空的以后还能补，填错了改不回来
    代码变更：改名前一天谁用旧代码 → 它；查不到再看改名当天谁用新代码。symbol_change_fixes.csv 里忽略的不填
    收购：被收购方 = 文件日谁用这个代码，查不到用 CIK（只有一只证券用这个 CIK 时）；收购方同理

命令行：
    python -m pipelines.event_security_backfill            # 试算：统计 + 报告，不写库
    python -m pipelines.event_security_backfill --apply    # 写库
"""
import argparse
import csv
import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg

from config.paths import DATA_DIR
from db.meta_store import MetaStore
from fetch_data.events.common import to_date
from pipelines.security_master import (
    CACHE_PATH, build_record, final_candidates, load_change_fixes, load_inputs_fixed, load_overrides,
)

log = logging.getLogger(__name__)

REPORT_PATH = DATA_DIR / "event_security_backfill_report.csv"


# =========================
# 证券主表（内存里查，比逐条调 ref.resolve_security 快）
# =========================
@dataclass
class Master:
    symbol: dict = field(default_factory=lambda: defaultdict(list))     # 代码 → [(security_id, valid_from, valid_to)]
    cik: dict = field(default_factory=lambda: defaultdict(list))
    life: dict = field(default_factory=dict)                            # security_id → (首个交易日, 最后交易日)
    last_symbol: dict = field(default_factory=dict)                     # security_id → 最后使用的代码

    @classmethod
    def load(cls, conn) -> "Master":
        m = cls()
        for sid, t, v, vf, vt in conn.execute("SELECT security_id, id_type, id_value, valid_from, valid_to "
                                              "FROM ref.security_identifiers WHERE id_type IN ('symbol', 'cik')"):
            (m.symbol if t == "symbol" else m.cik)[v].append((sid, vf, vt))
            if t == "symbol" and (sid not in m.last_symbol or vf > m.last_symbol[sid][1]):
                m.last_symbol[sid] = (v, vf)
        m.last_symbol = {k: v[0] for k, v in m.last_symbol.items()}
        m.life = {sid: (ft, lt) for sid, ft, lt in conn.execute(
            "SELECT security_id, first_trade_date, last_trade_date FROM ref.securities")}
        return m

    def resolve(self, id_type: str, value: str | None, d: date | None) -> int | None:
        """某一天用这个代码的证券；CIK 可能多只共用，只有一只时才返回"""
        if not value or d is None:
            return None
        hits = {sid for sid, vf, vt in (self.symbol if id_type == "symbol" else self.cik).get(value, [])
                if vf <= d and (vt is None or d < vt)}
        return hits.pop() if len(hits) == 1 else None

    def alive(self, sid: int, d: date) -> bool:
        ft, lt = self.life[sid]
        return (ft is None or ft <= d) and (lt is None or d <= lt)


# =========================
# 各类事件的对应规则
# =========================
def map_per_symbol(rows: list[dict], master: Master, reused: dict[str, date | None]) -> dict[int, tuple[int | None, str]]:
    """分红 / 拆股：rows = {event_id, symbol, d}；返回 event_id → (security_id, 依据)"""
    owner_of_last = defaultdict(set)
    for sid, sym in master.last_symbol.items():
        owner_of_last[sym].add(sid)
    out = {}
    for r in rows:
        sym, d = r["symbol"], r["d"]
        if sym in reused:
            new_ipo = reused[sym]
            sid = master.resolve("symbol", sym, d) if new_ipo and d < new_ipo else None
            out[r["event_id"]] = (sid, "当天用这个代码（代码后来给了别家，只填别家上市以前的）" if sid
                                  else "代码现在被主表以外的公司使用")
            continue
        sid = master.resolve("symbol", sym, d)
        if sid:
            out[r["event_id"]] = (sid, "当天用这个代码")
            continue
        owners = owner_of_last.get(sym, set())
        if len(owners) == 1 and master.alive(next(iter(owners)), d):
            out[r["event_id"]] = (next(iter(owners)), "FMP 按现在的代码记历史")
        else:
            out[r["event_id"]] = (None, "对应不上")
    return out


def map_index_changes(rows: list[dict], master: Master, by_candidate: dict[int, int]) -> dict:
    out = {}
    for r in rows:
        d = r["d"] if r["action"] == "add" else r["d"] - timedelta(days=1)
        sid = master.resolve("symbol", r["symbol"], d)
        cand_sid = by_candidate.get(r["event_id"])
        if sid and cand_sid not in (None, sid):
            out[r["event_id"]] = (None, f"不一致：当天用这个代码的是 {sid}，成分区间对应的是 {cand_sid}")
        elif sid:
            out[r["event_id"]] = (sid, "当天用这个代码")
        elif cand_sid:
            out[r["event_id"]] = (cand_sid, "成分区间对应")
        else:
            out[r["event_id"]] = (None, "对应不上（多为 2010 年以前调出、不在股票池）")
    return out


def map_symbol_changes(rows: list[dict], master: Master, ignore: set[tuple]) -> dict:
    out = {}
    for r in rows:
        if (r["old_symbol"], r["new_symbol"], r["d"]) in ignore:
            out[r["event_id"]] = (None, "人工标记忽略")
            continue
        sid = master.resolve("symbol", r["old_symbol"], r["d"] - timedelta(days=1))
        why = "改名前一天用旧代码"
        if not sid:
            sid, why = master.resolve("symbol", r["new_symbol"], r["d"]), "改名当天用新代码"
        out[r["event_id"]] = (sid, why if sid else "对应不上（多为股票池以外）")
    return out


def map_party(master: Master, symbol: str | None, cik: str | None, d: date) -> tuple[int | None, str]:
    sid = master.resolve("symbol", symbol, d)
    if sid:
        return sid, "文件日用这个代码"
    sid = master.resolve("cik", cik, d)
    return (sid, "CIK") if sid else (None, "对应不上（多为股票池以外）")


# =========================
# 读输入
# =========================
def index_candidate_map(conn) -> dict[int, int]:
    """
    成分变更事件 → 证券：用生成主表时同一套串联得到候选，每个候选按第一段代码（代码 + 开始日）找到主表里的证券
    需要生成主表时的缓存（profile / 最后交易日），保证和主表一致
    """
    cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    profiles = cache["profiles"]
    last_prices = {k: date.fromisoformat(v) for k, v in cache["last_prices"].items()}
    inputs = load_inputs_fixed(conn)
    overrides = load_overrides()
    by_first_symbol = {(v, vf): sid for sid, v, vf in conn.execute(
        "SELECT security_id, id_value, valid_from FROM ref.security_identifiers WHERE id_type = 'symbol'")}
    out = {}
    for c in final_candidates(inputs, profiles, overrides):
        rec = build_record(c, profile=profiles.get(c.anchor), sec_cik=None, last_price_date=last_prices.get(c.anchor),
                           acquisitions=inputs["acquisitions"], symbol_changes=inputs["symbol_changes"],
                           overrides=overrides, today=date.fromisoformat(cache["date"]))
        syms = sorted((i for i in rec.identifiers if i["id_type"] == "symbol"), key=lambda i: i["valid_from"])
        sid = by_first_symbol.get((syms[0]["id_value"], syms[0]["valid_from"])) if syms else None
        if sid is None:
            continue
        for iv in c.intervals:
            for eid in (iv.add_event_id, iv.remove_event_id):
                if eid is not None:
                    out[eid] = sid
    return out


def reused_symbols(master: Master, today: date) -> dict[str, date | None]:
    """FMP profile 显示仍在交易、但主表里现在没人用这个代码 → 代码现在属于主表以外的公司；值为那家公司的上市日"""
    profiles = json.loads(CACHE_PATH.read_text(encoding="utf-8"))["profiles"]
    return {s: to_date(p.get("ipoDate")) for s, p in profiles.items()
            if p.get("isActivelyTrading") and not master.resolve("symbol", s, today)}


def load_rows(conn) -> dict[str, list[dict]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        q = lambda sql: cur.execute(sql).fetchall()
        return {
            "dividend": q("SELECT event_id, symbol, ex_date AS d FROM events.v_dividends_current WHERE security_id IS NULL"),
            "split": q("SELECT event_id, symbol, effective_date AS d FROM events.v_splits_current WHERE security_id IS NULL"),
            "index_change": q("SELECT event_id, symbol, effective_date AS d, action FROM events.v_sp500_index_changes_current "
                              "WHERE security_id IS NULL"),
            "symbol_change": q("SELECT event_id, old_symbol, new_symbol, change_date AS d FROM events.v_symbol_changes_current "
                               "WHERE security_id IS NULL"),
            "acquisition": q("SELECT event_id, target_security_id, acquirer_security_id, target_symbol, target_cik, "
                             "acquirer_symbol, acquirer_cik, filing_date AS d FROM events.v_acquisitions_current "
                             "WHERE target_security_id IS NULL OR acquirer_security_id IS NULL"),
        }


# =========================
# 计算 / 写库
# =========================
def compute(conn, today: date | None = None) -> tuple[dict, dict]:
    """返回 ({事件类型: {event_id: (security_id, 依据)}}, {event_id: (收购方 security_id, 依据)})"""
    today = today or date.today()
    master = Master.load(conn)
    rows = load_rows(conn)
    _, ignore = load_change_fixes()
    reused = reused_symbols(master, today)
    result = {
        "dividend": map_per_symbol(rows["dividend"], master, reused),
        "split": map_per_symbol(rows["split"], master, reused),
        "index_change": map_index_changes(rows["index_change"], master, index_candidate_map(conn)),
        "symbol_change": map_symbol_changes(rows["symbol_change"], master, ignore),
        "acquisition": {},
    }
    acquirers = {}
    for r in rows["acquisition"]:
        if r["target_security_id"] is None:
            result["acquisition"][r["event_id"]] = map_party(master, r["target_symbol"], r["target_cik"], r["d"])
        if r["acquirer_security_id"] is None:
            acquirers[r["event_id"]] = map_party(master, r["acquirer_symbol"], r["acquirer_cik"], r["d"])
    return result, acquirers


def apply(conn, result: dict, acquirers: dict) -> dict:
    """一个事务里写：先放进临时表，再批量 UPDATE（只填空的）"""
    pairs = [(eid, sid) for m in result.values() for eid, (sid, _) in m.items() if sid]
    acq_pairs = [(eid, sid) for eid, (sid, _) in acquirers.items() if sid]
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_event_security (event_id bigint PRIMARY KEY, security_id bigint) ON COMMIT DROP")
        with cur.copy("COPY tmp_event_security FROM STDIN") as cp:
            for p in pairs:
                cp.write_row(p)
        n_events = cur.execute("UPDATE events.corporate_events e SET security_id = t.security_id "
                               "FROM tmp_event_security t WHERE e.event_id = t.event_id AND e.security_id IS NULL").rowcount
        cur.execute("CREATE TEMP TABLE tmp_acquirer_security (event_id bigint PRIMARY KEY, security_id bigint) ON COMMIT DROP")
        with cur.copy("COPY tmp_acquirer_security FROM STDIN") as cp:
            for p in acq_pairs:
                cp.write_row(p)
        n_acq = cur.execute("UPDATE events.acquisitions a SET acquirer_security_id = t.security_id "
                            "FROM tmp_acquirer_security t WHERE a.event_id = t.event_id AND a.acquirer_security_id IS NULL").rowcount
    return {"events": n_events, "acquirers": n_acq}


def summarize(result: dict, acquirers: dict) -> dict:
    out = {k: Counter(why for _, why in m.values()) for k, m in result.items()}
    out["acquirer"] = Counter(why for _, why in acquirers.values())
    return out


def write_report(conn, result: dict, acquirers: dict, path=REPORT_PATH) -> None:
    """每条事件一行：类型、事件、代码、日期、对应到的证券、依据"""
    info = {eid: (sym, d) for eid, sym, d in conn.execute(
        "SELECT event_id, symbol, coalesce(d.ex_date, s.effective_date, i.effective_date, c.change_date, a.filing_date) "
        "FROM events.corporate_events e "
        "LEFT JOIN events.dividends d USING (event_id) LEFT JOIN events.splits s USING (event_id) "
        "LEFT JOIN events.sp500_index_changes i USING (event_id) LEFT JOIN events.symbol_changes c USING (event_id) "
        "LEFT JOIN events.acquisitions a USING (event_id)")}
    names = dict(conn.execute("SELECT security_id, name FROM ref.securities"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_type", "event_id", "symbol", "date", "security_id", "security_name", "reason"])
        for kind, m in list(result.items()) + [("acquirer", acquirers)]:
            for eid, (sid, why) in sorted(m.items()):
                sym, d = info.get(eid, (None, None))
                w.writerow([kind, eid, sym, d, sid, names.get(sid), why])


def main():
    parser = argparse.ArgumentParser(description="事件回填 security_id（默认试算，--apply 才写库；写库不可逆）")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    meta = MetaStore()
    result, acquirers = compute(meta.conn)
    for kind, c in summarize(result, acquirers).items():
        print(kind, dict(c.most_common()))
    write_report(meta.conn, result, acquirers)
    print(f"明细报告：{REPORT_PATH}")
    if args.apply:
        print("写库：", apply(meta.conn, result, acquirers))
    else:
        print("试算完成，没有写库；确认后加 --apply")


if __name__ == "__main__":
    main()
