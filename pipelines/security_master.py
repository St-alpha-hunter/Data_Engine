"""
由事件生成证券主表（ref.securities + ref.security_identifiers）

流程：
    1. 串联：S&P 500 成分股区间 → 用"代码变更 + 同一天调出 / 调入"把改名前后串成同一只证券；
             同一代码多段区间（调出后又调入）先合并；如果 FMP profile 的上市日落在两段之间的空档里，
             说明是两家公司（私有化后重新上市、代码给了别家，如 HLT、DELL、BR），拆成两只证券
    2. 补改名：社区数据集有时把整段历史记成后来的代码；如果 FMP 代码变更里有 X → Y 且日期落在 Y 的区间内，
             把 Y 之前的那段改回 X
    3. 人工对照：config/security_overrides.csv 里登记的证券，代码历史以人工为准（verified）
    4. 核对与补字段：
         FMP profile（按当前代码）→ CIK / CUSIP / ISIN / 上市日 / 名称 / 类型 / 交易所 / 是否仍在交易
             上市日晚于首次进入指数（且不在空档里）→ 可能是重组日期，也可能同一天被新公司接替：
             仍用 profile，但 CIK / CUSIP / ISIN 从 profile 上市日起生效，标 needs_review
         SEC 官方代码表 → CIK，和 profile 的 CIK 交叉核对（只对仍在交易的）
         收购事件 → 退市原因、退市公司的 CIK 和名称
         FMP 日线最后一天 → 退市证券的最后交易日
    5. 写库：一个事务；代码冲突（排除约束）的单条代码跳过，该证券标 needs_review
    6. review_status：人工对照 → verified；所有核对都通过 → auto；否则 needs_review（原因写在 notes）

只覆盖股票池：2010 年以后在过 S&P 500 的证券。

命令行：
    python -m pipelines.security_master build              # 证券主表为空时生成
    python -m pipelines.security_master build --rebuild    # 清空后重新生成（事件还没回填 security_id 时才允许）
    加 --use-cache：当天已经拉过的 profile / 最后交易日直接用缓存（data/security_master_cache.json）
"""
import argparse
import csv
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg
import requests

from config.paths import BASE_DIR, DATA_DIR
from config.symbol_map import to_fmp_symbol
from db.meta_store import MetaStore
from fetch_data.events.common import clean_text, to_cik, to_date, to_symbol
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

UNIVERSE_START = date(2010, 1, 1)
RENAME_TOLERANCE_DAYS = 3            # 代码变更日和调出 / 调入日允许相差的天数
IPO_TOLERANCE_DAYS = 3               # 上市日允许比首次进入指数晚的天数（数据误差）
STALE_PRICE_DAYS = 10                # 日线最后一天早于今天这么多天，视为已停止交易
OVERRIDES_PATH = BASE_DIR / "config" / "security_overrides.csv"
CHANGE_FIXES_PATH = BASE_DIR / "config" / "symbol_change_fixes.csv"   # 人工串联 / 忽略 FMP 的错误代码变更
REPORT_PATH = DATA_DIR / "security_master_review.csv"
CACHE_PATH = DATA_DIR / "security_master_cache.json"     # 当天的 profile / 最后交易日缓存，当天重新生成不必重复请求
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
EXCHANGE_TO_MIC = {"NASDAQ": "XNAS", "NYSE": "XNYS", "AMEX": "XASE"}


# =========================
# 数据结构
# =========================
@dataclass
class Interval:
    """一段 S&P 500 成分区间：[start, end)，end 为空表示仍在成分里"""
    symbol: str
    start: date
    end: date | None
    add_event_id: int | None = None
    remove_event_id: int | None = None


@dataclass
class Candidate:
    """一只证券候选：若干段成分区间（可能跨多个代码）"""
    intervals: list[Interval]
    renames: list[tuple] = field(default_factory=list)          # 用来串联的代码变更 (old, new, date)
    use_profile: bool = True                                     # 拆出来的"之前那家公司"不能用 profile（profile 是后来的公司）
    note: str | None = None
    override: list[dict] | None = None                           # 人工对照拆成多只证券时，这只对应的那组

    @property
    def first_add(self) -> date:
        return min(i.start for i in self.intervals)

    @property
    def last_end(self) -> date | None:
        return None if any(i.end is None for i in self.intervals) else max(i.end for i in self.intervals)

    @property
    def in_index(self) -> bool:
        return self.last_end is None

    @property
    def anchor(self) -> str:
        """最后使用的代码（数据集里最后一段的代码），用来查 FMP / SEC、匹配人工对照"""
        return max(self.intervals, key=lambda i: i.start).symbol

    @property
    def symbols(self) -> list[str]:
        return list(dict.fromkeys(i.symbol for i in sorted(self.intervals, key=lambda i: i.start)))


@dataclass
class SecurityRecord:
    """写入证券主表的一只证券：ref.securities 的字段 + 代码历史 + 核对结果"""
    anchor: str
    name: str
    security_type: str = "common_stock"
    share_class: str | None = None
    country: str | None = None
    primary_exchange: str | None = None
    first_trade_date: date | None = None
    last_trade_date: date | None = None
    status: str = "active"
    delisting_reason: str | None = None
    review_status: str = "auto"
    reasons: list[str] = field(default_factory=list)
    identifiers: list[dict] = field(default_factory=list)      # {id_type, id_value, valid_from, valid_to, source}
    candidate: Candidate | None = None


# =========================
# 1. 成分区间
# =========================
def build_intervals(index_events: list[dict]) -> list[Interval]:
    """S&P 500 成分股变更事件（symbol, effective_date, action, event_id）→ 成分区间"""
    by_symbol = defaultdict(list)
    for e in index_events:
        by_symbol[e["symbol"]].append(e)
    intervals = []
    for symbol, events in by_symbol.items():
        events.sort(key=lambda e: (e["effective_date"], 0 if e["action"] == "remove" else 1))
        current = None
        for e in events:
            if e["action"] == "add" and current is None:
                current = Interval(symbol, e["effective_date"], None, e.get("event_id"))
            elif e["action"] == "remove" and current is not None:
                current.end, current.remove_event_id = e["effective_date"], e.get("event_id")
                intervals.append(current)
                current = None
        if current is not None:
            intervals.append(current)
    return intervals


# =========================
# 2. 串联成证券候选
# =========================
def build_candidates(intervals: list[Interval], symbol_changes: list[dict],
                     since: date = UNIVERSE_START, tolerance: int = RENAME_TOLERANCE_DAYS,
                     manual_links: list[tuple[str, str]] = ()) -> list[Candidate]:
    """
    合并规则：
        改名：代码变更 X → Y（日期 d），X 的某段区间在 d 附近调出、Y 的某段区间在 d 附近调入 → 同一只证券
        再次调入：同一代码的多段区间 → 同一只证券（代码被别的公司重复使用的情况，由 profile 上市日核对发现）
    只保留 2010 年以后还在成分里的证券
    """
    parent = list(range(len(intervals)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a, b):
        parent[find(a)] = find(b)

    by_symbol = defaultdict(list)
    for idx, iv in enumerate(intervals):
        by_symbol[iv.symbol].append(idx)
    for idxs in by_symbol.values():
        for a, b in zip(idxs, idxs[1:]):
            union(a, b)

    links = defaultdict(list)
    for old, new in manual_links:                        # 人工串联：两个代码的所有区间并成一只
        for a in by_symbol.get(old, []):
            for b in by_symbol.get(new, []):
                union(a, b)
    tol = timedelta(days=tolerance)
    for c in symbol_changes:
        old, new, d = c["old_symbol"], c["new_symbol"], c["change_date"]
        for a in by_symbol.get(old, []):
            ia = intervals[a]
            if ia.end is None or abs(ia.end - d) > tol:
                continue
            for b in by_symbol.get(new, []):
                if abs(intervals[b].start - d) <= tol:
                    union(a, b)
                    links[a].append((old, new, d))

    groups = defaultdict(list)
    for idx in range(len(intervals)):
        groups[find(idx)].append(idx)

    candidates = []
    for idxs in groups.values():
        cand = Candidate(sorted((intervals[i] for i in idxs), key=lambda iv: iv.start),
                         [link for i in idxs for link in links.get(i, [])])
        if cand.last_end is None or cand.last_end >= since:
            candidates.append(cand)
    return sorted(candidates, key=lambda c: c.anchor)


def split_reused(cands: list[Candidate], profiles: dict[str, dict], since: date = UNIVERSE_START,
                 tolerance: int = IPO_TOLERANCE_DAYS) -> list[Candidate]:
    """
    同一代码多段区间，profile 上市日晚于第一段开始、但不晚于后面某段开始 → 两家公司，在那段之前拆开：
    （通常落在两段之间的空档里，如 HLT、DELL；也有数据集把老公司记在同一代码下、上市日落在第一段里的，如 CCI、CNC）
        之前那部分：已退市的老公司（不用 profile），同样只保留 2010 年以后还在成分里的
        之后那部分：profile 对应的公司
    """
    tol = timedelta(days=tolerance)
    out = []
    for c in cands:
        prof = profiles.get(c.anchor) if c.use_profile else None
        ipo = to_date(prof.get("ipoDate")) if prof else None
        ivs = sorted(c.intervals, key=lambda i: i.start)
        k = None
        if ipo is not None and ipo > c.first_add + tol:
            k = next((j for j in range(1, len(ivs))
                      if ivs[j - 1].end is not None and ivs[j - 1].symbol == ivs[j].symbol   # 改名串联的不拆
                      and ipo <= ivs[j].start + tol), None)
        if k is None:
            out.append(c)
            continue
        old_ivs, new_ivs = ivs[:k], ivs[k:]
        old_syms, new_syms = {i.symbol for i in old_ivs}, {i.symbol for i in new_ivs}
        old = Candidate(old_ivs, [r for r in c.renames if r[0] in old_syms and r[1] in old_syms], use_profile=False,
                        note=f"代码 {c.anchor} {ipo} 起被另一家公司使用（{prof.get('companyName')}），"
                             f"这只是之前的公司，没有 FMP profile")
        new = Candidate(new_ivs, [r for r in c.renames if r[1] in new_syms and r[2] >= new_ivs[0].start - tol])
        if old.last_end >= since:
            out.append(old)
        out.append(new)
    return out


# =========================
# 3. 代码历史（各段代码的起止）
# =========================
def symbol_segments(cand: Candidate, symbol_changes: list[dict], first_start: date | None = None) -> list[tuple[str, date]]:
    """
    返回 [(代码, 开始日), ...]，按时间顺序；每段到下一段开始为止
    第一段从 first_start（首个交易日）开始；在成分区间的代码基础上，补上漏掉的改名：
        · 数据集被"回溯成后来代码"（改名发生在这段区间内）
        · 上市后、进入指数前就改过名
    """
    segments = []
    for iv in sorted(cand.intervals, key=lambda i: i.start):
        if not segments or segments[-1][0] != iv.symbol:
            segments.append((iv.symbol, iv.start))
    if first_start is not None and first_start < segments[0][1]:
        segments[0] = (segments[0][0], first_start)

    changes_to = defaultdict(list)
    for c in symbol_changes:
        changes_to[c["new_symbol"]].append(c)

    for _ in range(10):                                  # 连续多次改名，最多补 10 次
        changed = False
        for k, (sym, start) in enumerate(segments):
            prev = segments[k - 1][0] if k > 0 else None
            # 这一段的结束：下一段开始；最后一段为调出指数的日期（之后的改名属于代码被别人重复使用，如 BBBY）
            nxt = segments[k + 1][1] if k + 1 < len(segments) else cand.last_end
            for c in changes_to.get(sym, []):
                d = c["change_date"]
                if start < d and (nxt is None or d < nxt) and c["old_symbol"] != prev:
                    segments[k:k + 1] = [(c["old_symbol"], start), (sym, d)]
                    changed = True
                    break
            if changed:
                break
        if not changed:
            break
    return segments


# =========================
# 4. 核对、补字段 → SecurityRecord
# =========================
def load_overrides(path=OVERRIDES_PATH) -> dict[str, list[dict]]:
    overrides = defaultdict(list)
    if not os.path.exists(path):
        return overrides
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            overrides[row["anchor_symbol"].strip().upper()].append({
                "symbol": row["symbol"].strip().upper(),
                "valid_from": date.fromisoformat(row["valid_from"]),
                "valid_to": date.fromisoformat(row["valid_to"]) if row.get("valid_to") else None,
                "share_class": clean_text(row.get("share_class")),
                "note": clean_text(row.get("note")),
                "security": clean_text(row.get("security")),
                "name": clean_text(row.get("name")),
                "cik": to_cik(row.get("cik")) if clean_text(row.get("cik")) else None,
            })
    return overrides


def load_change_fixes(path=CHANGE_FIXES_PATH) -> tuple[list[tuple[str, str]], set[tuple]]:
    """返回 (人工串联 [(old, new)], 要忽略的代码变更 {(old, new, date)})"""
    links, ignore = [], set()
    if not os.path.exists(path):
        return links, ignore
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            old, new = row["old_symbol"].strip().upper(), row["new_symbol"].strip().upper()
            if row["action"] == "link":
                links.append((old, new))
            elif row["action"] == "ignore":
                ignore.add((old, new, date.fromisoformat(row["change_date"])))
            else:
                raise ValueError(f"symbol_change_fixes.csv：未知 action {row['action']}")
    return links, ignore


def expand_overrides(cands: list[Candidate], overrides: dict) -> list[Candidate]:
    """
    人工对照里一个代码下分了多只证券（security 列不同，如 FOXA：21 世纪福克斯 / 新福克斯）→ 拆成多只候选，
    成分区间按各自的代码历史期间切开；仍在交易的那只（最后一段没有结束日）用 profile，其他不用
    """
    out = []
    for c in cands:
        rows = overrides.get(c.anchor)
        groups = defaultdict(list)
        for r in rows or []:
            groups[r["security"]].append(r)
        if len(groups) <= 1:
            out.append(c)
            continue
        for grp in groups.values():
            start = min(r["valid_from"] for r in grp)
            end = None if any(r["valid_to"] is None for r in grp) else max(r["valid_to"] for r in grp)
            ivs = []
            for i in c.intervals:
                s, e = max(i.start, start), min((x for x in (i.end, end) if x is not None), default=None)
                if e is None or s < e:
                    ivs.append(Interval(i.symbol, s, e, i.add_event_id if s == i.start else None,
                                        i.remove_event_id if e == i.end else None))
            out.append(Candidate(ivs or [Interval(c.anchor, start, end)], use_profile=end is None, override=grp))
    return out


def profile_is_valid(profile: dict | None, cand: Candidate) -> tuple[bool, str | None]:
    """profile 能否代表这只证券：上市日不能晚于首次进入指数（否则是代码被别的公司重复使用）"""
    if not profile:
        return False, "FMP profile 查不到"
    ipo = to_date(profile.get("ipoDate"))
    if ipo is not None and ipo > cand.first_add + timedelta(days=IPO_TOLERANCE_DAYS):
        return False, f"代码可能被重复使用：profile 上市日 {ipo} 晚于首次进入指数 {cand.first_add}（{profile.get('companyName')}）"
    return True, None


def match_acquisition(cand: Candidate, acquisitions: list[dict], last_trade: date | None) -> dict | None:
    """被收购方代码是这只证券用过的代码，且文件日期在它最后交易日之前（含 30 天误差）"""
    end = last_trade or cand.last_end
    hits = [a for a in acquisitions if a["target_symbol"] in cand.symbols
            and cand.first_add <= a["filing_date"] and (end is None or a["filing_date"] <= end + timedelta(days=30))]
    return max(hits, key=lambda a: a["filing_date"]) if hits else None


def build_record(cand: Candidate, *, profile: dict | None, sec_cik: str | None, last_price_date: date | None,
                 acquisitions: list[dict], symbol_changes: list[dict], overrides: dict, today: date) -> SecurityRecord:
    override = cand.override if cand.override is not None else overrides.get(cand.anchor)
    p = profile if cand.use_profile else None
    ipo = to_date(p.get("ipoDate")) if p else None
    rec = SecurityRecord(anchor=cand.anchor, name=cand.anchor, candidate=cand)
    if ipo is not None and not cand.in_index and not profile_is_valid(p, cand)[0]:
        # 已调出指数、上市日又晚于首次进入：基本是代码在退市后被别的公司拿去用了（如 APC、GENZ、SBNY），
        # 有的 ETF 的上市日还会落在原证券的区间里，profile 不是这只证券的
        rec.reasons.append(f"代码可能被重复使用：profile 上市日 {ipo} 晚于首次进入指数 {cand.first_add}（{p.get('companyName')}）")
        p, ipo = None, None
    late_ipo = not profile_is_valid(p, cand)[0] if p else False
    if p is None and not rec.reasons:
        rec.reasons.append(cand.note or "FMP profile 查不到")
    elif late_ipo:
        rec.reasons.append(f"profile 上市日 {ipo} 晚于首次进入指数 {cand.first_add}（可能是公司重组日期，"
                           f"也可能同一天被新公司接替：{p.get('companyName')}）；CIK / CUSIP / ISIN 从 {ipo} 起生效")

    # ---- 类型、国家、交易所、名称 ----
    if p:
        rec.name = clean_text(p.get("companyName")) or rec.name
        rec.security_type = ("etf" if p.get("isEtf") else "adr" if p.get("isAdr")
                             else "other" if p.get("isFund") else "common_stock")
        country = clean_text(p.get("country"))
        rec.country = country if country and len(country) == 2 and country.isalpha() and country.isupper() else None

    # ---- 首个交易日 ----
    rec.first_trade_date = min(ipo, cand.first_add) if ipo else cand.first_add
    if not ipo:
        rec.reasons.append("首个交易日为近似值（用首次进入指数的日期）")

    # ---- 状态、最后交易日 ----
    if override:
        last_to = max((o["valid_to"] for o in override), default=None, key=lambda d: d or date.max)
        if last_to is not None and all(o["valid_to"] for o in override):
            rec.status, rec.last_trade_date = "delisted", last_to - timedelta(days=1)
    elif cand.in_index:
        rec.status = "active"
    elif p and p.get("isActivelyTrading"):
        rec.status = "active"                            # 调出指数但仍在交易（如降到 S&P 400）
    elif last_price_date and last_price_date >= today - timedelta(days=STALE_PRICE_DAYS) and p:
        rec.status = "active"
    else:
        rec.status = "delisted"
        if last_price_date and cand.use_profile and (p or not profile):     # 代码被重复使用的：日线是别家的，不用
            rec.last_trade_date = last_price_date
        else:
            rec.last_trade_date = cand.last_end - timedelta(days=1)
            rec.reasons.append("最后交易日为近似值（调出指数前一天）")

    if rec.status == "active" and p:
        rec.primary_exchange = EXCHANGE_TO_MIC.get(clean_text(p.get("exchange")) or "")

    # ---- 退市原因、名称兜底 ----
    acq = match_acquisition(cand, acquisitions, rec.last_trade_date) if rec.status == "delisted" else None
    if acq:
        rec.delisting_reason = "acquired"
        if not p:
            rec.name = acq.get("target_name") or rec.name
    if rec.name == cand.anchor:
        # 只用这只证券存续期间的代码变更（代码后来给了别家时，之后的变更是别家的名字）
        end = cand.last_end or today
        names = [c.get("company_name") for c in symbol_changes if c["new_symbol"] in cand.symbols and c.get("company_name")
                 and cand.first_add - timedelta(days=RENAME_TOLERANCE_DAYS) <= c["change_date"] <= end]
        rec.name = names[-1] if names else rec.name

    # ---- CIK ----
    profile_cik = to_cik(p.get("cik")) if p else None
    cik = profile_cik
    if rec.status == "active" and sec_cik:
        if profile_cik and profile_cik != sec_cik:
            rec.reasons.append(f"CIK 不一致：FMP {profile_cik}，SEC {sec_cik}")
        cik = cik or sec_cik
    elif rec.status == "active" and not sec_cik and not override:
        rec.reasons.append("SEC 代码表里查不到这个代码，CIK 没有交叉核对")
    manual_cik = next((o["cik"] for o in override if o.get("cik")), None) if override else None
    cik = manual_cik or cik
    if not cik and acq and acq.get("target_cik"):
        cik = acq["target_cik"]
    if not cik:
        rec.reasons.append("没有 CIK")

    # ---- 代码历史 ----
    end_exclusive = rec.last_trade_date + timedelta(days=1) if rec.last_trade_date else None
    if override:
        rec.share_class = next((o["share_class"] for o in override if o["share_class"]), None)
        sym_periods = [(o["symbol"], o["valid_from"], o["valid_to"], "manual") for o in sorted(override, key=lambda o: o["valid_from"])]
        rec.first_trade_date = sym_periods[0][1]
        rec.name = next((o["name"] for o in override if o.get("name")), rec.name)
        rec.review_status, rec.reasons = "verified", [f"人工对照：{override[0]['note']}"]
    else:
        segs = symbol_segments(cand, symbol_changes, rec.first_trade_date)
        sym_periods = []
        for k, (sym, start) in enumerate(segs):
            vf = start
            vt = segs[k + 1][1] if k + 1 < len(segs) else end_exclusive
            sym_periods.append((sym, vf, vt, "sp500_github" if sym in cand.symbols else "fmp_symbol_change"))
    for sym, vf, vt, src in sym_periods:
        if vt is None or vt > vf:
            rec.identifiers.append({"id_type": "symbol", "id_value": sym, "valid_from": vf, "valid_to": vt, "source": src})

    ids_from = ipo if late_ipo else rec.first_trade_date            # 上市日存疑：CIK / CUSIP / ISIN 只保证 profile 上市日之后
    ids_to = None if rec.status == "active" else end_exclusive
    for id_type, value, src in [("cik", cik, "manual" if cik == manual_cik else "fmp_profile" if cik == profile_cik
                                 else "sec" if cik == sec_cik else "fmp_acquisition"),
                                ("cusip", clean_text(p.get("cusip")) if p else None, "fmp_profile"),
                                ("isin", clean_text(p.get("isin")) if p else None, "fmp_profile")]:
        if value and (ids_to is None or ids_to > ids_from):
            rec.identifiers.append({"id_type": id_type, "id_value": value, "valid_from": ids_from, "valid_to": ids_to, "source": src})

    if rec.review_status != "verified":
        rec.review_status = "needs_review" if rec.reasons else "auto"
    return rec


# =========================
# 5. 读输入 / 拉外部数据 / 写库
# =========================
def load_inputs(conn) -> dict:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        index_events = cur.execute("SELECT event_id, symbol, effective_date, action "
                                   "FROM events.v_sp500_index_changes_current").fetchall()
        changes = cur.execute("SELECT old_symbol, new_symbol, change_date, company_name "
                              "FROM events.v_symbol_changes_current ORDER BY change_date").fetchall()
        acqs = cur.execute("SELECT target_symbol, target_name, target_cik, filing_date "
                           "FROM events.v_acquisitions_current").fetchall()
    return {"index_events": index_events, "symbol_changes": changes, "acquisitions": acqs}


def fetch_profiles(meta: MetaStore, symbols: list[str], fetcher_cls=FetchData) -> dict[str, dict]:
    with meta.ingestion_run("profile", symbols) as run:
        with fetcher_cls(symbols, "profile", None) as fetcher:
            df, summary, succeed, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))
    if df.empty:
        return {}
    df = df.drop(columns=["endpoint"], errors="ignore")
    return {to_symbol(r["symbol"]) or r["symbol"]: r for r in df.to_dict(orient="records")}


def fetch_sec_ciks() -> dict[str, str]:
    """SEC 官方 代码 → CIK（当前在交易的）；键用项目内部写法"""
    r = requests.get(SEC_TICKERS_URL, headers={"User-Agent": os.environ["SEC_USER_AGENT"]}, timeout=60)
    r.raise_for_status()
    data = r.json()
    fields = data["fields"]
    out = {}
    for row in data["data"]:
        rec = dict(zip(fields, row))
        sym = to_symbol(rec.get("ticker"))
        if sym:
            out.setdefault(sym, to_cik(rec.get("cik")))
    return out


def fetch_last_price_dates(meta: MetaStore, cands: list[Candidate], today: date, fetcher_cls=FetchData) -> dict[str, date]:
    """退市候选：从调出指数前一段时间拉到今天，取日线最后一天"""
    out = {}
    if not cands:
        return out
    with meta.ingestion_run("price_volume", [c.anchor for c in cands], {"purpose": "security_master_last_trade_date"}) as run:
        n_ok, n_missing, failed_all = 0, 0, []
        for c in cands:
            params = {"from_date": c.last_end - timedelta(days=45), "to_date": today}
            with fetcher_cls([c.anchor], "price_volume", params) as fetcher:
                df, summary, succeed, failed = fetcher.fetch_fmp_batch()
            failed_all += failed
            if not df.empty:
                out[c.anchor] = max(to_date(d) for d in df["date"])
                n_ok += 1
            else:
                n_missing += 1
        run.finish({"success": n_ok, "missing": n_missing, "failed": len(failed_all)}, failed_all)
    return out


def write_records(conn, records: list[SecurityRecord], rebuild: bool = False) -> dict:
    with conn.transaction(), conn.cursor() as cur:
        n_existing = cur.execute("SELECT count(*) FROM ref.securities").fetchone()[0]
        if n_existing:
            if not rebuild:
                raise RuntimeError(f"证券主表已有 {n_existing} 只证券；要重新生成请加 --rebuild")
            used = cur.execute("SELECT (SELECT count(*) FROM events.corporate_events WHERE security_id IS NOT NULL)"
                               " + (SELECT count(*) FROM events.acquisitions WHERE acquirer_security_id IS NOT NULL)").fetchone()[0]
            if used:
                raise RuntimeError(f"已有 {used} 条事件回填了 security_id，不能再清空重建")
            cur.execute("DELETE FROM ref.security_identifiers")
            cur.execute("DELETE FROM ref.securities")

        conflicts = sum(insert_record(conn, cur, rec)[1] for rec in records)
    return {"securities": len(records), "identifier_conflicts": conflicts}


def insert_record(conn, cur, rec: SecurityRecord) -> tuple[int, int]:
    """
    插入一只证券和它的代码历史（在调用方的事务里）；返回 (security_id, 冲突数)
    单条代码冲突（排除约束）只跳过那一条，证券标 needs_review、原因写进 notes
    """
    sid = cur.execute(
        "INSERT INTO ref.securities (name, security_type, share_class, country, primary_exchange, "
        "first_trade_date, last_trade_date, status, delisting_reason, review_status, notes) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING security_id",
        (rec.name, rec.security_type, rec.share_class, rec.country, rec.primary_exchange,
         rec.first_trade_date, rec.last_trade_date, rec.status, rec.delisting_reason,
         rec.review_status, None)).fetchone()[0]
    conflicts = 0
    for ident in rec.identifiers:
        try:
            with conn.transaction():
                cur.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, valid_to, source, event_id) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (sid, ident["id_type"], ident["id_value"], ident["valid_from"], ident["valid_to"], ident["source"],
                             ident.get("event_id")))
        except (psycopg.errors.ExclusionViolation, psycopg.errors.CheckViolation) as e:
            conflicts += 1
            rec.reasons.append(f"{ident['id_type']} {ident['id_value']} 写入冲突：{str(e).splitlines()[0][:80]}")
            if rec.review_status == "auto":
                rec.review_status = "needs_review"
    cur.execute("UPDATE ref.securities SET review_status = %s, notes = %s WHERE security_id = %s",
                (rec.review_status, "；".join(rec.reasons) or None, sid))
    return sid, conflicts


def write_report(records: list[SecurityRecord], path=REPORT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["anchor", "name", "status", "review_status", "first_trade_date", "last_trade_date",
                    "symbols", "first_add", "last_end", "reasons"])
        for r in sorted(records, key=lambda r: (r.review_status != "needs_review", r.anchor)):
            c = r.candidate
            w.writerow([r.anchor, r.name, r.status, r.review_status, r.first_trade_date, r.last_trade_date,
                        " → ".join(i["id_value"] for i in r.identifiers if i["id_type"] == "symbol"),
                        c.first_add if c else "", c.last_end if c else "", "；".join(r.reasons)])


# =========================
# 一键生成
# =========================
def load_inputs_fixed(conn) -> dict:
    """load_inputs + 人工修正：去掉要忽略的代码变更，带上人工串联"""
    inputs = load_inputs(conn)
    manual_links, ignore = load_change_fixes()
    inputs["symbol_changes"] = [c for c in inputs["symbol_changes"]
                                if (c["old_symbol"], c["new_symbol"], c["change_date"]) not in ignore]
    inputs["manual_links"] = manual_links
    return inputs


def final_candidates(inputs: dict, profiles: dict, overrides: dict) -> list[Candidate]:
    """串联 → 拆开代码被重复使用的 → 按人工对照拆分；生成主表和回填事件用的是同一套"""
    cands = build_candidates(build_intervals(inputs["index_events"]), inputs["symbol_changes"],
                             manual_links=inputs["manual_links"])
    return expand_overrides(split_reused(cands, profiles), overrides)


def _load_cache(today: date) -> dict:
    try:
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if cache.get("date") != str(today):
        log.warning(f"使用 {cache.get('date')} 的缓存（profile、退市证券最后交易日），不是今天的")
    return cache


def _save_cache(today: date, profiles: dict, last_prices: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps({"date": str(today), "profiles": profiles,
                                      "last_prices": {k: str(v) for k, v in last_prices.items()}},
                                     ensure_ascii=False, default=str), encoding="utf-8")


def build(*, meta: MetaStore | None = None, rebuild: bool = False, today: date | None = None,
          fetcher_cls=FetchData, sec_ciks: dict | None = None, overrides: dict | None = None,
          use_cache: bool = False) -> tuple[list[SecurityRecord], dict]:
    meta = meta or MetaStore()
    today = today or date.today()
    inputs = load_inputs_fixed(meta.conn)
    overrides = overrides if overrides is not None else load_overrides()
    cache = _load_cache(today) if use_cache else {}
    profiles = cache.get("profiles")
    if not profiles:
        anchors = [c.anchor for c in build_candidates(build_intervals(inputs["index_events"]), inputs["symbol_changes"],
                                                      manual_links=inputs["manual_links"])]
        profiles = fetch_profiles(meta, anchors, fetcher_cls)
    candidates = final_candidates(inputs, profiles, overrides)
    log.info(f"串联完成：{len(candidates)} 只证券候选（已拆开代码被重复使用的）")

    sec = sec_ciks if sec_ciks is not None else fetch_sec_ciks()

    # 需要查最后交易日的：已调出指数、没有人工对照、不是拆出来的老公司，且 profile 查不到或已不在交易
    # （拆出来的老公司不查：按这个代码拉回来的是后来那家公司的日线）
    def needs_price(c):
        if c.in_index or c.anchor in overrides or not c.use_profile:
            return False
        prof = profiles.get(c.anchor)
        return prof is None or (profile_is_valid(prof, c)[0] and not prof.get("isActivelyTrading"))
    need_price = [c for c in candidates if needs_price(c)]
    if "last_prices" in cache:
        last_prices = {k: date.fromisoformat(v) for k, v in cache["last_prices"].items()}
    else:
        last_prices = fetch_last_price_dates(meta, need_price, today, fetcher_cls)
    if use_cache and not cache:                          # 新拉的才存；用的是旧缓存就不覆盖（日期保持原样）
        _save_cache(today, profiles, last_prices)

    records = [build_record(c, profile=profiles.get(c.anchor), sec_cik=sec.get(c.anchor) if c.use_profile else None,
                            last_price_date=last_prices.get(c.anchor), acquisitions=inputs["acquisitions"],
                            symbol_changes=inputs["symbol_changes"], overrides=overrides, today=today)
               for c in candidates]
    stats = write_records(meta.conn, records, rebuild=rebuild)
    write_report(records)
    return records, stats


def main():
    parser = argparse.ArgumentParser(description="由事件生成证券主表")
    parser.add_argument("command", choices=["build"])
    parser.add_argument("--rebuild", action="store_true", help="清空后重新生成（事件还没回填 security_id 时才允许）")
    parser.add_argument("--use-cache", action="store_true", help="用当天缓存的 profile / 最后交易日，不重新请求 FMP")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("fetch_data.fetch").setLevel(logging.ERROR)
    logging.getLogger("db").setLevel(logging.WARNING)

    records, stats = build(rebuild=args.rebuild, use_cache=args.use_cache)
    from collections import Counter
    print(f"证券数：{stats['securities']}，代码写入冲突：{stats['identifier_conflicts']}")
    print("状态：", dict(Counter(r.status for r in records)))
    print("核对：", dict(Counter(r.review_status for r in records)))
    print("待核对原因：", dict(Counter(reason.split("：")[0] for r in records for reason in r.reasons if r.review_status == "needs_review")))
    print(f"明细报告：{REPORT_PATH}")


if __name__ == "__main__":
    main()
