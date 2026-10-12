from datetime import date

import pytest

from pipelines.security_master import (
    Candidate, Interval, SecurityRecord, build_candidates, build_intervals, build_record,
    expand_overrides, profile_is_valid, split_reused, symbol_segments, write_records,
)

"""

证券主表生成测试：串联、补改名、profile 核对、各种情况下的记录组装（纯函数），以及写库

"""

TODAY = date(2026, 10, 10)
D = date.fromisoformat


def ev(symbol, d, action, event_id=None):
    return {"symbol": symbol, "effective_date": D(d), "action": action, "event_id": event_id}


def chg(old, new, d, name=None):
    return {"old_symbol": old, "new_symbol": new, "change_date": D(d), "company_name": name}


# =========================
# ✅ 成分区间与串联
# =========================
def test_build_intervals():
    ivs = build_intervals([ev("FB", "2013-12-23", "add"), ev("FB", "2022-06-09", "remove"),
                           ev("META", "2022-06-09", "add"), ev("X", "2000-01-03", "add"),
                           ev("X", "2005-01-03", "remove"), ev("X", "2012-01-03", "add")])
    got = sorted((i.symbol, str(i.start), str(i.end)) for i in ivs)
    assert got == [("FB", "2013-12-23", "2022-06-09"), ("META", "2022-06-09", "None"),
                   ("X", "2000-01-03", "2005-01-03"), ("X", "2012-01-03", "None")]


def test_rename_chain_requires_same_day_remove_and_add():
    ivs = build_intervals([ev("FB", "2013-12-23", "add"), ev("FB", "2022-06-09", "remove"),
                           ev("META", "2022-06-09", "add"),
                           ev("OLD", "2000-01-03", "add"), ev("OLD", "2015-01-02", "remove"),
                           ev("NEW", "2018-01-02", "add")])                # 改名记录日期对不上调出 / 调入
    cands = build_candidates(ivs, [chg("FB", "META", "2022-06-09"), chg("OLD", "NEW", "2015-01-02")])
    by_anchor = {c.anchor: c for c in cands}
    assert by_anchor["META"].symbols == ["FB", "META"]                    # 串成同一只
    assert "OLD" in by_anchor and "NEW" in by_anchor                       # NEW 调入日不在改名日附近：不串


def test_universe_filter_and_reentry():
    ivs = build_intervals([ev("WB", "1996-01-02", "add"), ev("WB", "2009-01-02", "remove"),     # 2010 年前调出
                           ev("X", "2000-01-03", "add"), ev("X", "2005-01-03", "remove"),
                           ev("X", "2012-01-03", "add")])                                         # 调出后又调入
    cands = build_candidates(ivs, [])
    assert [c.anchor for c in cands] == ["X"]
    assert len(cands[0].intervals) == 2 and cands[0].in_index


def test_manual_link():
    """HRS → LHX：调出 / 调入日和改名日差一个月，自动串联串不上，人工串联"""
    ivs = build_intervals([ev("HRS", "2008-09-22", "add"), ev("HRS", "2019-06-01", "remove"), ev("LHX", "2019-06-01", "add")])
    assert len(build_candidates(ivs, [chg("HRS", "LHX", "2019-07-01")])) == 2
    cands = build_candidates(ivs, [chg("HRS", "LHX", "2019-07-01")], manual_links=[("HRS", "LHX")])
    assert [c.symbols for c in cands] == [["HRS", "LHX"]]


# =========================
# ✅ 补改名
# =========================
def cand(*intervals):
    return Candidate([Interval(s, D(a), D(b) if b else None) for s, a, b in intervals])


def test_segments_backfill_chain_inside_interval():
    """Anthem：数据集只有 ANTM / ELV，FMP 代码变更里还有 ATH → WLP → ANTM"""
    c = cand(("ANTM", "2002-07-25", "2022-06-28"), ("ELV", "2022-06-28", None))
    changes = [chg("ATH", "WLP", "2004-11-30"), chg("WLP", "ANTM", "2014-11-12"), chg("ANTM", "ELV", "2022-06-28")]
    assert symbol_segments(c, changes) == [("ATH", D("2002-07-25")), ("WLP", D("2004-11-30")),
                                           ("ANTM", D("2014-11-12")), ("ELV", D("2022-06-28"))]


def test_segments_ignore_change_after_removal():
    """BBBY 2023 年后被别的公司拿去用：调出指数后的改名不算"""
    c = cand(("BBBY", "1999-10-01", "2017-07-26"))
    assert symbol_segments(c, [chg("BYON", "BBBY", "2025-08-29")]) == [("BBBY", D("1999-10-01"))]


def test_segments_first_start_and_rename_before_index_entry():
    """上市后、进入指数前就改过名：第一段从上市日开始，改名前用旧代码"""
    c = cand(("NEW", "2015-01-02", None))
    assert symbol_segments(c, [chg("OLD", "NEW", "2010-06-01")], first_start=D("2005-03-01")) == \
           [("OLD", D("2005-03-01")), ("NEW", D("2010-06-01"))]


# =========================
# ✅ profile 核对与记录组装
# =========================
PROFILE = {"symbol": "AAPL", "companyName": "Apple Inc.", "cik": "0000320193", "isin": "US0378331005",
           "cusip": "037833100", "exchange": "NASDAQ", "ipoDate": "1980-12-12", "country": "US",
           "isEtf": False, "isAdr": False, "isFund": False, "isActivelyTrading": True}


def rec_for(c, **kw):
    args = dict(profile=None, sec_cik=None, last_price_date=None, acquisitions=[], symbol_changes=[],
                overrides={}, today=TODAY)
    args.update(kw)
    return build_record(c, **args)


def test_profile_reuse_detected():
    c = cand(("FB", "2013-12-23", "2022-06-09"))
    ok, why = profile_is_valid({**PROFILE, "ipoDate": "2025-06-26", "companyName": "ProShares ETF"}, c)
    assert not ok and "重复使用" in why


def test_split_reused_ticker_in_gap():
    """HLT：老希尔顿 2007 年调出，新希尔顿 2013 年上市、2017 年调入 → 拆成两只；老的早于 2010 年调出，丢掉"""
    c = cand(("HLT", "1996-01-02", "2007-10-24"), ("HLT", "2017-06-19", None))
    out = split_reused([c], {"HLT": {**PROFILE, "symbol": "HLT", "ipoDate": "2013-12-12", "companyName": "Hilton"}})
    assert [(x.first_add, x.last_end, x.use_profile) for x in out] == [(D("2017-06-19"), None, True)]

    c = cand(("DELL", "1996-01-02", "2013-10-30"), ("DELL", "2024-09-23", None))
    old, new = split_reused([c], {"DELL": {**PROFILE, "symbol": "DELL", "ipoDate": "2016-08-17"}})
    assert (old.last_end, old.use_profile, new.first_add, new.use_profile) == (D("2013-10-30"), False, D("2024-09-23"), True)
    r = rec_for(old, profile={**PROFILE, "symbol": "DELL"}, sec_cik=None)
    assert (r.status, r.last_trade_date, r.review_status) == ("delisted", D("2013-10-29"), "needs_review")
    assert not any(i["id_type"] in ("cusip", "isin") for i in r.identifiers)   # 不用后来那家公司的 profile


def test_reentry_same_company_not_split():
    c = cand(("X", "2000-01-03", "2005-01-03"), ("X", "2012-01-03", None))
    assert split_reused([c], {"X": {**PROFILE, "ipoDate": "1990-01-02"}}) == [c]


def test_profile_ipo_after_removal_not_used():
    """APC 2019 年被收购退市，代码后来给了 ARKO（2026 年上市）：不能用 ARKO 的 profile"""
    c = cand(("APC", "1997-07-28", "2019-08-09"))
    r = rec_for(c, profile={**PROFILE, "symbol": "APC", "ipoDate": "2026-02-12", "isActivelyTrading": True})
    assert (r.status, r.last_trade_date, r.review_status) == ("delisted", D("2019-08-08"), "needs_review")
    assert any("重复使用" in x for x in r.reasons)
    assert not any(i["id_type"] in ("cusip", "isin") for i in r.identifiers)


def test_late_ipo_single_interval_ids_from_ipo():
    """ED：1996 年就在指数里，profile 上市日是 2001 年（重组成控股公司）"""
    r = rec_for(cand(("ED", "1996-01-02", None)), profile={**PROFILE, "ipoDate": "2001-01-02"}, sec_cik="0000320193")
    assert (r.status, r.first_trade_date, r.review_status) == ("active", D("1996-01-02"), "needs_review")
    froms = {i["id_type"]: i["valid_from"] for i in r.identifiers}
    assert froms == {"symbol": D("1996-01-02"), "cik": D("2001-01-02"), "cusip": D("2001-01-02"), "isin": D("2001-01-02")}


def test_active_auto():
    r = rec_for(cand(("AAPL", "1982-11-30", None)), profile=PROFILE, sec_cik="0000320193")
    assert (r.status, r.review_status, r.primary_exchange, r.first_trade_date) == ("active", "auto", "XNAS", D("1980-12-12"))
    ids = {(i["id_type"], i["id_value"]) for i in r.identifiers}
    assert ids == {("symbol", "AAPL"), ("cik", "0000320193"), ("cusip", "037833100"), ("isin", "US0378331005")}
    assert all(i["valid_to"] is None for i in r.identifiers)


def test_cik_mismatch_needs_review():
    r = rec_for(cand(("AAPL", "1982-11-30", None)), profile=PROFILE, sec_cik="0000999999")
    assert r.review_status == "needs_review" and any("CIK 不一致" in x for x in r.reasons)


def test_delisted_acquired():
    c = cand(("ATVI", "2005-01-03", "2023-10-18"))
    prof = {**PROFILE, "symbol": "ATVI", "companyName": "Activision Blizzard", "cik": "0000718877",
            "ipoDate": "1993-10-25", "isActivelyTrading": False}
    acq = [{"target_symbol": "ATVI", "target_name": "Activision Blizzard", "target_cik": "0000718877",
            "filing_date": D("2022-01-18")}]
    r = rec_for(c, profile=prof, last_price_date=D("2023-10-12"), acquisitions=acq)
    assert (r.status, r.last_trade_date, r.delisting_reason, r.review_status) == \
           ("delisted", D("2023-10-12"), "acquired", "auto")
    sym = next(i for i in r.identifiers if i["id_type"] == "symbol")
    assert sym["valid_to"] == D("2023-10-13")                              # 最后交易日的下一天（不含）


def test_delisted_no_profile_uses_acquisition_and_approx_date():
    c = cand(("CELG", "2005-01-03", "2019-11-21"))
    acq = [{"target_symbol": "CELG", "target_name": "Celgene", "target_cik": "0000816284", "filing_date": D("2019-02-22")}]
    r = rec_for(c, acquisitions=acq)
    assert (r.status, r.name, r.last_trade_date) == ("delisted", "Celgene", D("2019-11-20"))
    assert ("cik", "0000816284") in {(i["id_type"], i["id_value"]) for i in r.identifiers}
    assert r.review_status == "needs_review"                               # 没有 profile、最后交易日是近似值


def test_override_verified():
    overrides = {"AABA": [
        {"symbol": "YHOO", "valid_from": D("1996-04-12"), "valid_to": D("2017-06-19"), "share_class": None, "note": "雅虎"},
        {"symbol": "AABA", "valid_from": D("2017-06-19"), "valid_to": D("2019-10-03"), "share_class": None, "note": "Altaba"}]}
    r = rec_for(cand(("AABA", "1999-12-08", "2017-06-19")), overrides=overrides)
    assert (r.review_status, r.status, r.last_trade_date, r.first_trade_date) == \
           ("verified", "delisted", D("2019-10-02"), D("1996-04-12"))
    assert [(i["id_value"], i["valid_from"], i["valid_to"]) for i in r.identifiers if i["id_type"] == "symbol"] == \
           [("YHOO", D("1996-04-12"), D("2017-06-19")), ("AABA", D("2017-06-19"), D("2019-10-03"))]


def test_override_split_into_two_securities():
    """FOXA：同一天 21 世纪福克斯改代码退市、新福克斯接手 FOXA → 拆成两只，成分区间按期间切开"""
    def o(sec, sym, a, b, name=None, cik=None):
        return {"security": sec, "symbol": sym, "valid_from": D(a), "valid_to": D(b) if b else None,
                "share_class": "A", "note": sec, "name": name, "cik": cik}
    overrides = {"FOXA": [o("21CF", "NWSA", "2004-12-20", "2013-07-01", "21CF", "0001308161"),
                          o("21CF", "FOXA", "2013-07-01", "2019-03-19", "21CF", "0001308161"),
                          o("21CF", "TFCFA", "2019-03-19", "2019-03-20", "21CF", "0001308161"),
                          o("FOX_CORP", "FOXA", "2019-03-19", None)]}
    old, new = expand_overrides([cand(("FOXA", "2004-12-20", None))], overrides)
    assert ([(i.start, i.end) for i in old.intervals], old.use_profile) == ([(D("2004-12-20"), D("2019-03-20"))], False)
    assert ([(i.start, i.end) for i in new.intervals], new.use_profile) == ([(D("2019-03-19"), None)], True)
    r_old = rec_for(old, profile={**PROFILE, "symbol": "FOXA"}, overrides=overrides)
    assert (r_old.name, r_old.status, r_old.last_trade_date, r_old.review_status) == ("21CF", "delisted", D("2019-03-19"), "verified")
    assert ("cik", "0001308161") in {(i["id_type"], i["id_value"]) for i in r_old.identifiers}
    r_new = rec_for(new, profile={**PROFILE, "symbol": "FOXA", "companyName": "Fox Corporation"}, overrides=overrides)
    assert (r_new.name, r_new.status, r_new.first_trade_date) == ("Fox Corporation", "active", D("2019-03-19"))


# =========================
# ✅ 写库
# =========================
def test_write_records_and_conflict(conn):
    a = SecurityRecord(anchor="AAA", name="A Corp", identifiers=[
        {"id_type": "symbol", "id_value": "AAA", "valid_from": D("2000-01-03"), "valid_to": None, "source": "x"}])
    b = SecurityRecord(anchor="BBB", name="B Corp", identifiers=[
        {"id_type": "symbol", "id_value": "AAA", "valid_from": D("2010-01-04"), "valid_to": None, "source": "x"},   # 冲突
        {"id_type": "symbol", "id_value": "BBB", "valid_from": D("2010-01-04"), "valid_to": None, "source": "x"}])
    stats = write_records(conn, [a, b])
    assert stats == {"securities": 2, "identifier_conflicts": 1}
    rows = dict(conn.execute("SELECT name, review_status FROM ref.securities").fetchall())
    assert rows == {"A Corp": "auto", "B Corp": "needs_review"}
    assert conn.execute("SELECT count(*) FROM ref.security_identifiers").fetchone()[0] == 2   # 冲突那条跳过


def test_write_refuses_without_rebuild_and_after_backfill(conn, store):
    a = SecurityRecord(anchor="AAA", name="A Corp")
    write_records(conn, [a])
    with pytest.raises(RuntimeError, match="--rebuild"):
        write_records(conn, [a])
    assert write_records(conn, [a], rebuild=True)["securities"] == 1                         # 可以重建

    with store.ingestion_run("x", ["A"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    eid = conn.execute("SELECT event_id FROM events.record_event('split','AAA','2020-01-02','confirmed','fmp','k',%s,'{}')",
                       (run.batch_id,)).fetchone()[0]
    sid = conn.execute("SELECT security_id FROM ref.securities").fetchone()[0]
    conn.execute("UPDATE events.corporate_events SET security_id = %s WHERE event_id = %s", (sid, eid))
    with pytest.raises(RuntimeError, match="不能再清空重建"):
        write_records(conn, [a], rebuild=True)
