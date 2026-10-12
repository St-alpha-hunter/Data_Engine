from datetime import date

from psycopg.types.json import Jsonb

from pipelines.security_maintenance import (
    apply_delisting, apply_new_security, apply_rename, plan_delistings, plan_new_securities, plan_renames,
)

"""

证券主表增量维护：改名、新调入、退市（测试库）

"""

TODAY = date(2026, 10, 12)
D = date.fromisoformat


def batch(store):
    with store.ingestion_run("x", ["A"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def security(conn, symbol, start="2000-01-03", name=None):
    sid = conn.execute("INSERT INTO ref.securities (name, security_type, first_trade_date, status, review_status) "
                       "VALUES (%s, 'common_stock', %s, 'active', 'auto') RETURNING security_id", (name or symbol, D(start))).fetchone()[0]
    conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source) "
                 "VALUES (%s, 'symbol', %s, %s, 'manual')", (sid, symbol, D(start)))
    return sid


def symbol_change(conn, b, old, new, d):
    return conn.execute("SELECT event_id FROM events.record_symbol_change(%s, %s, %s, 'fmp', %s, %s, %s)",
                        (D(d), old, new, f"{old}-{new}", b, Jsonb({}))).fetchone()[0]


def index_change(conn, b, symbol, d, action, sid=None):
    return conn.execute("SELECT event_id FROM events.record_sp500_index_change(%s, %s, %s, 'sp500_github', %s, %s, "
                        "p_source_event_key => %s, p_security_id => %s)",
                        (symbol, D(d), action, b, Jsonb({}), f"{symbol}-{d}-{action}", sid)).fetchone()[0]


def symbols_of(conn, sid):
    return conn.execute("SELECT id_value, valid_from, valid_to FROM ref.security_identifiers "
                        "WHERE security_id = %s AND id_type = 'symbol' ORDER BY valid_from", (sid,)).fetchall()


# =========================
# ✅ 改名
# =========================
def test_rename(conn, store):
    sid = security(conn, "OLD")
    eid = symbol_change(conn, batch(store), "OLD", "NEW", "2026-10-01")
    actions = plan_renames(conn, ignore=set())
    assert [(a.kind, a.security_id, a.event_id) for a in actions] == [("rename", sid, eid)]
    apply_rename(conn, actions[0])
    assert symbols_of(conn, sid) == [("OLD", D("2000-01-03"), D("2026-10-01")), ("NEW", D("2026-10-01"), None)]
    assert conn.execute("SELECT security_id FROM events.corporate_events WHERE event_id = %s", (eid,)).fetchone()[0] == sid
    assert plan_renames(conn, ignore=set()) == []                              # 重复执行：没有新动作


def test_rename_skipped_when_new_symbol_taken_or_ignored(conn, store):
    security(conn, "OLD")
    security(conn, "NEW", name="别的公司")
    b = batch(store)
    symbol_change(conn, b, "OLD", "NEW", "2026-10-01")
    assert [a.kind for a in plan_renames(conn, ignore=set())] == ["skip"]
    assert plan_renames(conn, ignore={("OLD", "NEW", D("2026-10-01"))}) == []


def test_historical_change_of_closed_symbol_ignored(conn, store):
    sid = security(conn, "OLD")
    conn.execute("UPDATE ref.security_identifiers SET valid_to = '2015-01-02' WHERE security_id = %s", (sid,))
    symbol_change(conn, batch(store), "OLD", "XYZ", "2015-01-02")
    assert plan_renames(conn, ignore=set()) == []


# =========================
# ✅ 新调入
# =========================
PROFILE = {"symbol": "NEWCO", "companyName": "NewCo Inc.", "cik": "0000123456", "isin": "US1234567890",
           "cusip": "123456789", "exchange": "NYSE", "ipoDate": "2015-06-01", "country": "US",
           "isEtf": False, "isAdr": False, "isFund": False, "isActivelyTrading": True}


def test_new_security(conn, store):
    b = batch(store)
    eid = index_change(conn, b, "NEWCO", "2026-09-22", "add")
    index_change(conn, b, "OLDCO", "2008-01-02", "add")                        # 2010 年以前：不新建
    actions = plan_new_securities(conn, store, TODAY, profiles={"NEWCO": PROFILE}, sec_ciks={"NEWCO": "0000123456"})
    assert [(a.kind, a.symbol, a.event_id) for a in actions] == [("new_security", "NEWCO", eid)]
    sid = apply_new_security(conn, actions[0])
    row = conn.execute("SELECT name, status, first_trade_date, primary_exchange, review_status FROM ref.securities "
                       "WHERE security_id = %s", (sid,)).fetchone()
    assert row == ("NewCo Inc.", "active", D("2015-06-01"), "XNYS", "auto")
    assert symbols_of(conn, sid) == [("NEWCO", D("2015-06-01"), None)]
    assert conn.execute("SELECT security_id FROM events.corporate_events WHERE event_id = %s", (eid,)).fetchone()[0] == sid
    assert plan_new_securities(conn, store, TODAY, profiles={}, sec_ciks={}) == []


def test_readded_existing_security_not_created(conn, store):
    security(conn, "AAA")
    index_change(conn, batch(store), "AAA", "2026-09-22", "add")
    assert plan_new_securities(conn, store, TODAY, profiles={}, sec_ciks={}) == []


# =========================
# ✅ 退市
# =========================
def test_delisting(conn, store):
    b = batch(store)
    gone, live, nodata, member = (security(conn, s) for s in ("GONE", "LIVE", "NODATA", "MEMBER"))
    for sid, sym in [(gone, "GONE"), (live, "LIVE"), (nodata, "NODATA")]:
        index_change(conn, b, sym, "2012-01-03", "add", sid)
        index_change(conn, b, sym, "2026-06-01", "remove", sid)
    index_change(conn, b, "MEMBER", "2012-01-03", "add", member)
    conn.execute("SELECT * FROM events.record_acquisition('GONE', '2025-11-03', 'deal-1', 'fmp', %s, %s, "
                 "p_acquirer_name => 'BigCo', p_security_id => %s)", (b, Jsonb({}), gone))

    prices = {"GONE": D("2026-08-28"), "LIVE": D("2026-10-09"), "NODATA": None, "MEMBER": D("2020-01-02")}
    actions = plan_delistings(conn, TODAY, prices)
    assert sorted((a.kind, a.symbol) for a in actions) == [("delist", "GONE"), ("skip", "NODATA")]   # 还在指数里的不查
    apply_delisting(conn, next(a for a in actions if a.kind == "delist"))
    assert conn.execute("SELECT status, last_trade_date, delisting_reason FROM ref.securities WHERE security_id = %s",
                        (gone,)).fetchone() == ("delisted", D("2026-08-28"), "acquired")
    assert symbols_of(conn, gone) == [("GONE", D("2000-01-03"), D("2026-08-29"))]
    assert [a.kind for a in plan_delistings(conn, TODAY, prices)] == ["skip"]     # 已退市的不再查
