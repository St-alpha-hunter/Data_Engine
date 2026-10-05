import json
from datetime import date

import psycopg
import pytest

"""

代码变更明细 events.symbol_changes 测试
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


@pytest.fixture
def batch(store):
    with store.ingestion_run("symbol_change", ["FB"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record_change(conn, batch_id, old="FB", new="META", d=date(2022, 6, 9), company="Meta Platforms, Inc.", key=None):
    return conn.execute(
        "SELECT * FROM events.record_symbol_change(%s, %s, %s, 'fmp', %s, %s, %s, %s)",
        (d, old, new, key or f"{old}|{new}|{d}", batch_id,
         json.dumps({"oldSymbol": old, "newSymbol": new, "date": str(d)}), company),
    ).fetchone()


def test_record_and_main_symbol_is_old(conn, batch):
    assert record_change(conn, batch)[1:] == (1, "inserted")
    assert record_change(conn, batch)[1:] == (1, "duplicate")
    row = conn.execute("SELECT e.symbol, c.old_symbol, c.new_symbol, c.company_name "
                       "FROM events.corporate_events e JOIN events.symbol_changes c USING (event_id)").fetchone()
    assert row == ("FB", "FB", "META", "Meta Platforms, Inc.")          # 总表 symbol = 旧代码
    cur = conn.execute("SELECT old_symbol, new_symbol, change_date FROM events.v_symbol_changes_current").fetchall()
    assert cur == [("FB", "META", date(2022, 6, 9))]


def test_invalid_date_not_blocked(conn, batch):
    """1969-12-31 这类无效日期不在表里拦截，导入时作为质量问题处理"""
    assert record_change(conn, batch, "ABC", "ABCD", date(1969, 12, 31), None)[2] == "inserted"


@pytest.mark.parametrize("old, new, constraint", [
    ("FB", "FB", "chk_symbol_changes_different"),
    ("FB", "meta", "chk_symbol_changes_new_format"),
])
def test_constraints_and_atomicity(conn, batch, old, new, constraint):
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        record_change(conn, batch, old, new)
    assert conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0] == 0


def test_old_symbol_format_checked_by_main_table(conn, batch):
    """旧代码同时是总表的 symbol，格式不对时总表先拒绝"""
    with pytest.raises(psycopg.errors.CheckViolation, match="chk_corporate_events_symbol_format"):
        record_change(conn, batch, "fb", "META")


@pytest.mark.parametrize("sql, message", [
    ("UPDATE events.symbol_changes SET new_symbol = 'MVRS'", "不允许修改"),
    ("DELETE FROM events.symbol_changes", "不允许删除"),
])
def test_append_only(conn, batch, sql, message):
    record_change(conn, batch)
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        conn.execute(sql)


def test_type_must_be_symbol_change(conn, batch):
    div_id = conn.execute("SELECT event_id FROM events.record_event('dividend','AAPL','2022-06-09','confirmed',"
                          "'fmp','d1',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match="不是 symbol_change"):
        conn.execute("INSERT INTO events.symbol_changes (event_id, change_date, old_symbol, new_symbol) "
                     "VALUES (%s, '2022-06-09', 'A', 'B')", (div_id,))
