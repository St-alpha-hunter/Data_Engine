import json
from datetime import date

import psycopg
import pytest

"""

S&P 500 成分股变更明细 events.sp500_index_changes 测试
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


@pytest.fixture
def batch(store):
    with store.ingestion_run("sp500_github", ["SP500"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record_change(conn, batch_id, symbol, d, action, **kw):
    params = {"p_symbol": symbol, "p_effective_date": d, "p_action": action, "p_source": "sp500_github",
              "p_batch_id": batch_id, "p_payload": json.dumps({"ticker": symbol, "date": str(d), "action": action}),
              **{f"p_{k}": v for k, v in kw.items()}}
    names = ", ".join(f"{k} => %({k})s" for k in params)
    return conn.execute(f"SELECT * FROM events.record_sp500_index_change({names})", params).fetchone()


def test_add_remove_and_key(conn, batch):
    assert record_change(conn, batch, "FB", date(2013, 12, 23), "add")[2] == "inserted"
    assert record_change(conn, batch, "FB", date(2022, 6, 9), "remove")[2] == "inserted"
    assert record_change(conn, batch, "META", date(2022, 6, 9), "add")[2] == "inserted"
    assert record_change(conn, batch, "FB", date(2013, 12, 23), "add")[2] == "duplicate"
    rows = conn.execute("SELECT e.symbol, e.source_event_key, c.event_type, e.index_code, e.action "
                        "FROM events.v_sp500_index_changes_current e JOIN events.corporate_events c USING (event_id) "
                        "ORDER BY e.effective_date, e.symbol").fetchall()
    assert rows == [
        ("FB", "SP500|FB|add|2013-12-23", "index_change", "SP500", "add"),
        ("FB", "SP500|FB|remove|2022-06-09", "index_change", "SP500", "remove"),
        ("META", "SP500|META|add|2022-06-09", "index_change", "SP500", "add"),
    ]


def test_optional_fields(conn, batch):
    record_change(conn, batch, "META", date(2022, 6, 9), "add", company_name="Meta Platforms",
                  counterpart_symbol="FB", reason="symbol change")
    assert conn.execute("SELECT company_name, counterpart_symbol, reason FROM events.sp500_index_changes"
                        ).fetchone() == ("Meta Platforms", "FB", "symbol change")


def test_action_constraint_and_atomicity(conn, batch):
    with pytest.raises(psycopg.errors.CheckViolation, match="chk_sp500_index_changes_action"):
        record_change(conn, batch, "X", date(2022, 1, 3), "delete")
    assert conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0] == 0


def test_index_code_fixed(conn, batch):
    eid = conn.execute("SELECT event_id FROM events.record_event('index_change','Y','2022-01-03','confirmed',"
                       "'x','y',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.CheckViolation, match="chk_sp500_index_changes_index_code"):
        conn.execute("INSERT INTO events.sp500_index_changes (event_id, effective_date, index_code, action) "
                     "VALUES (%s, '2022-01-03', 'NDX', 'add')", (eid,))


@pytest.mark.parametrize("sql, message", [
    ("UPDATE events.sp500_index_changes SET reason = 'x'", "不允许修改"),
    ("DELETE FROM events.sp500_index_changes", "不允许删除"),
])
def test_append_only(conn, batch, sql, message):
    record_change(conn, batch, "FB", date(2013, 12, 23), "add")
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        conn.execute(sql)
