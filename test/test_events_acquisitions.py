import json
from datetime import date

import psycopg
import pytest

"""

收购明细 events.acquisitions 测试：中立结构（一笔交易一条）、两个视角的视图、
公告 → 完成的版本、acquirer_security_id 只能回填一次
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


@pytest.fixture
def batch(store):
    with store.ingestion_run("mergers_acquisitions", ["AAA"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record_acq(conn, batch_id, target, filing, n, completion=None, acquirer="AAA"):
    deal_key = f"{n:010d}|0000000001"
    return conn.execute(
        "SELECT * FROM events.record_acquisition(p_target_symbol => %s, p_filing_date => %s, p_deal_key => %s, "
        "p_source => 'fmp', p_batch_id => %s, p_payload => %s, p_completion_date => %s, "
        "p_acquirer_symbol => %s, p_acquirer_cik => '0000000001', p_target_cik => %s)",
        (target, filing, deal_key, batch_id, json.dumps({"target": target, "completion": str(completion)}),
         completion, acquirer, f"{n:010d}"),
    ).fetchone()


@pytest.fixture
def a_buys_four(conn, batch):
    """A 收购 B、C、D、E；B 已完成"""
    for n, (t, d) in enumerate([("BBB", date(2024, 1, 10)), ("CCC", date(2024, 3, 10)),
                                ("DDD", date(2024, 5, 10)), ("EEE", date(2024, 7, 10))], start=2):
        record_acq(conn, batch, t, d, n)
    record_acq(conn, batch, "BBB", date(2024, 1, 10), 2, completion=date(2024, 6, 28))
    a_id = conn.execute("INSERT INTO ref.securities (name, security_type) VALUES ('Company A', 'common_stock') "
                        "RETURNING security_id").fetchone()[0]
    return a_id


def test_one_event_per_deal(conn, a_buys_four):
    """中立结构：4 笔交易在总表里是 4 个事件（B 有公告、完成两个版本）"""
    n_events = conn.execute("SELECT count(*) FROM events.v_corporate_events_current "
                            "WHERE event_type = 'acquisition'").fetchone()[0]
    assert n_events == 4


def test_announced_then_completed(conn, a_buys_four):
    rows = {r[0]: r[1:] for r in conn.execute(
        "SELECT symbol, status, revision, event_date FROM events.v_corporate_events_current "
        "WHERE event_type = 'acquisition'")}
    assert rows["BBB"] == ("confirmed", 2, date(2024, 6, 28))           # 完成后：event_date = 完成日
    assert rows["CCC"] == ("announced", 1, date(2024, 3, 10))           # 公告阶段：event_date = 文件日


def test_two_perspectives(conn, a_buys_four):
    conn.execute("UPDATE events.acquisitions SET acquirer_security_id = %s WHERE acquirer_symbol = 'AAA'", (a_buys_four,))
    as_acq = conn.execute("SELECT counterparty_symbol FROM events.v_acquisitions_as_acquirer "
                          "WHERE security_id = %s ORDER BY effective_date", (a_buys_four,)).fetchall()
    assert [r[0] for r in as_acq] == ["CCC", "DDD", "BBB", "EEE"]       # 收购方视角：A 收购了 4 家
    as_tgt = conn.execute("SELECT counterparty_symbol, role, completion_date FROM events.v_acquisitions_as_target "
                          "WHERE symbol = 'BBB'").fetchall()
    assert as_tgt == [("AAA", "target", date(2024, 6, 28))]             # 被收购方视角：B 被 A 收购


def test_acquirer_security_backfill_once(conn, a_buys_four):
    conn.execute("UPDATE events.acquisitions SET acquirer_security_id = %s WHERE acquirer_symbol = 'AAA'", (a_buys_four,))
    with pytest.raises(psycopg.errors.RaiseException, match="只能从空回填"):
        conn.execute("UPDATE events.acquisitions SET acquirer_security_id = NULL")


@pytest.mark.parametrize("sql, message", [
    ("UPDATE events.acquisitions SET target_name = 'X'", "只能回填 acquirer_security_id"),
    ("DELETE FROM events.acquisitions", "不允许删除"),
])
def test_append_only(conn, a_buys_four, sql, message):
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        conn.execute(sql)


def test_duplicate_delivery(conn, batch):
    assert record_acq(conn, batch, "BBB", date(2024, 1, 10), 2)[2] == "inserted"
    assert record_acq(conn, batch, "BBB", date(2024, 1, 10), 2)[2] == "duplicate"


def test_cik_format(conn, batch):
    with pytest.raises(psycopg.errors.CheckViolation, match="chk_acquisitions_target_cik_format"):
        conn.execute("SELECT * FROM events.record_acquisition(p_target_symbol => 'XX', p_filing_date => '2024-01-10', "
                     "p_deal_key => 'k', p_source => 'fmp', p_batch_id => %s, p_payload => '{}', p_target_cik => '123')",
                     (batch,))
    assert conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0] == 0


def test_other_detail_tables_still_fully_immutable(conn, batch):
    """通用触发器加了"可回填列"参数后，没有传这个参数的明细表仍然完全不能改"""
    conn.execute("SELECT * FROM events.record_split('AAPL','2020-08-31',4,1,'fmp','sp',%s,'{}')", (batch,))
    with pytest.raises(psycopg.errors.RaiseException, match="不允许修改"):
        conn.execute("UPDATE events.splits SET numerator = 8")
