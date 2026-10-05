import json
from datetime import date
from decimal import Decimal

import psycopg
import pytest

"""

分红明细 events.dividends 测试：record_dividend 的写入 / 去重 / 修订、跨表检查、只追加、单事务
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


@pytest.fixture
def batch(store):
    with store.ingestion_run("dividends", ["AAPL"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record_div(conn, batch_id, amount=0.27, key="AAPL|2026-08-10|dividend", ex_date=date(2026, 8, 10), **kw):
    # 金额用 Decimal 传（精确，且能匹配函数的 numeric 参数；float 会变成 double precision）
    to_dec = lambda v: Decimal(str(v)) if isinstance(v, float) or isinstance(v, int) and not isinstance(v, bool) else v
    kw = {k: to_dec(v) if k == "adj_amount" else v for k, v in kw.items()}
    params = {"p_symbol": "AAPL", "p_ex_date": ex_date, "p_amount": to_dec(amount), "p_source": "fmp",
              "p_source_event_key": key, "p_batch_id": batch_id,
              "p_payload": json.dumps({"dividend": amount, **{k: str(v) for k, v in kw.items()}}),
              **{f"p_{k}": v for k, v in kw.items()}}
    names = ", ".join(f"{k} => %({k})s" for k in params)
    return conn.execute(f"SELECT * FROM events.record_dividend({names})", params).fetchone()


def test_insert_duplicate_revise(conn, batch):
    full = dict(record_date=date(2026, 8, 10), payment_date=date(2026, 8, 13),
                declaration_date=date(2026, 7, 30), frequency="Quarterly", adj_amount=0.27)
    assert record_div(conn, batch, **full)[1:] == (1, "inserted")
    assert record_div(conn, batch, **full)[1:] == (1, "duplicate")
    assert record_div(conn, batch, amount=0.28, frequency="Quarterly")[1:] == (2, "revised")

    assert conn.execute("SELECT count(*) FROM events.dividends").fetchone()[0] == 2      # 重复投递不写明细
    cur = conn.execute("SELECT revision, amount, dividend_type, currency, declaration_date "
                       "FROM events.v_dividends_current").fetchall()
    assert len(cur) == 1
    revision, amount, dtype, currency, decl = cur[0]
    assert (revision, float(amount), dtype, currency, decl) == (2, 0.28, "regular", "USD", None)


def test_special_dividend(conn, batch):
    record_div(conn, batch, amount=1.5, frequency="Special", dividend_type="special")
    assert conn.execute("SELECT dividend_type FROM events.dividends").fetchone()[0] == "special"


@pytest.mark.parametrize("kw, constraint", [
    ({"amount": 0}, "chk_dividends_amount_positive"),
    ({"adj_amount": -1}, "chk_dividends_adj_amount_positive"),
    ({"frequency": "Daily"}, "chk_dividends_frequency"),
    ({"dividend_type": "stock"}, "chk_dividends_type"),
    ({"currency": "usd"}, "chk_dividends_currency"),
])
def test_constraints_and_atomicity(conn, batch, kw, constraint):
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        record_div(conn, batch, **kw)
    # 明细写入失败，总表的事件也一起回滚
    assert conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0] == 0


def test_event_type_must_be_dividend(conn, batch):
    split_id = conn.execute("SELECT event_id FROM events.record_event('split','AAPL','2020-08-31','confirmed',"
                            "'fmp','AAPL|2020-08-31|split',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match="不是 dividend"):
        conn.execute("INSERT INTO events.dividends (event_id, ex_date, amount, dividend_type) "
                     "VALUES (%s, '2020-08-31', 1, 'regular')", (split_id,))


def test_ex_date_must_match_event_date(conn, batch):
    event_id = conn.execute("SELECT event_id FROM events.record_event('dividend','AAPL','2026-08-10','confirmed',"
                            "'fmp','k',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match="不一致"):
        conn.execute("INSERT INTO events.dividends (event_id, ex_date, amount, dividend_type) "
                     "VALUES (%s, '2026-08-11', 1, 'regular')", (event_id,))


@pytest.mark.parametrize("sql, message", [
    ("UPDATE events.dividends SET amount = 9", "不允许修改"),
    ("DELETE FROM events.dividends", "不允许删除"),
])
def test_append_only(conn, batch, sql, message):
    record_div(conn, batch)
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        conn.execute(sql)
