import json
from datetime import date
from decimal import Decimal

import psycopg
import pytest

"""

拆股明细 events.splits 测试：record_split 的写入 / 类型推导 / 生成列、约束、通用触发器
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


@pytest.fixture
def batch(store):
    with store.ingestion_run("splits", ["AAPL"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record_split(conn, batch_id, symbol, d, num, den, split_type=None, key=None):
    return conn.execute(
        "SELECT * FROM events.record_split(%s, %s, %s, %s, 'fmp', %s, %s, %s, %s)",
        (symbol, d, Decimal(str(num)), Decimal(str(den)), key or f"{symbol}|{d}|split", batch_id,
         json.dumps({"numerator": num, "denominator": den}), split_type),
    ).fetchone()


def split_row(conn, symbol):
    return conn.execute("SELECT numerator, denominator, split_ratio, split_type FROM events.v_splits_current "
                        "WHERE symbol = %s", (symbol,)).fetchone()


@pytest.mark.parametrize("symbol, num, den, given, expected_type, ratio", [
    ("AAPL", 4, 1, None, "forward", Decimal("4")),
    ("RETO", 1, 20, None, "reverse", Decimal("0.05")),                # 不传类型：按比例推导为合股
    ("XYZ", 21, 20, "stock_dividend", "stock_dividend", Decimal("1.05")),
    ("GOOGL", 999, 500, None, "forward", Decimal("1.998")),           # 非整数比例按原值保存
])
def test_record_and_derive(conn, batch, symbol, num, den, given, expected_type, ratio):
    assert record_split(conn, batch, symbol, date(2020, 8, 31), num, den, given)[2] == "inserted"
    n, d, r, t = split_row(conn, symbol)
    assert (n, d, t) == (Decimal(num), Decimal(den), expected_type)
    assert r == ratio


def test_duplicate_and_revision(conn, batch):
    assert record_split(conn, batch, "AAPL", date(2020, 8, 31), 4, 1)[1:] == (1, "inserted")
    assert record_split(conn, batch, "AAPL", date(2020, 8, 31), 4, 1)[1:] == (1, "duplicate")
    assert record_split(conn, batch, "AAPL", date(2020, 8, 31), 5, 1)[1:] == (2, "revised")     # 比例改了
    assert split_row(conn, "AAPL")[0] == Decimal(5)
    assert conn.execute("SELECT count(*) FROM events.splits").fetchone()[0] == 2


@pytest.mark.parametrize("num, den, split_type, constraint", [
    (1, 1, None, "chk_splits_not_one_to_one"),
    (0, 1, None, "chk_splits_positive"),
    (1, 20, "forward", "chk_splits_type_matches_ratio"),
    (4, 1, "reverse", "chk_splits_type_matches_ratio"),
    (4, 1, "merge", "chk_splits_type"),
])
def test_constraints_and_atomicity(conn, batch, num, den, split_type, constraint):
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        record_split(conn, batch, "X", date(2026, 1, 2), num, den, split_type)
    assert conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0] == 0   # 总表一起回滚


def test_generated_ratio_not_writable(conn, batch):
    record_split(conn, batch, "AAPL", date(2020, 8, 31), 4, 1)
    with pytest.raises(psycopg.errors.GeneratedAlways):
        conn.execute("UPDATE events.splits SET split_ratio = 8")


@pytest.mark.parametrize("sql, message", [
    ("UPDATE events.splits SET numerator = 8", "不允许修改"),
    ("DELETE FROM events.splits", "不允许删除"),
])
def test_append_only(conn, batch, sql, message):
    record_split(conn, batch, "AAPL", date(2020, 8, 31), 4, 1)
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        conn.execute(sql)


def test_generic_trigger_type_and_date(conn, batch):
    """通用触发器：拆股明细挂到分红事件上 / 生效日和总表不一致，都被拒绝"""
    div_id = conn.execute("SELECT event_id FROM events.record_event('dividend','AAPL','2020-08-31','confirmed',"
                          "'fmp','k1',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match="不是 split"):
        conn.execute("INSERT INTO events.splits (event_id, effective_date, numerator, denominator, split_type) "
                     "VALUES (%s, '2020-08-31', 4, 1, 'forward')", (div_id,))
    sp_id = conn.execute("SELECT event_id FROM events.record_event('split','AAPL','2020-08-31','confirmed',"
                         "'fmp','k2',%s,'{}')", (batch,)).fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match="effective_date .* 不一致"):
        conn.execute("INSERT INTO events.splits (event_id, effective_date, numerator, denominator, split_type) "
                     "VALUES (%s, '2020-09-01', 4, 1, 'forward')", (sp_id,))
