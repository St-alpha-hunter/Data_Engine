import json
import threading
from datetime import date, datetime, timezone

import psycopg
import pytest

from db.connections import connect_pg

"""

事件总表 events.corporate_events 测试：record_event 的去重 / 版本、按时点查询、只追加保护、并发写入
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""

KEY = "FB|META|2022-06-09"


@pytest.fixture
def batch(store):
    with store.ingestion_run("symbol_change", ["FB"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


def record(conn, batch_id, status="announced", payload=None, known_at=None, key=KEY, symbol="FB",
           event_type="symbol_change", event_date=date(2022, 6, 9)):
    payload = payload if payload is not None else {"oldSymbol": "FB", "newSymbol": "META"}
    return conn.execute(
        "SELECT * FROM events.record_event(%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)",
        (event_type, symbol, event_date, status, "fmp", key, batch_id, json.dumps(payload),
         known_at or datetime.now(timezone.utc)),
    ).fetchone()


# =========================
# ✅ 去重与版本
# =========================
def test_insert_duplicate_revise(conn, batch):
    assert record(conn, batch)[1:] == (1, "inserted")
    assert record(conn, batch)[1:] == (1, "duplicate")                       # Kafka 重复投递
    assert record(conn, batch, status="confirmed")[1:] == (2, "revised")      # 状态变了 → 新版本
    assert record(conn, batch)[1:] == (1, "duplicate")                       # 旧消息晚到：仍是重复
    n = conn.execute("SELECT count(*) FROM events.corporate_events").fetchone()[0]
    assert n == 2


def test_payload_change_is_revision(conn, batch):
    record(conn, batch, payload={"amount": 0.24})
    assert record(conn, batch, payload={"amount": 0.25})[1:] == (2, "revised")


def test_different_key_is_new_event(conn, batch):
    record(conn, batch)
    assert record(conn, batch, key="AAPL|2020-08-31|split", symbol="AAPL",
                  event_type="split", event_date=date(2020, 8, 31))[1:] == (1, "inserted")


def test_hash_ignores_json_key_order(conn, batch):
    """jsonb 会规范化键的顺序：键顺序不同但内容相同，算重复"""
    record(conn, batch, payload={"a": 1, "b": 2})
    assert record(conn, batch, payload={"b": 2, "a": 1})[2] == "duplicate"


# =========================
# ✅ 当前版本 / 按时点
# =========================
def test_current_and_asof(conn, batch):
    record(conn, batch, status="announced", known_at=datetime(2022, 5, 1, tzinfo=timezone.utc))
    record(conn, batch, status="confirmed", known_at=datetime(2022, 6, 9, tzinfo=timezone.utc))

    cur = conn.execute("SELECT revision, status FROM events.v_corporate_events_current").fetchall()
    assert cur == [(2, "confirmed")]
    asof = conn.execute("SELECT revision, status FROM events.corporate_events_asof('2022-05-15')").fetchall()
    assert asof == [(1, "announced")]
    before = conn.execute("SELECT count(*) FROM events.corporate_events_asof('2022-04-01')").fetchone()[0]
    assert before == 0


# =========================
# ✅ 只追加保护
# =========================
@pytest.fixture
def event_and_security(conn, batch):
    event_id = record(conn, batch)[0]
    sid = conn.execute("INSERT INTO ref.securities (name, security_type) VALUES ('Meta', 'common_stock') "
                       "RETURNING security_id").fetchone()[0]
    return event_id, sid


def test_backfill_security_id_once(conn, event_and_security):
    event_id, sid = event_and_security
    conn.execute("UPDATE events.corporate_events SET security_id = %s WHERE event_id = %s", (sid, event_id))
    with pytest.raises(psycopg.errors.RaiseException, match="只能从空回填"):
        conn.execute("UPDATE events.corporate_events SET security_id = NULL WHERE event_id = %s", (event_id,))


def test_other_fields_immutable(conn, event_and_security):
    event_id, _ = event_and_security
    with pytest.raises(psycopg.errors.RaiseException, match="只允许回填 security_id"):
        conn.execute("UPDATE events.corporate_events SET status = 'cancelled' WHERE event_id = %s", (event_id,))


def test_delete_forbidden(conn, event_and_security):
    event_id, _ = event_and_security
    with pytest.raises(psycopg.errors.RaiseException, match="不允许删除"):
        conn.execute("DELETE FROM events.corporate_events WHERE event_id = %s", (event_id,))


@pytest.mark.parametrize("kw, error", [
    ({"symbol": "fb"}, psycopg.errors.CheckViolation),                  # 小写
    ({"event_type": "merger"}, psycopg.errors.CheckViolation),          # 不在 5 类里
    ({"status": "revised"}, psycopg.errors.CheckViolation),             # 版本变化用 revision 表示
])
def test_invalid_inputs(conn, batch, kw, error):
    with pytest.raises(error):
        record(conn, batch, **kw)


def test_unknown_batch_rejected(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        record(conn, "no_such_batch")


def test_identifier_event_fk(conn, event_and_security):
    event_id, sid = event_and_security
    conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source, event_id) "
                 "VALUES (%s, 'symbol', 'META', '2022-06-09', 'fmp_symbol_change', %s)", (sid, event_id))
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, valid_to, source, event_id) "
                     "VALUES (%s, 'symbol', 'FB', '2012-05-18', '2022-06-09', 'x', 999999)", (sid,))


# =========================
# ✅ 并发：多个消费者同时写同一事件
# =========================
def test_concurrent_revisions(test_db_config, conn, batch):
    errors, results = [], []

    def worker(i):
        try:
            with connect_pg(test_db_config) as c:
                results.append(record(c, batch, payload={"n": i}))
        except Exception as e:                      # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert errors == []
    assert sorted(r[1] for r in results) == list(range(1, 11))          # 版本号 1..10，不冲突不跳号
