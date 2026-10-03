import pytest
import pandas as pd
import numpy as np
import psycopg

from config.paths import BASE_DIR
from db.config import PGConfig
from db.connections import connect_pg
from db.meta_store import MetaStore, track_ingestion, new_batch_id

"""

MetaStore 集成测试：连接 Docker 里的 PostgreSQL，在独立的测试库 data_engine_test 上运行，
不会影响正式库 data_engine。测试库每次运行都按 sql/postgres/ 下的迁移文件从零重建。
PostgreSQL 没启动时自动跳过。

"""

TEST_DB = "data_engine_test"
MIGRATIONS = sorted((BASE_DIR / "sql" / "postgres").glob("*.sql"))
META_TABLES = "meta.data_corrections, meta.quarantine_records, meta.quality_issues, meta.ingestion"


# =========================
# fixtures
# =========================
@pytest.fixture(scope="session")
def test_db_config():
    try:
        admin = connect_pg()
    except (psycopg.OperationalError, KeyError) as e:
        pytest.skip(f"PostgreSQL 不可用：{e}")

    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {TEST_DB}")

    cfg = PGConfig.from_env(dbname=TEST_DB)
    with connect_pg(cfg) as conn:
        for path in MIGRATIONS:
            conn.execute(path.read_text(encoding="utf-8"))
    return cfg


@pytest.fixture
def conn(test_db_config):
    """每个测试一个干净的库：清空业务数据，保留规则表的初始数据"""
    with connect_pg(test_db_config) as c:
        # TRUNCATE 不触发行级触发器，所以能清空只追加的 data_corrections
        c.execute(f"TRUNCATE {META_TABLES} RESTART IDENTITY CASCADE")
        yield c


@pytest.fixture
def store(conn):
    return MetaStore(conn)


def make_batch(store, symbols=("AAPL", "MSFT")):
    """插入一个已完成的批次，返回 batch_id"""
    with store.ingestion_run("price_volume", list(symbols)) as run:
        run.finish({"success": len(symbols), "missing": 0, "failed": 0}, [])
    return run.batch_id


# =========================
# ✅ 迁移文件
# =========================
def test_migrations_build_all_tables(conn):
    tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'meta'")}
    assert tables == {"ingestion", "quality_rules", "quality_issues", "quarantine_records", "data_corrections"}


def test_new_batch_id_format():
    batch_id = new_batch_id("price_volume")
    prefix, ymd, hms, suffix = batch_id.rsplit("_", 3)
    assert prefix == "price_volume"
    assert len(ymd) == 8 and len(hms) == 6 and len(suffix) == 4


# =========================
# ✅ ingestion_run
# =========================
@pytest.mark.parametrize("summary, failed_list, expected", [
    ({"success": 3, "missing": 0, "failed": 0}, [], "success"),
    ({"success": 1, "missing": 1, "failed": 1}, [{"symbol": "NVDA"}], "partial"),
    ({"success": 0, "missing": 0, "failed": 3}, [{"symbol": s} for s in ("AAPL", "MSFT", "NVDA")], "failed"),
    ({"success": 2, "missing": 1, "failed": 0, "missing_symbols": ["NVDA"]}, [], "success"),  # 无数据不算失败
])
def test_ingestion_status(store, summary, failed_list, expected):
    with store.ingestion_run("price_volume", ["AAPL", "MSFT", "NVDA"], {"from_date": "2022-01-01"}) as run:
        assert store.get_ingestion(run.batch_id)["status"] == "running"
        run.finish(summary, failed_list, rows_fetched=40)

    row = store.get_ingestion(run.batch_id)
    assert row["status"] == expected
    assert row["n_ticker"] == 3
    assert row["rows_fetched"] == 40
    assert row["failed_tickers"] == [f["symbol"] for f in failed_list]
    assert row["missing_tickers"] == summary.get("missing_symbols", [])
    assert row["params"] == {"from_date": "2022-01-01"}
    assert row["finished_at"] is not None


def test_ingestion_exception_marks_failed_and_reraises(store):
    with pytest.raises(RuntimeError, match="网络炸了"):
        with store.ingestion_run("price_volume", ["AAPL"]) as run:
            raise RuntimeError("网络炸了")

    row = store.get_ingestion(run.batch_id)
    assert row["status"] == "failed"
    assert row["failure_detail"][-1]["error_type"] == "exception"
    assert "网络炸了" in row["failure_detail"][-1]["error"]


def test_ingestion_without_finish_marks_failed(store):
    with store.ingestion_run("price_volume", ["AAPL"]) as run:
        pass
    row = store.get_ingestion(run.batch_id)
    assert row["status"] == "failed"
    assert row["failure_detail"][0]["error_type"] == "not_finished"


def test_ingestion_report_and_json_types(store):
    """params 里有 datetime / numpy 类型也能存；report 路径能回填"""
    params = {"from_date": pd.Timestamp("2022-01-01"), "limit": np.int64(10)}
    with store.ingestion_run("price_volume", ["AAPL"], params) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
        run.set_report(BASE_DIR / "error_report" / "x.json")

    row = store.get_ingestion(run.batch_id)
    assert row["params"] == {"from_date": "2022-01-01T00:00:00", "limit": 10}
    assert row["report"].endswith("x.json")


# =========================
# ✅ 装饰器
# =========================
def test_track_ingestion_decorator(store):
    seen = {}

    @track_ingestion("price_volume", store=store)
    def fetch(symbols, from_date, to_date="2022-01-31", run=None):
        seen["batch_id"] = run.batch_id
        df = pd.DataFrame({"symbol": symbols, "adjClose": [1.0] * len(symbols)})
        return df, {"success": 1, "missing": 0, "failed": 1}, ["AAPL"], [{"symbol": "MSFT"}]

    df, summary, succeed, failed = fetch(["AAPL", "MSFT"], "2022-01-01")

    row = store.get_ingestion(seen["batch_id"])
    assert row["status"] == "partial"
    assert row["rows_fetched"] == 2
    assert row["params"] == {"from_date": "2022-01-01", "to_date": "2022-01-31"}
    assert succeed == ["AAPL"]


def test_track_ingestion_requires_symbols():
    with pytest.raises(TypeError, match="symbols"):
        @track_ingestion("price_volume")
        def fetch(tickers):
            pass


# =========================
# ✅ 规则
# =========================
def test_active_rules(store):
    rules = store.active_rules("price_volume")
    assert set(rules) == {"PV001", "PV002", "PV003", "PV004"}
    assert rules["PV003"]["params"] == {"max_range": 0.15}
    assert rules["PV001"]["severity"] == "danger"


# =========================
# ✅ quality_issues
# =========================
def test_record_issues_dedup(store):
    batch_id = make_batch(store)
    issues = [
        {"rule_id": "PV001", "rule_version": 1, "severity": "danger", "ticker": "AAPL",
         "date": pd.Timestamp("2022-01-03"), "detail": {"adjLow": 0.0, "volume": np.int64(100)}},
        {"rule_id": "PV003", "rule_version": 1, "severity": "warning", "ticker": "MSFT",
         "date": "2022-01-04", "detail": {"range": float("nan")}},
        {"rule_id": "PV004", "rule_version": 1, "severity": "warning", "ticker": "MSFT", "date": None},
    ]
    assert store.record_issues(batch_id, issues) == 3
    assert store.record_issues(batch_id, issues) == 0   # 重复校验不重复记录

    detail = store.conn.execute(
        "SELECT detail FROM meta.quality_issues WHERE rule_id = 'PV003'").fetchone()[0]
    assert detail == {"range": None}   # NaN 转成 null


def test_record_issues_unknown_rule_rejected(store):
    batch_id = make_batch(store)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        store.record_issues(batch_id, [{"rule_id": "PV999", "rule_version": 1, "severity": "danger",
                                        "ticker": "AAPL", "date": "2022-01-03"}])


# =========================
# ✅ quarantine / release / discard / outbox
# =========================
def quarantine_two(store):
    batch_id = make_batch(store)
    n = store.quarantine(batch_id, "price_volume", [
        {"ticker": "AAPL", "date": "2022-01-03", "record": {"adjLow": 0.0, "date": pd.Timestamp("2022-01-03")}},
        {"ticker": "MSFT", "date": "2022-01-03", "record": {"adjHigh": -1.0}},
    ])
    ids = [r[0] for r in store.conn.execute(
        "SELECT quarantine_id FROM meta.quarantine_records ORDER BY ticker")]
    return batch_id, n, ids


def test_quarantine_dedup(store):
    batch_id, n, ids = quarantine_two(store)
    assert n == 2
    assert store.quarantine(batch_id, "price_volume", [
        {"ticker": "AAPL", "date": "2022-01-03", "record": {}}]) == 0


def test_release_and_outbox(store):
    _, _, (aapl_id, msft_id) = quarantine_two(store)

    correction_id = store.release(aapl_id, "核对后属实", "K.Hawk")
    store.discard(msft_id, "确认错误", "K.Hawk")

    status = dict(store.conn.execute("SELECT quarantine_id, status FROM meta.quarantine_records"))
    assert status == {aapl_id: "released", msft_id: "discarded"}

    action = store.conn.execute(
        "SELECT action FROM meta.data_corrections WHERE correction_id = %s", (correction_id,)).fetchone()[0]
    assert action == "release"

    # outbox 只返回已放行、未写入的；丢弃的不会出现
    outbox = store.pending_outbox("price_volume")
    assert [r["quarantine_id"] for r in outbox] == [aapl_id]
    assert outbox[0]["record"]["adjLow"] == 0.0

    assert store.mark_written([aapl_id, msft_id]) == 1   # 丢弃的那条不会被标记
    assert store.pending_outbox() == []


def test_release_twice_rejected_and_rolled_back(store):
    _, _, (aapl_id, _) = quarantine_two(store)
    store.release(aapl_id, "第一次", "K.Hawk")

    with pytest.raises(ValueError, match="不是 pending"):
        store.release(aapl_id, "第二次", "K.Hawk")
    with pytest.raises(ValueError, match="不是 pending"):
        store.discard(aapl_id, "放行后又想丢弃", "K.Hawk")

    # 失败的操作没有留下处理记录
    n = store.conn.execute("SELECT count(*) FROM meta.data_corrections").fetchone()[0]
    assert n == 1


def test_release_blank_reason_rolls_back_status(store):
    """处理记录写入失败时，隔离状态也不能被改掉（同一个事务）"""
    _, _, (aapl_id, _) = quarantine_two(store)
    with pytest.raises(psycopg.errors.CheckViolation):
        store.release(aapl_id, "   ", "K.Hawk")

    status = store.conn.execute(
        "SELECT status FROM meta.quarantine_records WHERE quarantine_id = %s", (aapl_id,)).fetchone()[0]
    assert status == "pending"
