import json

import numpy as np
import pandas as pd
import psycopg
import pytest

import monitoring.feed_error_deputy as feed_error_deputy
from clean.base_clean import CleanBasic, Rule
from clean.registry import get_cleaner, load_all
from db.object_store import IntermediateStore
from pipelines.clean_runner import CleanError, run_clean
from pipelines.raw_loader import ingest

"""

C 步骤集成测试：先用假 fetcher 走 ingest（A+B）写入 raw_test，再跑 run_clean
用到 Postgres 测试库 + ClickHouse 测试库 + 临时目录（中间态 parquet、错误报告）

"""

RAW_TABLE = "raw_test.price_volume_daily"
PARAMS = {"from_date": "2022-01-01", "to_date": "2022-01-31"}


def fmp_rows():
    """AAPL 4 天正常 + 1 天价格为 0（danger）+ 1 天振幅 30%（warning）；MSFT 2 天正常"""
    rows = [
        ("AAPL", "2022-01-03", 100.0, 102.0, 99.0, 101.0, 1000),
        ("AAPL", "2022-01-04", 101.0, 103.0, 100.0, 102.0, 1000),
        ("AAPL", "2022-01-05", 102.0, 104.0, 101.0, 103.0, 1000),
        ("AAPL", "2022-01-06", 0.0, 104.0, 0.0, 103.0, 1000),          # PV001 → 隔离
        ("AAPL", "2022-01-07", 103.0, 130.0, 100.0, 104.0, 1000),      # PV003 → warning
        ("MSFT", "2022-01-03", 300.0, 302.0, 299.0, 301.0, 2000),
        ("MSFT", "2022-01-04", 301.0, 303.0, 300.0, 302.0, 2000),
    ]
    df = pd.DataFrame(rows, columns=["symbol", "date", "adjOpen", "adjHigh", "adjLow", "adjClose", "volume"])
    df["endpoint"] = "price_volume"
    return df


def fake_fetcher(df):
    class FakeFetchData:
        def __init__(self, symbols, endpoint_name, extra_params):
            self.symbols = symbols

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def fetch_fmp_batch(self):
            return df, {"success": len(self.symbols), "missing": 0, "failed": 0, "missing_symbols": []}, list(self.symbols), []
    return FakeFetchData


@pytest.fixture
def env(store, ch, tmp_path, monkeypatch):
    monkeypatch.setattr(feed_error_deputy, "ERRORS_REPORT", tmp_path / "reports")
    intermediate = IntermediateStore(base_url=str(tmp_path / "intermediate"))
    batch_id = ingest("price_volume", ["AAPL", "MSFT"], PARAMS, store=store, ch=ch,
                      raw_table=RAW_TABLE, fetcher_cls=fake_fetcher(fmp_rows()))
    return {"store": store, "ch": ch, "intermediate": intermediate, "batch_id": batch_id, "tmp": tmp_path}


def clean(env):
    return run_clean(env["batch_id"], store=env["store"], ch=env["ch"], intermediate=env["intermediate"])


# =========================
# ✅ 正常流程
# =========================
def test_run_clean_end_to_end(env):
    store, batch_id = env["store"], env["batch_id"]
    r = clean(env)

    # 对账：7 = 5 health + 1 warning + 1 danger
    assert r.raw_rows == 7
    assert (r.stats["health"], r.stats["warning"], r.stats["danger"]) == (5, 1, 1)

    # 规则已同步（PV003 v2、PV005 进了规则表）
    active = store.active_rules("price_volume")
    assert active["PV003"]["rule_version"] == 2 and "PV005" in active

    # 问题记录 + 隔离区
    issues = store.conn.execute(
        "SELECT rule_id, rule_version, symbol, date::text FROM meta.quality_issues ORDER BY rule_id").fetchall()
    assert issues == [("PV001", 1, "AAPL", "2022-01-06"), ("PV003", 2, "AAPL", "2022-01-07")]
    q = store.conn.execute("SELECT symbol, date::text, status, record FROM meta.quarantine_records").fetchall()
    assert [(s, d, st) for s, d, st, _ in q] == [("AAPL", "2022-01-06", "pending")]
    assert q[0][3]["adjOpen"] == 0.0

    # 中间态 parquet：6 行（不含隔离的），带追溯字段
    df = env["intermediate"].read(r.intermediate_location)
    assert len(df) == 6
    assert set(df["quality_status"]) == {"health", "warning"}
    assert (df["batch_id"] == batch_id).all() and (df["source_logic_version"] == 1).all()
    assert not {"return", "prev_return"} & set(df.columns)

    # load_log：raw → quarantine → intermediate
    loads = [(l["layer"], l["rows_written"], l["status"], l["location"]) for l in store.get_loads(batch_id)]
    assert loads == [("raw", 7, "success", None), ("quarantine", 1, "success", None),
                     ("intermediate", 6, "success", r.intermediate_location)]

    # 错误报告：已生成并回填到 ingestion.report
    assert r.report is not None
    assert store.get_ingestion(batch_id)["report"] == r.report
    payload = json.loads(open(r.report, encoding="utf-8").read())
    assert payload["summary报表总览"]["total_error_rows"] == 2


def test_run_clean_rerun_is_idempotent(env):
    first = clean(env)
    second = clean(env)
    assert (second.issues_inserted, second.quarantined_inserted) == (0, 0)     # 不重复
    assert second.intermediate_location == first.intermediate_location      # 覆盖同一个文件
    n = env["store"].conn.execute("SELECT count(*) FROM meta.quarantine_records").fetchone()[0]
    assert n == 1


# =========================
# ✅ 前置检查
# =========================
def test_run_clean_rejects_unknown_batch(env):
    with pytest.raises(CleanError, match="不存在"):
        run_clean("no_such_batch", store=env["store"], ch=env["ch"], intermediate=env["intermediate"])


def test_run_clean_rejects_failed_batch(env):
    env["store"].conn.execute("UPDATE meta.ingestion SET status = 'failed' WHERE batch_id = %s", (env["batch_id"],))
    with pytest.raises(CleanError, match="failed"):
        clean(env)


def test_run_clean_detects_raw_row_mismatch(env):
    env["ch"].command(f"INSERT INTO {RAW_TABLE} (symbol, date, batch_id) VALUES ('X', '2022-01-03', '{env['batch_id']}')")
    with pytest.raises(CleanError, match="对不上"):
        clean(env)


def test_run_clean_stops_on_rule_param_change_without_bump(env, monkeypatch):
    clean(env)                                     # 先同步一次规则
    cls = get_cleaner("price_volume")
    changed = [r if r.rule_id != "PV003" else Rule(**{**r.__dict__, "params": {"max_range": 0.5}}) for r in cls.rules]
    monkeypatch.setattr(cls, "rules", changed)
    with pytest.raises(ValueError, match="请把版本号升到"):
        clean(env)


# =========================
# ✅ 单事务：问题记录和隔离数据要么都写，要么都不写
# =========================
def test_record_validation_is_atomic(env):
    store, batch_id = env["store"], env["batch_id"]
    issues = [{"rule_id": "PV001", "rule_version": 1, "severity": "danger", "symbol": "AAPL", "date": "2022-01-06"}]
    bad_records = [{"symbol": None, "date": "2022-01-06", "record": {}}]       # symbol 为空 → 隔离写入失败
    with pytest.raises(psycopg.errors.NotNullViolation):
        store.record_validation(batch_id, "price_volume", issues, bad_records)
    assert store.conn.execute("SELECT count(*) FROM meta.quality_issues").fetchone()[0] == 0   # 问题记录也回滚了


# =========================
# ✅ 自动注册
# =========================
def test_registry():
    registry = load_all()
    assert registry["price_volume"].__name__ == "PriceVolume"
    assert {"market_cap", "income_statement", "balance_sheet", "cash_flow"} <= set(registry)
    with pytest.raises(KeyError, match="没有 endpoint=nope"):
        get_cleaner("nope")


def test_registry_rejects_duplicate_endpoint():
    with pytest.raises(TypeError, match="已经有清洗类"):
        class AnotherPriceVolume(CleanBasic):
            endpoint_name = "price_volume"


# =========================
# ✅ 中间态存储位置
# =========================
def test_intermediate_default_uses_endpoint_dirs(monkeypatch):
    from config.paths import VOLUME_DATA_PATH, DATA_DIR
    monkeypatch.delenv("INTERMEDIATE_URL", raising=False)
    s = IntermediateStore()
    assert s.location("price_volume", "b1") == str(VOLUME_DATA_PATH / "b1.parquet")
    assert s.location("not_registered", "b1") == str(DATA_DIR / "not_registered" / "b1.parquet")


def test_intermediate_custom_base_url():
    assert IntermediateStore(base_url="s3://bucket/inter").location("price_volume", "b1") == "s3://bucket/inter/price_volume/b1.parquet"
    assert IntermediateStore(base_url="s3://bucket/inter", storage_options={}).is_remote
