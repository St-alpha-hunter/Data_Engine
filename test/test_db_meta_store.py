import pytest
import pandas as pd
import numpy as np
import psycopg

from config.paths import BASE_DIR
from db.meta_store import MetaStore, track_ingestion, new_batch_id

"""

MetaStore 集成测试：连接 Docker 里的 PostgreSQL，在独立的测试库 data_engine_test 上运行，
不会影响正式库 data_engine。夹具见 conftest.py。

"""

# 数据库夹具（test_db_config / conn / store）在 conftest.py


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
    assert tables == {"ingestion", "quality_rules", "quality_issues", "quarantine_records",
                      "data_corrections", "load_log"}


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
    assert row["n_symbols"] == 3
    assert row["rows_fetched"] == 40
    assert row["failed_symbols"] == [f["symbol"] for f in failed_list]
    assert row["missing_symbols"] == summary.get("missing_symbols", [])
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
# ✅ load_log
# =========================
def test_log_load_and_reconcile(store):
    batch_id = make_batch(store)
    store.log_load(batch_id, "raw", "raw.price_volume_daily", 40)
    store.log_load(batch_id, "golden", "golden.price_volume_daily", 38)
    store.log_load(batch_id, "quarantine", "meta.quarantine_records", 2)

    loads = store.get_loads(batch_id)
    assert [(l["layer"], l["rows_written"], l["status"]) for l in loads] == [
        ("raw", 40, "success"), ("golden", 38, "success"), ("quarantine", 2, "success")]


def test_load_step_success(store):
    batch_id = make_batch(store)
    with store.load_step(batch_id, "raw", "raw.price_volume_daily") as step:
        step.rows = 40
    [load] = store.get_loads(batch_id)
    assert (load["status"], load["rows_written"], load["error"]) == ("success", 40, None)


def test_load_step_failure_logged_and_reraised(store):
    batch_id = make_batch(store)
    with pytest.raises(ConnectionError, match="ClickHouse 挂了"):
        with store.load_step(batch_id, "raw", "raw.price_volume_daily") as step:
            step.rows = 10          # 写了一部分后失败
            raise ConnectionError("ClickHouse 挂了")
    [load] = store.get_loads(batch_id)
    assert load["status"] == "failed"
    assert load["rows_written"] == 10
    assert "ClickHouse 挂了" in load["error"]


def test_load_step_retry_keeps_history(store):
    """失败后重试：failed 和 success 两条都保留"""
    batch_id = make_batch(store)
    with pytest.raises(RuntimeError):
        with store.load_step(batch_id, "raw", "raw.price_volume_daily"):
            raise RuntimeError("第一次失败")
    with store.load_step(batch_id, "raw", "raw.price_volume_daily") as step:
        step.rows = 40
    assert [l["status"] for l in store.get_loads(batch_id)] == ["failed", "success"]


@pytest.mark.parametrize("kwargs, constraint", [
    ({"layer": "bronze"}, "chk_load_log_layer"),
    ({"target_table": "price_volume_daily"}, "chk_load_log_table_qualified"),
    ({"status": "failed"}, "chk_load_log_error_iff_failed"),
    ({"error": "成功却带错误信息"}, "chk_load_log_error_iff_failed"),
])
def test_log_load_constraints(store, kwargs, constraint):
    batch_id = make_batch(store)
    args = {"layer": "raw", "target_table": "raw.price_volume_daily", "rows_written": 1, **kwargs}
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        store.log_load(batch_id, **args)


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
        {"rule_id": "PV001", "rule_version": 1, "severity": "danger", "symbol": "AAPL",
         "date": pd.Timestamp("2022-01-03"), "detail": {"adjLow": 0.0, "volume": np.int64(100)}},
        {"rule_id": "PV003", "rule_version": 1, "severity": "warning", "symbol": "MSFT",
         "date": "2022-01-04", "detail": {"range": float("nan")}},
        {"rule_id": "PV004", "rule_version": 1, "severity": "warning", "symbol": "MSFT", "date": None},
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
                                        "symbol": "AAPL", "date": "2022-01-03"}])


# =========================
# ✅ quarantine / release / discard / outbox
# =========================
def quarantine_two(store):
    batch_id = make_batch(store)
    n = store.quarantine(batch_id, "price_volume", [
        {"symbol": "AAPL", "date": "2022-01-03", "record": {"adjLow": 0.0, "date": pd.Timestamp("2022-01-03")}},
        {"symbol": "MSFT", "date": "2022-01-03", "record": {"adjHigh": -1.0}},
    ])
    ids = [r[0] for r in store.conn.execute(
        "SELECT quarantine_id FROM meta.quarantine_records ORDER BY symbol")]
    return batch_id, n, ids


def test_quarantine_dedup(store):
    batch_id, n, ids = quarantine_two(store)
    assert n == 2
    assert store.quarantine(batch_id, "price_volume", [
        {"symbol": "AAPL", "date": "2022-01-03", "record": {}}]) == 0


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


# =========================
# ✅ sync_rules：规则以代码为准
# =========================
from clean.clean_price_volume_fmp_dev import PriceVolume


def test_sync_rules_from_code(store):
    result = store.sync_rules(PriceVolume.rule_definitions())
    assert result["unchanged"] == ["PV001", "PV002", "PV004"]   # 002 里已经有 v1
    assert result["upgraded"] == ["PV003"]                       # 代码里是 v2（跳过价格 <= 0 的行）
    assert result["inserted"] == ["PV005"]                       # 代码里新加的
    active = store.active_rules("price_volume")
    assert set(active) == {"PV001", "PV002", "PV003", "PV004", "PV005"}
    assert active["PV003"]["rule_version"] == 2

    # 再同步一次：什么都不变
    again = store.sync_rules(PriceVolume.rule_definitions())
    assert again["inserted"] == [] and again["upgraded"] == []


def test_sync_rules_changed_params_without_bump_rejected(store):
    store.sync_rules(PriceVolume.rule_definitions())
    defs = PriceVolume.rule_definitions()
    defs[2]["params"] = {"max_range": 0.20}           # 改了 PV003 阈值，没升版本
    defs.append({**defs[0], "rule_id": "PV999"})      # 同一次同步里还有一条新规则
    with pytest.raises(ValueError, match="PV003 v2 的 severity/params 与数据库登记的不一致"):
        store.sync_rules(defs)
    # 整体回滚：PV999 也没有插进去
    assert "PV999" not in store.active_rules("price_volume")


def test_sync_rules_version_bump(store):
    store.sync_rules(PriceVolume.rule_definitions())
    defs = PriceVolume.rule_definitions()
    defs[2].update(rule_version=3, params={"max_range": 0.20})
    result = store.sync_rules(defs)
    assert result["upgraded"] == ["PV003"]

    active = store.active_rules("price_volume")["PV003"]
    assert (active["rule_version"], active["params"]) == (3, {"max_range": 0.20})
    versions = store.conn.execute(
        "SELECT rule_version, is_active FROM meta.quality_rules WHERE rule_id = 'PV003' ORDER BY 1").fetchall()
    assert versions == [(1, False), (2, False), (3, True)]     # 旧版本保留，只是不生效

    # 旧代码（v2）再来同步：拒绝
    with pytest.raises(ValueError, match="低于数据库最高版本"):
        store.sync_rules(PriceVolume.rule_definitions())
