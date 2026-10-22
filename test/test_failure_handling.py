"""2026-10-17 断网暴露的问题：空结果崩溃、暂时性失败不报错、汇总跨流程、事件失败挡住日线"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from pipelines import backfill, daily, event_loader, periodic

D = date.fromisoformat


def batch(store, endpoint, failed=(), symbols=("AAPL", "MSFT")):
    with store.ingestion_run(endpoint, list(symbols)) as run:
        detail = [{"symbol": s, "error_type": t} for s, t in failed]
        run.finish({"success": len(symbols) - len(detail), "failed": len(detail)}, detail)
    return run.batch_id


# =========================
# ✅ 批次一行都没写进 raw 时，ClickHouse 返回不带列的空表
# =========================
def test_done_symbols_empty_raw_result():
    conn = SimpleNamespace(execute=lambda *a: SimpleNamespace(fetchall=lambda: [("b1", ["XYZ"])]))
    ch = SimpleNamespace(query_df=lambda *a: pd.DataFrame())
    assert backfill.done_symbols(SimpleNamespace(conn=conn), ch, "price_volume", {}) == {"XYZ"}


# =========================
# ✅ 暂时性失败（网络 / 限流）要能查出来，并让任务失败
# =========================
def test_transient_failures_only_network_errors(store):
    b = batch(store, "price_volume", [("AAPL", "timeout"), ("MSFT", "schema_error")])
    assert backfill.transient_failures(store, b) == ["AAPL"]


def test_ingest_prices_raises_on_transient(monkeypatch):
    monkeypatch.setattr(daily, "daily_symbols", lambda conn, d: ["AAPL", "MSFT"])
    monkeypatch.setattr(daily, "run_job", lambda name, *a, **k: backfill.JobResult(
        name, 2, batches=["b1"], transient_failures=["AAPL"]))
    with pytest.raises(RuntimeError, match="暂时性拉取失败"):
        daily.ingest_prices(D("2026-10-16"), store=SimpleNamespace(conn=None), ch=object())


def test_periodic_ingest_raises_on_transient(monkeypatch):
    monkeypatch.setattr(periodic, "daily_symbols", lambda conn, d: ["AAPL"])
    monkeypatch.setattr(periodic, "run_job", lambda name, *a, **k: backfill.JobResult(
        name, 1, batches=["b1"], transient_failures=["AAPL"]))
    with pytest.raises(RuntimeError, match="暂时性拉取失败"):
        periodic._ingest("income_annual", "income_statement", {}, D("2026-10-17"),
                         SimpleNamespace(conn=None), object())


# =========================
# ✅ 事件来源逐个试：一个失败不影响其他，最后统一报错
# =========================
def test_load_events_tries_every_source(monkeypatch, store):
    ok = batch(store, "symbol_change")
    bad = batch(store, "mergers_acquisitions", [("AAPL", "timeout"), ("MSFT", "timeout")])
    called = []

    def result(name, batch_id):
        def fn(*a, **k):
            called.append(name)
            return SimpleNamespace(batch_id=batch_id, rows_fetched=1, write={})
        return fn

    def boom(*a, **k):
        called.append("sp500")
        raise ConnectionError("github down")

    monkeypatch.setattr(event_loader, "load_symbol_changes", result("symbol_change", ok))
    monkeypatch.setattr(event_loader, "load_sp500_index_changes", boom)
    monkeypatch.setattr(event_loader, "load_acquisitions", result("acquisition", bad))
    monkeypatch.setattr(event_loader, "load_dividends_calendar", result("dividends_calendar", ok))
    monkeypatch.setattr(event_loader, "load_splits_calendar", result("splits_calendar", ok))
    with pytest.raises(RuntimeError) as e:
        daily.load_events(D("2026-10-16"), store=store)
    assert called == ["symbol_change", "sp500", "acquisition", "dividends_calendar", "splits_calendar"]
    assert "sp500" in str(e.value) and "acquisition" in str(e.value) and "symbol_change（" not in str(e.value)


# =========================
# ✅ 汇总只统计本流程的接口（之前月度流程被每周的报表失败拖成失败）
# =========================
def test_summarize_scoped_by_endpoint(store):
    since = datetime.now(timezone.utc) - timedelta(minutes=1)
    batch(store, "balance_sheet", [("AAPL", "timeout"), ("MSFT", "timeout")])
    batch(store, "profile")
    monthly = daily.summarize(D("2026-10-17"), since, store=store, endpoints=periodic.MONTHLY_ENDPOINTS)
    weekly = daily.summarize(D("2026-10-17"), since, store=store, endpoints=periodic.WEEKLY_ENDPOINTS)
    assert monthly["batches"] == {"success": 1}
    assert weekly["batches"] == {"failed": 1}
    assert daily.summarize(D("2026-10-17"), since, store=store)["batches"] == {"success": 1}   # 默认按每日接口（含 profile）


def test_endpoint_lists_are_known():
    from config.endpoints import FMP_ENDPOINTS
    for ep in daily.DAILY_ENDPOINTS + periodic.WEEKLY_ENDPOINTS + periodic.MONTHLY_ENDPOINTS:
        assert ep in FMP_ENDPOINTS or ep in {"sp500_github"}, ep


# =========================
# ✅ 汇总只把"最后仍没补上"的暂时性失败标红（2026-10-21：重试补齐了还被算成失败）
# =========================
def test_unresolved_failures_ignore_recovered(store, monkeypatch):
    since = datetime.now(timezone.utc) - timedelta(minutes=1)
    batch(store, "symbol_change", [("ALL", "request_error")], symbols=("ALL",))      # 事件类：先失败……
    batch(store, "symbol_change", symbols=("ALL",))                                # ……重试成功 → 已补上
    batch(store, "mergers_acquisitions", [("0", "timeout")], symbols=("0",))       # 没再成功 → 没补上
    batch(store, "price_volume", [("AAPL", "timeout"), ("MSFT", "timeout"), ("XYZ", "not_found")], symbols=("AAPL", "MSFT", "XYZ", "IBM"))
    monkeypatch.setattr(daily, "done_symbols", lambda store, ch, ep, params: {"AAPL"})   # 重试时 AAPL 写进了 raw
    out = daily.unresolved_failures(store, since, daily.DAILY_ENDPOINTS, ch=object())
    assert out == {"mergers_acquisitions": ["0"], "price_volume": ["MSFT"]}          # XYZ 是永久性失败，不算


def test_alerts_recovered_is_warning_unresolved_is_error():
    from pipelines import alerts
    base = {"batches": {"success": 10, "failed": 3}, "issues": {}}
    assert [(a["level"], a["code"]) for a in alerts.evaluate("daily_eod", {**base, "unresolved": {}})] == \
        [("warning", "batch_recovered")]
    got = alerts.evaluate("daily_eod", {**base, "unresolved": {"price_volume": ["MSFT"]}})
    assert got[0]["level"] == "error" and "MSFT" in got[0]["message"]


# =========================
# ✅ 事件修订分三类：只有"股票池里、内容变了"的才提醒（2026-10-21：一天两百多条几乎全是状态变化和池外股票）
# =========================
def test_event_revision_counts(store):
    import json as _json
    b = batch(store, "dividends_calendar", symbols=("2026-10-16",))
    sid = store.conn.execute("INSERT INTO ref.securities (name, security_type) VALUES ('X', 'common_stock') "
                             "RETURNING security_id").fetchone()[0]

    def rec(key, symbol, status, payload):
        return store.conn.execute(
            "SELECT event_id FROM events.record_event('dividend', %s, '2026-10-16', %s, 'fmp', %s, %s, %s, NULL, now())",
            (symbol, status, key, b, _json.dumps(payload))).fetchone()[0]

    since = datetime.now(timezone.utc) - timedelta(minutes=1)
    e = rec("IN|1", "IN", "announced", {"dividend": 1.0})
    store.conn.execute("UPDATE events.corporate_events SET security_id = %s WHERE event_id = %s", (sid, e))
    rec("IN|1", "IN", "confirmed", {"dividend": 1.0})                  # 只是状态变了
    rec("IN|1", "IN", "confirmed", {"dividend": 1.1})                  # 内容变了（新修订没有 security_id，回填前）
    store.conn.execute("UPDATE events.corporate_events SET security_id = %s WHERE source_event_key = 'IN|1'", (sid,))
    rec("OUT|1", "OUTF", "announced", {"dividend": 0.0370})
    rec("OUT|1", "OUTF", "announced", {"dividend": 0.0371})            # 池外、汇率微调
    assert daily.event_revision_counts(store, since) == \
        {"event_revisions": 1, "event_status_changes": 1, "event_revisions_other": 1}
