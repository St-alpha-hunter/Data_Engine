import json
from datetime import date

import pytest

from pipelines import alerts, periodic

"""

提醒规则、每周拉取参数、月度 SEC 核对

"""

D = date.fromisoformat


# =========================
# ✅ 提醒
# =========================
def test_evaluate_rules():
    ok = {"batches": {"success": 30}, "issues": {"warning": 5}, "event_revisions": 3, "gd001_total": 87}
    assert alerts.evaluate("daily_eod", ok, previous={"gd001_total": 87}) == []

    bad = {"batches": {"success": 28, "failed": 1, "partial": 1}, "issues": {"danger": 4},
           "event_revisions": 81, "gd001_total": 90}
    got = {(a["level"], a["code"]) for a in alerts.evaluate("daily_eod", bad, previous={"gd001_total": 87})}
    assert got == {("error", "batch_failed"), ("warning", "batch_partial"), ("warning", "new_danger"),
                   ("warning", "event_revisions"), ("warning", "gd001_increase")}


def test_check_records_alerts_and_compares_with_previous(tmp_path, monkeypatch):
    monkeypatch.setattr(alerts, "ALERT_LOG", tmp_path / "alerts.jsonl")
    monkeypatch.setattr(alerts, "SUMMARY_LOG", tmp_path / "summaries.jsonl")
    s = {"batches": {"success": 1}, "issues": {}, "event_revisions": 0, "gd001_total": 87}
    assert alerts.check("daily_eod", "run1", s) == []                       # 第一次：没有上一次可比
    got = alerts.check("daily_eod", "run2", {**s, "gd001_total": 95})
    assert [a["code"] for a in got] == ["gd001_increase"]
    records = [json.loads(l) for l in (tmp_path / "alerts.jsonl").read_text(encoding="utf-8").splitlines()]
    assert records[0]["run_id"] == "run2" and records[0]["flow"] == "daily_eod"
    assert alerts.check("weekly_fundamentals", "w1", {**s, "gd001_total": 200}) == []   # 不同流程分开比较


# =========================
# ✅ 每周参数：带 as_of，不然第二周会被当成"已经拉过"
# =========================
def test_weekly_params(monkeypatch):
    seen = {}
    monkeypatch.setattr(periodic, "_ingest", lambda name, ep, params, d, store, ch: seen.setdefault(name, (ep, params)))
    periodic.weekly_job("income_quarter", D("2026-10-17"), store=object(), ch=object())
    periodic.weekly_job("market_cap", D("2026-10-17"), store=object(), ch=object())
    assert seen["income_quarter"] == ("income_statement", {"period": "quarter", "limit": 8, "as_of": "2026-10-17"})
    assert seen["market_cap"] == ("market_cap", {"from_date": "2026-10-03", "to_date": "2026-10-17"})


# =========================
# ✅ 月度：和 SEC 代码表核对
# =========================
def security(conn, symbol, cik, status="active"):
    sid = conn.execute("INSERT INTO ref.securities (name, security_type, first_trade_date, status, review_status) "
                       "VALUES (%s, 'common_stock', '2000-01-03', %s, 'auto') RETURNING security_id",
                       (symbol, status)).fetchone()[0]
    conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source) "
                 "VALUES (%s, 'symbol', %s, '2000-01-03', 'manual')", (sid, symbol))
    if cik:
        conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source) "
                     "VALUES (%s, 'cik', %s, '2000-01-03', 'manual')", (sid, cik))


def test_check_against_sec(store, tmp_path, monkeypatch):
    monkeypatch.setattr(periodic, "REPORT_DIR", tmp_path)
    security(store.conn, "ZQOK", "0000000001")
    security(store.conn, "ZQGONE", "0000000002")            # SEC 里没有：可能已退市或改名
    security(store.conn, "ZQCIK", "0000000003")             # CIK 不一致
    sec = {"ZQOK": "0000000001", "ZQCIK": "0000000099"}
    out = periodic.check_against_sec(D("2026-11-01"), store=store, sec_ciks=sec)
    assert (out["active"], out["not_in_sec"], out["cik_mismatch"]) == (3, 1, 1)
    report = (tmp_path / "sec_check_2026-11-01.csv").read_text(encoding="utf-8-sig")
    assert "ZQGONE" in report and "ZQCIK" in report and "ZQOK" not in report
    assert [c["code"] for c in periodic.monthly_checks(out, {"fmp_issues": 0})] == ["sec_mismatch"]
