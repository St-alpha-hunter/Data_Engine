from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from db.event_store import EVENT_RULES, EventStore
from fetch_data.events import normalize_dividends, normalize_splits
from fetch_data.events.common import NormalizeResult
from fetch_data.fetch import FetchData
from pipelines import event_loader

"""

事件写库测试：FetchData 的请求方式、EventStore 写库与质量问题、各导入入口的端到端流程
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


# =========================
# ✅ FetchData 的 iterate
# =========================
def resp(data):
    r = MagicMock(status_code=200)
    r.json.return_value = data
    return r


@patch("fetch_data.fetch.time.sleep", return_value=None)
def test_fetch_iterate_none_keeps_symbol(_):
    f = FetchData(["ALL"], "mergers_acquisitions", None)
    f.session = MagicMock()
    rows = [{"symbol": "HTB", "targetedSymbol": "BRBS"}]
    with patch.dict("config.endpoints.FMP_ENDPOINTS", {"mergers_acquisitions": {
            "url": "https://x?limit=1&apikey={apikey}", "required_params": [], "iterate": "none"}}):
        f.session.get.side_effect = [resp(rows)]
        df, *_ = f.fetch_fmp_batch()
    assert df["symbol"].tolist() == ["HTB"]                      # 没有被覆盖成 "ALL"


@patch("fetch_data.fetch.time.sleep", return_value=None)
def test_fetch_iterate_page_stops_on_empty(_):
    f = FetchData(list(range(10)), "mergers_acquisitions", None)
    f.session = MagicMock()
    f.session.get.side_effect = [resp([{"symbol": "A"}]), resp([{"symbol": "B"}]), resp([])]
    df, summary, succeed, failed = f.fetch_fmp_batch()
    assert f.session.get.call_count == 3                         # 第 2 页是空页，后面不再请求
    assert "page=0" in f.session.get.call_args_list[0][0][0] and "page=2" in f.session.get.call_args_list[2][0][0]
    assert df["symbol"].tolist() == ["A", "B"] and succeed == [0, 1]


# =========================
# ✅ EventStore
# =========================
@pytest.fixture
def batch(store):
    with store.ingestion_run("dividends", ["AAPL"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    return run.batch_id


DIV_ROWS = [
    {"symbol": "AAPL", "date": "2026-08-10", "recordDate": "2026-08-10", "paymentDate": "2026-08-13",
     "declarationDate": "2026-07-30", "adjDividend": 0.27, "dividend": 0.27, "yield": 0.34, "frequency": "Quarterly"},
    {"symbol": "AAPL", "date": "1969-12-31", "dividend": 0.1, "frequency": "Quarterly"},          # 无效日期
    {"symbol": "AAPL", "date": "2026-05-11", "dividend": 0, "frequency": "Quarterly"},            # 金额为 0
]


def test_event_store_writes_and_records_issues(conn, store, batch):
    es = EventStore(conn, store)
    stats = es.load("dividend", normalize_dividends(DIV_ROWS, today=date(2026, 10, 7)), batch)
    assert (stats["inserted"], stats["rejected"], stats["issues_recorded"]) == (1, 2, 2)

    row = conn.execute("SELECT amount, frequency FROM events.v_dividends_current").fetchone()
    assert row == (Decimal("0.27"), "Quarterly")
    issues = conn.execute("SELECT rule_id, symbol, date, detail->>'reason' FROM meta.quality_issues "
                          "ORDER BY detail->>'reason'").fetchall()
    # 金额为 0 的那行日期有效，质量问题带上日期；无效日期的那行日期为空
    assert issues == [("EVD001", "AAPL", date(2026, 5, 11), "invalid_amount"), ("EVD001", "AAPL", None, "invalid_date")]
    assert "EVD001" in store.active_rules("dividends")          # 规则已同步


def test_event_store_rerun_is_duplicate(conn, store, batch):
    es = EventStore(conn, store)
    result = normalize_dividends(DIV_ROWS[:1])
    assert es.load("dividend", result, batch)["inserted"] == 1
    assert es.load("dividend", result, batch)["duplicate"] == 1


def test_event_store_db_rejection_is_isolated(conn, store, batch):
    """单条被数据库拒绝不影响其他事件，并记为 XXX002"""
    good = normalize_splits([{"symbol": "AAPL", "date": "2020-08-31", "numerator": 4, "denominator": 1}]).events[0]
    bad = {**good, "symbol": "BAD", "source_event_key": "BAD|split", "split_type": "stock_dividend",
           "numerator": Decimal(1), "denominator": Decimal(2)}               # 绕过整理函数：股票股利却减少股数
    result = NormalizeResult("split", events=[bad, good])
    stats = EventStore(conn, store).load("split", result, batch)
    assert (stats["inserted"], stats["db_rejected"]) == (1, 1)
    rule_id, detail = conn.execute("SELECT rule_id, detail FROM meta.quality_issues").fetchone()
    assert rule_id == "EVS002" and "chk_splits_type_matches_ratio" in detail["error"]


def test_all_event_rules_valid(store):
    for rules in EVENT_RULES.values():
        store.sync_rules(rules)                                   # 规则定义本身能通过数据库约束


# =========================
# ✅ 导入入口（假数据）
# =========================
def fake_fetcher(rows_by_unit):
    class Fake:
        def __init__(self, units, endpoint, params):
            self.units = units
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def fetch_fmp_batch(self):
            rows = [r for u in self.units for r in rows_by_unit.get(u, [])]
            ok = [u for u in self.units if rows_by_unit.get(u)]
            return (pd.DataFrame(rows), {"success": len(ok), "missing": len(self.units) - len(ok), "failed": 0,
                                         "missing_symbols": []}, ok, [])
    return Fake


def test_load_sp500_index_changes(conn, store):
    csv = "ticker,start_date,end_date\nFB,2013-12-23,2022-06-09\nMETA,2022-06-09,\nBAD,2020-01-02,2019-01-02\n"
    r = event_loader.load_sp500_index_changes(meta=store, csv_text=csv)
    assert r.rows_fetched == 3
    assert (r.write["inserted"], r.write["rejected"]) == (3, 1)
    assert conn.execute("SELECT count(*) FROM events.v_sp500_index_changes_current").fetchone()[0] == 3
    assert store.get_ingestion(r.batch_id)["status"] == "success"


def test_load_symbol_changes(conn, store):
    rows = [{"date": "2022-06-09", "companyName": "Meta", "oldSymbol": "FB", "newSymbol": "META"},
            {"date": "1969-12-31", "companyName": "X", "oldSymbol": "ABC", "newSymbol": "ABCD"},
            {"date": "2023-10-26", "companyName": "Sprott", "oldSymbol": "COPP B", "newSymbol": "COPP"}]
    r = event_loader.load_symbol_changes(meta=store, fetcher_cls=fake_fetcher({"ALL": rows}))
    assert (r.write["inserted"], r.write["rejected"]) == (1, 2)
    issues = dict(conn.execute("SELECT detail->>'reason', symbol FROM meta.quality_issues WHERE rule_id = 'EVC001'"))
    assert issues == {"invalid_date": "ABC", "invalid_symbol": "COPP B"}   # 质量问题里保留原始代码


def test_load_acquisitions(conn, store):
    def row(sym, target, d):
        return {"symbol": sym, "companyName": "HomeTrust", "cik": "0001538263", "targetedSymbol": target,
                "targetedCompanyName": "Blue Ridge", "targetedCik": "0000842717", "transactionDate": d,
                "acceptedDate": f"{d} 17:04:52", "link": "https://sec"}
    pages = {0: [row("HTB", "BRBS", "2026-10-02"), row("HTBI", "BRBS", "2026-10-02")],
             1: [row("HTB", None, "2026-09-01")]}                          # 私有公司
    r = event_loader.load_acquisitions(max_pages=5, meta=store, fetcher_cls=fake_fetcher(pages))
    assert (r.write["inserted"], r.write["skipped"]) == (1, 1)
    accepted = conn.execute("SELECT accepted_at AT TIME ZONE 'America/New_York' FROM events.acquisitions").fetchone()[0]
    assert str(accepted) == "2026-10-02 17:04:52"                          # 按美东时间存


def test_load_dividends_and_splits(conn, store):
    div = event_loader.load_dividends(["AAPL"], meta=store, fetcher_cls=fake_fetcher({"AAPL": DIV_ROWS[:1]}))
    spl = event_loader.load_splits(["AAPL"], meta=store, fetcher_cls=fake_fetcher(
        {"AAPL": [{"symbol": "AAPL", "date": "2020-08-31", "numerator": 4, "denominator": 1, "splitType": "stock-split"}]}))
    assert div.write["inserted"] == 1 and spl.write["inserted"] == 1
    assert conn.execute("SELECT split_ratio FROM events.splits").fetchone()[0] == Decimal(4)


# =========================
# ✅ 代码写法映射（BRK.B ↔ BRK-B）
# =========================
@patch("fetch_data.fetch.time.sleep", return_value=None)
def test_fetch_maps_symbol_for_fmp(_):
    f = FetchData(["BRK.B", "AAPL"], "dividends", None)
    f.session = MagicMock()
    f.session.get.side_effect = [resp([{"symbol": "BRK-B", "date": "2020-01-02", "dividend": 1}]),
                                 resp([{"symbol": "AAPL", "date": "2020-01-02", "dividend": 1}])]
    df, summary, succeed, failed = f.fetch_fmp_batch()
    assert "symbol=BRK-B" in f.session.get.call_args_list[0][0][0]       # 请求用 FMP 写法
    assert df["symbol"].tolist() == ["BRK.B", "AAPL"]                   # 数据用内部写法
    assert succeed == ["BRK.B", "AAPL"]


def test_normalizer_maps_fmp_symbol_back():
    from fetch_data.events.common import to_symbol
    assert to_symbol("BRK-B") == "BRK.B" and to_symbol("bf-b") == "BF.B"
    assert to_symbol("MITT-PA") == "MITT-PA"                             # 没登记的不动
    r = normalize_splits([{"symbol": "BRK-B", "date": "2010-01-21", "numerator": 50, "denominator": 1}])
    assert r.events[0]["symbol"] == "BRK.B" and r.events[0]["source_event_key"] == "BRK.B|2010-01-21|split"


# =========================
# ✅ 分批写库
# =========================
def split_rows(symbol):
    return [{"symbol": symbol, "date": "2020-08-31", "numerator": 4, "denominator": 1, "splitType": "stock-split"}]


def test_chunked_load_aggregates(conn, store):
    data = {s: split_rows(s) for s in ["AAA", "BBB", "CCC", "DDD", "EEE"]}        # FFF 没有数据
    calls = []

    class Fake(fake_fetcher(data)):
        def __init__(self, units, endpoint, params):
            super().__init__(units, endpoint, params)
            calls.append(list(units))

    r = event_loader.load_splits(["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"], chunk_size=2, meta=store, fetcher_cls=Fake)
    assert calls == [["AAA", "BBB"], ["CCC", "DDD"], ["EEE", "FFF"]]          # 3 批
    assert (r.rows_fetched, r.write["inserted"]) == (5, 5)
    ing = store.get_ingestion(r.batch_id)
    assert (ing["n_symbols"], ing["n_success"], ing["n_missing"], ing["status"]) == (6, 5, 1, "success")
    # 所有批次的事件都属于同一个 ingestion 批次
    assert conn.execute("SELECT count(DISTINCT batch_id) FROM events.corporate_events").fetchone()[0] == 1


def test_chunked_load_keeps_written_chunks_on_crash(conn, store):
    data = {s: split_rows(s) for s in ["AAA", "BBB", "CCC", "DDD"]}

    class Crash(fake_fetcher(data)):
        def fetch_fmp_batch(self):
            if "CCC" in self.units:
                raise ConnectionError("网络断了")
            return super().fetch_fmp_batch()

    with pytest.raises(ConnectionError):
        event_loader.load_splits(["AAA", "BBB", "CCC", "DDD"], chunk_size=2, meta=store, fetcher_cls=Crash)
    assert conn.execute("SELECT count(*) FROM events.splits").fetchone()[0] == 2     # 第 1 批已写入，保留
    failed = conn.execute("SELECT status FROM meta.ingestion WHERE endpoint = 'splits'").fetchone()[0]
    assert failed == "failed"

    r = event_loader.load_splits(["AAA", "BBB", "CCC", "DDD"], chunk_size=2, meta=store,
                                 fetcher_cls=fake_fetcher(data))                       # 重跑
    assert (r.write["duplicate"], r.write["inserted"]) == (2, 2)
