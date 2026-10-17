from datetime import date
from decimal import Decimal

import pytest

from fetch_data.events import (
    normalize_acquisitions, normalize_dividends, normalize_sp500_index_changes,
    normalize_splits, normalize_symbol_changes,
)
from fetch_data.events.acquisitions import pick_common_symbol
from fetch_data.events.common import to_cik, to_date, to_decimal, to_symbol

"""

事件整理函数测试：纯函数，不需要数据库。样例数据取自 FMP / GitHub 的真实格式

"""

TODAY = date(2026, 10, 6)


# =========================
# ✅ 公共清洗函数
# =========================
@pytest.mark.parametrize("value, expected", [
    ("aapl", "AAPL"), (" BRK.B ", "BRK.B"), ("MITT-PA", "MITT-PA"),
    ("COPP B", None), ("JW/A", None), ("FTAI$C", None), (None, None), (float("nan"), None),
])
def test_to_symbol(value, expected):
    assert to_symbol(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("2022-06-09", date(2022, 6, 9)), ("2026-10-02 17:04:52", date(2026, 10, 2)),
    ("1969-12-31", None), ("1970-01-01", None),                 # FMP 的占位日期
    ("1968-04-23", date(1968, 4, 23)), ("1962-10-31", date(1962, 10, 31)),   # 1970 年以前的真实日期（IBM、AEP 拆股）
    ("not-a-date", None), ("", None), (None, None),
])
def test_to_date(value, expected):
    assert to_date(value) == expected


def test_to_decimal_exact():
    assert to_decimal(0.28) == Decimal("0.28")                 # 不是 0.28000000000000003
    assert to_decimal("abc") is None and to_decimal(float("nan")) is None


def test_to_cik():
    assert to_cik("320193") == "0000320193" and to_cik(320193) == "0000320193"
    assert to_cik("0000320193") == "0000320193" and to_cik("abc") is None


# =========================
# ✅ 分红
# =========================
DIV_ROW = {"symbol": "AAPL", "date": "2026-08-10", "recordDate": "2026-08-10", "paymentDate": "2026-08-13",
           "declarationDate": "2026-07-30", "adjDividend": 0.27, "dividend": 0.27, "yield": 0.34, "frequency": "Quarterly"}


def test_dividend_basic():
    r = normalize_dividends([DIV_ROW], today=TODAY)
    [e] = r.events
    assert (e["symbol"], e["ex_date"], e["amount"], e["dividend_type"], e["currency"], e["status"]) == \
           ("AAPL", date(2026, 8, 10), Decimal("0.27"), "regular", "USD", "confirmed")
    assert e["source_event_key"] == "AAPL|2026-08-10|dividend"
    assert "yield" not in e["payload"]                             # yield 按当前股价算、每天都变：不保存（否则产生假修订）
    assert "yield" not in e


def test_dividend_special_future_and_missing_dates():
    row = {**DIV_ROW, "date": "2026-12-01", "frequency": "Special", "declarationDate": None, "paymentDate": ""}
    [e] = normalize_dividends([row], today=TODAY).events
    assert (e["dividend_type"], e["status"], e["declaration_date"], e["payment_date"]) == ("special", "announced", None, None)


@pytest.mark.parametrize("change, reason", [
    ({"symbol": "aa pl"}, "invalid_symbol"), ({"date": "1969-12-31"}, "invalid_date"),
    ({"dividend": 0}, "invalid_amount"), ({"dividend": None}, "invalid_amount"),
    ({"frequency": "Daily"}, "unknown_frequency"),
])
def test_dividend_rejected(change, reason):
    r = normalize_dividends([{**DIV_ROW, **change}], today=TODAY)
    assert r.events == [] and r.rejected[0]["reason"] == reason


def test_dividend_duplicate_in_batch():
    r = normalize_dividends([DIV_ROW, DIV_ROW], today=TODAY)
    assert len(r.events) == 1 and r.summary()["skipped"] == {"duplicate_in_batch": 1}


# =========================
# ✅ 拆股
# =========================
@pytest.mark.parametrize("row, expected_type", [
    ({"numerator": 4, "denominator": 1, "splitType": "stock-split"}, "forward"),
    ({"numerator": 1, "denominator": 20, "splitType": "stock-split"}, "reverse"),
    ({"numerator": 1, "denominator": 5, "splitType": None}, "reverse"),             # splitType 为空：按比例推导
    ({"numerator": 21, "denominator": 20, "splitType": "stock-dividend"}, "stock_dividend"),
    ({"numerator": 999, "denominator": 500, "splitType": "stock-split"}, "forward"),  # GOOGL 2014
    ({"numerator": 4717, "denominator": 5000, "splitType": "stock-dividend"}, "adjustment"),   # BK 2007 合并
    ({"numerator": 989, "denominator": 1000, "splitType": "stock-dividend"}, "adjustment"),    # NBGEX
])
def test_split_types(row, expected_type):
    [e] = normalize_splits([{"symbol": "X", "date": "2020-08-31", **row}], today=TODAY).events
    assert e["split_type"] == expected_type
    assert e["numerator"] == Decimal(str(row["numerator"]))


@pytest.mark.parametrize("row, reason", [
    ({"numerator": 1, "denominator": 1}, "ratio_one_to_one"),
    ({"numerator": 0, "denominator": 1}, "invalid_ratio"),
])
def test_split_rejected(row, reason):
    r = normalize_splits([{"symbol": "X", "date": "2020-08-31", **row}], today=TODAY)
    assert r.rejected[0]["reason"] == reason


# =========================
# ✅ 代码变更
# =========================
def test_symbol_change():
    rows = [
        {"date": "2022-06-09", "companyName": "Meta Platforms, Inc.", "oldSymbol": "FB", "newSymbol": "META"},
        {"date": "1969-12-31", "companyName": "X", "oldSymbol": "ABC", "newSymbol": "ABCD"},
        {"date": "2023-10-26", "companyName": "Sprott", "oldSymbol": "COPP B", "newSymbol": "COPP"},
        {"date": "2023-01-03", "companyName": "Y", "oldSymbol": "SAME", "newSymbol": "SAME"},
    ]
    r = normalize_symbol_changes(rows, today=TODAY)
    [e] = r.events
    assert (e["old_symbol"], e["new_symbol"], e["change_date"], e["source_event_key"]) == \
           ("FB", "META", date(2022, 6, 9), "FB|META|2022-06-09")
    assert sorted(x["reason"] for x in r.rejected) == ["invalid_date", "invalid_symbol", "same_symbol"]


# =========================
# ✅ 收购
# =========================
def ma_row(symbol, target_symbol, tdate, cik="0001538263", tcik="0000842717", accepted=None):
    return {"symbol": symbol, "companyName": "HomeTrust Bancshares, Inc.", "cik": cik,
            "targetedCompanyName": "Blue Ridge Bankshares, Inc.", "targetedCik": tcik, "targetedSymbol": target_symbol,
            "transactionDate": tdate, "acceptedDate": accepted or f"{tdate} 17:04:52", "link": f"https://sec/{tdate}"}


def test_acquisition_merges_duplicate_rows():
    rows = [ma_row("HTBI", "BRBS", "2026-10-02"), ma_row("HTB", "BRBS", "2026-10-02"),
            ma_row("HTB", "BRBS", "2026-08-15")]                           # 同一笔交易：两个代码 + 两份文件
    r = normalize_acquisitions(rows)
    [e] = r.events
    assert e["deal_key"] == "0000842717|0001538263"
    assert e["filing_date"] == date(2026, 8, 15)                           # 最早的文件日期
    assert e["filing_url"] == "https://sec/2026-08-15"
    assert (e["target_symbol"], e["acquirer_symbol"]) == ("BRBS", "HTB")
    assert e["payload"]["acquirer_symbols"] == ["HTB", "HTBI"]
    assert len(e["payload"]["rows"]) == 3
    assert e["completion_date"] is None


def test_acquisition_private_target_skipped_and_separate_deals():
    rows = [ma_row("AAA", "BBB", "2024-01-10", cik="1", tcik="2"),
            ma_row("AAA", "CCC", "2024-03-10", cik="1", tcik="3"),
            ma_row("AAA", None, "2024-05-10", cik="1", tcik="4")]               # 私有公司
    r = normalize_acquisitions(rows)
    assert sorted(e["target_symbol"] for e in r.events) == ["BBB", "CCC"]     # A 收购两家：两笔交易
    assert r.summary()["skipped"] == {"private_target": 1}


@pytest.mark.parametrize("symbols, expected", [
    (["MITT", "MITT-PA", "MITT-PB"], "MITT"), (["CCXI", "CCXIU", "CCXIW"], "CCXI"),
    (["HTB", "HTBI"], "HTB"), ([None], None),
])
def test_pick_common_symbol(symbols, expected):
    assert pick_common_symbol(symbols) == expected


# =========================
# ✅ S&P 500 成分
# =========================
def test_sp500_add_remove():
    rows = [{"ticker": "FB", "start_date": "2013-12-23", "end_date": "2022-06-09"},
            {"ticker": "META", "start_date": "2022-06-09", "end_date": float("nan")},     # pandas 读出的空值
            {"ticker": "BAD", "start_date": "2020-01-02", "end_date": "2019-01-02"}]
    r = normalize_sp500_index_changes(rows)
    assert [(e["symbol"], e["action"], str(e["effective_date"])) for e in r.events] == [
        ("FB", "add", "2013-12-23"), ("FB", "remove", "2022-06-09"), ("META", "add", "2022-06-09")]
    assert r.events[0]["source_event_key"] == "SP500|FB|add|2013-12-23"
    assert r.events[2]["payload"]["end_date"] is None                     # NaN 存成 null
    assert r.rejected[0]["reason"] == "end_before_start"


def test_payload_is_json_serializable():
    import json
    for r in [normalize_dividends([DIV_ROW], today=TODAY),
              normalize_acquisitions([ma_row("HTB", "BRBS", "2026-10-02")]),
              normalize_sp500_index_changes([{"ticker": "META", "start_date": "2022-06-09", "end_date": float("nan")}])]:
        for e in r.events:
            json.dumps(e["payload"])


def test_dividend_payload_excludes_yield():
    """股息率按当前股价算、每天都变：不算进内容，否则每拉一次就多一个假修订"""
    row = {"symbol": "JPM", "date": "2026-10-06", "dividend": 1.65, "adjDividend": 1.65, "frequency": "Quarterly"}
    a = normalize_dividends([{**row, "yield": 1.8566}]).events[0]["payload"]
    b = normalize_dividends([{**row, "yield": 1.8564}]).events[0]["payload"]
    assert a == b and "yield" not in a


def test_acquisition_payload_order_is_deterministic():
    """同一份文件按不同股票类别各给一行（日期、受理时间相同）：输入顺序不同，合并后的内容也要一样"""
    base = {"targetedSymbol": "TGT", "targetedCik": "0000000001", "cik": "0000000002", "transactionDate": "2026-10-01",
            "acceptedDate": "2026-10-01 16:00:00", "link": "https://sec/x", "companyName": "Acq"}
    rows = [{**base, "symbol": "ACQ"}, {**base, "symbol": "ACQ.B"}]
    p1 = normalize_acquisitions(rows).events[0]["payload"]
    p2 = normalize_acquisitions(rows[::-1]).events[0]["payload"]
    assert p1 == p2
