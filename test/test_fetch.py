import pytest
import requests
from unittest.mock import patch, MagicMock
from fetch_data.fetch import FetchData

"""

测试数据输入点

"""

MOCK_DATA = {
    "end_name" :"financial_estimate",
    "extra_params": {
        "period": "annual",
        "page": 0,
        "limit": 10
    },
    "data": [
	{
		"symbol": "AAPL",
		"date": "2029-09-28",
		"revenueLow": 483092500000,
		"revenueHigh": 483093500000,
		"revenueAvg": 483093000000,
		"ebitdaLow": 155952166036,
		"ebitdaHigh": 155952488856,
		"ebitdaAvg": 155952327446,
		"ebitLow": 140628295747,
		"ebitHigh": 140628586847,
		"ebitAvg": 140628441297,
		"netIncomeLow": 139446957701,
		"netIncomeHigh": 157185372990,
		"netIncomeAvg": 149150359609,
		"sgaExpenseLow": 31694652812,
		"sgaExpenseHigh": 31694718420,
		"sgaExpenseAvg": 31694685616,
		"epsAvg": 9.68,
		"epsHigh": 10.20148,
		"epsLow": 9.05024,
		"numAnalystsRevenue": 16,
		"numAnalystsEps": 6
	}
]
}


# =========================
# 工具函数
# =========================
def make_resp(status_code=200, json_data=None, text=""):
    """构造一个假的 response；json_data 传异常时模拟 JSON 解析失败。"""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    if isinstance(json_data, Exception):
        resp.json.side_effect = json_data
    else:
        resp.json.return_value = json_data
    return resp


@pytest.fixture(autouse=True)
def no_sleep():
    """所有测试都跳过 time.sleep，避免重试等待拖慢测试。"""
    with patch("fetch_data.fetch.time.sleep", return_value=None) as mock_sleep:
        yield mock_sleep


def run_fetch(responses, symbols=("AAPL",), endpoint_name=MOCK_DATA["end_name"],
              extra_params=MOCK_DATA["extra_params"]):
    """
    responses 按顺序作为每次 session.get 的返回值（或抛出的异常）。
    返回 (fetch 实例, df, summary, succeed_list, failed_list)。
    """
    fetch = FetchData(list(symbols), endpoint_name=endpoint_name, extra_params=extra_params)
    fetch.session = MagicMock()
    fetch.session.get.side_effect = responses
    df, summary, succeed_list, failed_list = fetch.fetch_fmp_batch()
    return fetch, df, summary, succeed_list, failed_list


# =========================
# ✅ 成功测试
# =========================
def test_fetch_success():
    fetch, df, summary, succeed_list, failed_list = run_fetch([make_resp(200, MOCK_DATA["data"])])

    assert succeed_list == ["AAPL"]
    assert failed_list == []
    assert not df.empty
    assert (df["symbol"] == "AAPL").all()
    assert (df["endpoint"] == MOCK_DATA["end_name"]).all()
    assert summary["endpoint"] == MOCK_DATA["end_name"]
    assert summary["success"] == 1


def test_fetch_url_format():
    """URL 里的 symbol / period / page / limit 都应被正确替换。"""
    fetch, *_ = run_fetch([make_resp(200, MOCK_DATA["data"])])

    url = fetch.session.get.call_args[0][0]
    assert "symbol=AAPL" in url
    assert "period=annual" in url
    assert "page=0" in url
    assert "limit=10" in url
    assert "{" not in url


# =========================
# ✅ 无数据测试（重要）
# =========================
def test_fetch_missing():
    fetch, df, summary, succeed_list, failed_list = run_fetch([make_resp(200, [])])

    assert succeed_list == []
    assert failed_list == []
    assert df.empty
    assert summary["missing"] == 1
    assert summary["missing_symbols"] == ["AAPL"]


# =========================
# ✅ 不重试的错误：只请求一次就记失败
# =========================
@pytest.mark.parametrize("resp, error_type", [
    (make_resp(401, text="Invalid API KEY"), "auth_error"),
    (make_resp(402, text="upgrade plan"), "auth_error"),
    (make_resp(403, text="forbidden"), "auth_error"),
    (make_resp(404, text="not found"), "client_error"),
    (make_resp(200, ValueError("bad json"), text="<html>"), "invalid_json"),
    (make_resp(200, {"Error Message": "Invalid API KEY"}), "api_error"),
])
def test_fetch_no_retry_errors(resp, error_type):
    fetch, df, summary, succeed_list, failed_list = run_fetch([resp])

    assert fetch.session.get.call_count == 1
    assert succeed_list == []
    assert df.empty
    assert summary["failed"] == 1
    assert len(failed_list) == 1
    assert failed_list[0]["error_type"] == error_type
    assert failed_list[0]["endpoint"] == MOCK_DATA["end_name"]
    assert failed_list[0]["attempts"] == 1


# =========================
# ✅ 可重试的错误：重试满 3 次后记一条失败
# =========================
@pytest.mark.parametrize("make_one, error_type", [
    (lambda: make_resp(429), "rate_limited"),
    (lambda: make_resp(500), "server_error"),
    (lambda: make_resp(503), "server_error"),
    (lambda: requests.exceptions.Timeout(), "timeout"),
    (lambda: requests.exceptions.ConnectionError("conn reset"), "request_error"),
])
def test_fetch_retry_exhausted(make_one, error_type):
    fetch, df, summary, succeed_list, failed_list = run_fetch([make_one() for _ in range(3)])

    assert fetch.session.get.call_count == fetch.retries
    assert succeed_list == []
    assert len(failed_list) == 1
    assert failed_list[0]["error_type"] == error_type
    assert failed_list[0]["attempts"] == fetch.retries
    assert failed_list[0]["endpoint"] == MOCK_DATA["end_name"]


def test_fetch_retry_then_success():
    """前两次失败、第三次成功：应算成功，不留失败记录。"""
    fetch, df, summary, succeed_list, failed_list = run_fetch([
        make_resp(429),
        requests.exceptions.Timeout(),
        make_resp(200, MOCK_DATA["data"]),
    ])

    assert fetch.session.get.call_count == 3
    assert succeed_list == ["AAPL"]
    assert failed_list == []
    assert not df.empty


def test_fetch_multi_symbols_mixed():
    """多只股票：成功 / 无数据 / 失败各一只，互不影响。"""
    fetch, df, summary, succeed_list, failed_list = run_fetch(
        [make_resp(200, MOCK_DATA["data"]), make_resp(200, []), make_resp(403)],
        symbols=("AAPL", "MSFT", "NVDA"),
    )

    assert succeed_list == ["AAPL"]
    assert summary["missing_symbols"] == ["MSFT"]
    assert [f["symbol"] for f in failed_list] == ["NVDA"]
    assert summary == {
        "endpoint": MOCK_DATA["end_name"],
        "total": 3,
        "success": 1,
        "missing": 1,
        "failed": 1,
        "missing_symbols": ["MSFT"],
    }


# =========================
# ✅ endpoint 校验：请求前直接报错
# =========================
def test_unknown_endpoint():
    with pytest.raises(ValueError, match="未知 endpoint"):
        run_fetch([], endpoint_name="not_exist")


def test_missing_required_params():
    with pytest.raises(ValueError, match="缺少必填参数"):
        run_fetch([], endpoint_name="price_volume", extra_params={"from_date": "2024-01-01"})


# =========================
# ✅ session 管理
# =========================
def test_session_reusable_and_closed_by_with():
    """同一实例可多次调用；session 只在退出 with 时关闭。"""
    with FetchData(["AAPL"], MOCK_DATA["end_name"], MOCK_DATA["extra_params"]) as fetch:
        fetch.session = MagicMock()
        fetch.session.get.side_effect = [make_resp(200, MOCK_DATA["data"]) for _ in range(2)]

        _, _, first, _ = fetch.fetch_fmp_batch()
        _, _, second, _ = fetch.fetch_fmp_batch()

        assert first == second == ["AAPL"]
        fetch.session.close.assert_not_called()

    fetch.session.close.assert_called_once()


# =========================
# ✅ save_file 测试
# =========================
# @patch("fetch_data.fetch_market_cap_1y_fmp.pd.DataFrame.to_parquet")
# def test_save_file(mock_to_parquet):
#
#     df = pd.DataFrame({
#         "symbol": ["AAPL"],
#         "date": ["2025-01-01"],
#         "marketCap": [1000]
#     })
#
#     result = save_file(df)
#
#     assert isinstance(result, pd.DataFrame)
#     assert "symbol" in result.index.names
#     mock_to_parquet.assert_called_once()


# =========================
# ✅ 单次返回上限：按日期分段、顶到上限自动拆开
# =========================
from datetime import date as _date
from urllib.parse import parse_qs, urlparse

from fetch_data import fetch as fetch_module


def url_range(call):
    q = parse_qs(urlparse(call[0][0]).query)
    return q["from"][0], q["to"][0]


@pytest.fixture
def small_limits(monkeypatch):
    """把日线接口改成：每段 10 天、单次最多 3 行，方便构造"""
    cfg = {**fetch_module.FMP_ENDPOINTS["price_volume"], "max_rows": 3, "chunk_days": 10}
    monkeypatch.setitem(fetch_module.FMP_ENDPOINTS, "price_volume", cfg)
    return cfg


def rows(*dates):
    return [{"date": d, "close": 1.0} for d in dates]


def test_date_windows_cover_range_without_overlap():
    w = FetchData._date_windows({"from_date": "2020-01-01", "to_date": "2020-01-25"}, 10)
    assert w == [(_date(2020, 1, 1), _date(2020, 1, 10)), (_date(2020, 1, 11), _date(2020, 1, 20)),
                 (_date(2020, 1, 21), _date(2020, 1, 25))]
    assert FetchData._date_windows({"symbol": "X"}, 10) == [None]          # 没有日期参数：不分段


def test_chunked_requests(small_limits):
    fetch, df, summary, succeed, failed = run_fetch(
        [make_resp(200, rows("2020-01-02")), make_resp(200, rows("2020-01-13", "2020-01-14"))],
        endpoint_name="price_volume", extra_params={"from_date": "2020-01-01", "to_date": "2020-01-20"})
    assert [url_range(c) for c in fetch.session.get.call_args_list] == [("2020-01-01", "2020-01-10"), ("2020-01-11", "2020-01-20")]
    assert list(df["date"]) == ["2020-01-02", "2020-01-13", "2020-01-14"] and succeed == ["AAPL"]


def test_window_hitting_limit_is_split(small_limits):
    """第一段返回 3 行（达到上限，可能被截断）→ 拆成两半重拉，顶到上限的那次数据不要"""
    fetch, df, summary, succeed, failed = run_fetch(
        [make_resp(200, rows("2020-01-06", "2020-01-07", "2020-01-08")),    # 01-01~01-10：顶到上限
         make_resp(200, rows("2020-01-02", "2020-01-03")),                  # 01-01~01-05
         make_resp(200, rows("2020-01-06", "2020-01-07")),                  # 01-06~01-10
         make_resp(200, [])],                                               # 01-11~01-20
        endpoint_name="price_volume", extra_params={"from_date": "2020-01-01", "to_date": "2020-01-20"})
    assert [url_range(c) for c in fetch.session.get.call_args_list] == [
        ("2020-01-01", "2020-01-10"), ("2020-01-01", "2020-01-05"), ("2020-01-06", "2020-01-10"), ("2020-01-11", "2020-01-20")]
    assert list(df["date"]) == ["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"]


def test_failed_window_fails_whole_symbol(small_limits):
    """任一段失败：整只股票失败，不返回前面已拉到的部分（避免只写入半段历史）"""
    fetch, df, summary, succeed, failed = run_fetch(
        [make_resp(200, rows("2020-01-02")), make_resp(403)],
        endpoint_name="price_volume", extra_params={"from_date": "2020-01-01", "to_date": "2020-01-20"})
    assert df.empty and succeed == [] and [f["error_type"] for f in failed] == ["auth_error"]


def test_limit_without_chunking_is_failure(monkeypatch):
    """配了上限、没配分段的接口：顶到上限算失败（error_type = truncated）"""
    cfg = {**fetch_module.FMP_ENDPOINTS[MOCK_DATA["end_name"]], "max_rows": 1}
    monkeypatch.setitem(fetch_module.FMP_ENDPOINTS, MOCK_DATA["end_name"], cfg)
    _, df, _, succeed, failed = run_fetch([make_resp(200, MOCK_DATA["data"])])
    assert df.empty and [f["error_type"] for f in failed] == ["truncated"]
