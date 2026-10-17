from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from zoneinfo import ZoneInfo

from pipelines import daily, event_loader
from pipelines.trading_calendar import build_calendar, upsert_calendar

"""

每日增量：执行时间 → 交易日、每天要拉的代码、按日历增量拉分红（测试库）

"""

SH = ZoneInfo("Asia/Shanghai")
D = date.fromisoformat


@pytest.fixture
def calendar(conn):
    upsert_calendar(conn, build_calendar("XNYS", date(2026, 10, 1), date(2026, 12, 31)))
    return conn


# =========================
# ✅ 执行时间 → 交易日
# =========================
@pytest.mark.parametrize("beijing, expected", [
    ("2026-10-14 08:00", "2026-10-13"),     # 周三早上 → 纽约周二
    ("2026-10-17 08:00", "2026-10-16"),     # 周六早上 → 纽约周五
    ("2026-10-18 08:00", None),             # 周日早上 → 纽约周六：跳过
    ("2026-11-27 08:00", None),             # 纽约 11-26 感恩节：跳过
    ("2026-11-04 08:00", "2026-11-03"),     # 夏令时结束之后（纽约晚上 7 点）也是前一天
])
def test_trade_date_for(calendar, beijing, expected):
    logical = datetime.fromisoformat(beijing).replace(tzinfo=SH)
    assert daily.trade_date_for(logical, calendar) == (D(expected) if expected else None)


def test_trade_date_requires_timezone_and_calendar(calendar):
    with pytest.raises(ValueError, match="时区"):
        daily.trade_date_for(datetime(2026, 10, 14, 8), calendar)
    with pytest.raises(RuntimeError, match="交易日历里没有"):
        daily.trade_date_for(datetime(2030, 1, 3, 8, tzinfo=SH), calendar)


# =========================
# ✅ 每天要拉的代码
# =========================
def security(conn, symbol, status="active", last=None):
    sid = conn.execute("INSERT INTO ref.securities (name, security_type, first_trade_date, last_trade_date, status, "
                       "review_status) VALUES (%s, 'common_stock', '2000-01-03', %s, %s, 'auto') RETURNING security_id",
                       (symbol, last, status)).fetchone()[0]
    conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, valid_to, source) "
                 "VALUES (%s, 'symbol', %s, '2000-01-03', %s, 'manual')",
                 (sid, symbol, (last + timedelta(days=1)) if last else None))


def test_daily_symbols_skip_long_delisted(conn):
    security(conn, "ZQLIVE")
    security(conn, "ZQRECENT", "delisted", D("2026-10-09"))   # 回看窗口内才退市：还要拉（补最后几天）
    security(conn, "ZQOLD", "delisted", D("2019-06-28"))      # 早就退市：不再每天拉
    assert daily.daily_symbols(conn, D("2026-10-13")) == ["ZQLIVE", "ZQRECENT"]


# =========================
# ✅ 按日历增量拉分红
# =========================
class FakeCalendarFetcher:
    """每天返回一条分红；记录每次请求的日期"""
    calls = []

    def __init__(self, units, endpoint, params):
        self.params = params

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def fetch_fmp_batch(self):
        d = self.params["from_date"]
        FakeCalendarFetcher.calls.append(d)
        assert d == self.params["to_date"]                                    # 一次只查一天
        df = pd.DataFrame([{"symbol": "JPM", "date": d.isoformat(), "dividend": 1.65, "adjDividend": 1.65,
                            "frequency": "Quarterly", "endpoint": "dividends_calendar"}])
        return df, {"success": 1, "missing": 0, "failed": 0, "missing_symbols": []}, ["x"], []


def test_dividends_calendar_one_request_per_day_and_idempotent(store):
    FakeCalendarFetcher.calls = []
    r = event_loader.load_dividends_calendar(D("2026-10-05"), D("2026-10-07"), meta=store,
                                             fetcher_cls=FakeCalendarFetcher)
    assert FakeCalendarFetcher.calls == [D("2026-10-05"), D("2026-10-06"), D("2026-10-07")]
    assert (r.rows_fetched, r.write["inserted"]) == (3, 3)
    again = event_loader.load_dividends_calendar(D("2026-10-05"), D("2026-10-07"), meta=store,
                                                 fetcher_cls=FakeCalendarFetcher)
    assert (again.write["inserted"], again.write["duplicate"]) == (0, 3)     # 重复拉不产生重复事件
