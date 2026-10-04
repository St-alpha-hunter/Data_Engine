from datetime import date, time

import pandas as pd
import pytest

from pipelines.trading_calendar import CALENDAR_RULES, build_calendar, cross_check, run, upsert_calendar

"""

交易日历测试：生成逻辑（纯计算）+ 落库 + FMP 交叉核对（假数据）

"""


@pytest.fixture(scope="module")
def nyse_2015():
    return build_calendar("XNYS", date(2015, 1, 1), date(2015, 12, 31)).set_index("date")


# =========================
# ✅ 生成
# =========================
def test_trading_day_count(nyse_2015):
    assert int(nyse_2015["is_trading_day"].sum()) == 252
    assert len(nyse_2015) == 365


@pytest.mark.parametrize("d, session, name", [
    (date(2015, 1, 1), "closed", "New Year's Day"),
    (date(2015, 7, 3), "closed", "July 4th"),            # 7/4 是周六，周五补休
    (date(2015, 11, 26), "closed", "Thanksgiving"),
    (date(2015, 11, 27), "early_close", "Early close"),
    (date(2015, 11, 28), "closed", None),                # 普通周六
    (date(2015, 11, 30), "full", None),
])
def test_session_types(nyse_2015, d, session, name):
    row = nyse_2015.loc[d]
    assert row["session_type"] == session
    assert row["holiday_name"] == name
    assert row["is_trading_day"] == (session != "closed")


def test_times(nyse_2015):
    assert nyse_2015.loc[date(2015, 11, 27), "close_time"] == time(13, 0)
    assert nyse_2015.loc[date(2015, 11, 30), "open_time"] == time(9, 30)
    assert nyse_2015.loc[date(2015, 11, 30), "close_time"] == time(16, 0)
    assert nyse_2015.loc[date(2015, 11, 28), "open_time"] is None


def test_prev_next_and_seq(nyse_2015):
    sat = nyse_2015.loc[date(2015, 11, 28)]
    assert (sat["prev_trading_day"], sat["next_trading_day"]) == (date(2015, 11, 27), date(2015, 11, 30))
    thu = nyse_2015.loc[date(2015, 11, 26)]                  # 感恩节
    assert (thu["prev_trading_day"], thu["next_trading_day"]) == (date(2015, 11, 25), date(2015, 11, 27))
    # 交易日序号连续
    seq = nyse_2015[nyse_2015["is_trading_day"]]["trading_day_seq"].tolist()
    assert seq == list(range(1, 253))
    assert nyse_2015.loc[date(2015, 11, 28), "trading_day_seq"] is None


def test_special_closures():
    cal = build_calendar("XNYS", date(2012, 10, 1), date(2025, 1, 31)).set_index("date")
    for d in [date(2012, 10, 29), date(2012, 10, 30), date(2018, 12, 5), date(2025, 1, 9)]:
        assert cal.loc[d, "session_type"] == "closed"
        assert cal.loc[d, "holiday_name"] == "Special closure"


def test_new_year_on_saturday_friday_is_trading():
    """元旦是周六时，前一天周五照常交易（FMP 在 2027-12-31 标错了）"""
    cal = build_calendar("XNYS", date(2027, 12, 1), date(2028, 1, 31)).set_index("date")
    assert cal.loc[date(2027, 12, 31), "session_type"] == "full"


def test_xnas_built_separately():
    cal = build_calendar("XNAS", date(2015, 1, 1), date(2015, 12, 31))
    assert (cal["exchange"] == "XNAS").all()
    assert int(cal["is_trading_day"].sum()) == 252


# =========================
# ✅ 落库
# =========================
def test_upsert_idempotent(conn):
    cal = build_calendar("XNYS", date(2015, 1, 1), date(2015, 12, 31))
    assert upsert_calendar(conn, cal) == {"inserted": 365, "updated": 0, "unchanged": 0}
    assert upsert_calendar(conn, cal) == {"inserted": 0, "updated": 0, "unchanged": 365}

    changed = cal.copy()
    changed.loc[changed["date"] == date(2015, 11, 27), "holiday_name"] = "Day after Thanksgiving"
    assert upsert_calendar(conn, changed) == {"inserted": 0, "updated": 1, "unchanged": 364}

    row = conn.execute("SELECT session_type, close_time FROM ref.trading_calendar "
                       "WHERE exchange = 'XNYS' AND date = '2015-11-27'").fetchone()
    assert row == ("early_close", time(13, 0))


# =========================
# ✅ FMP 交叉核对
# =========================
def fmp_df(rows):
    return pd.DataFrame(rows, columns=["symbol", "date", "name", "isClosed", "adjOpenTime", "adjCloseTime"])


def fmp_2015_correct():
    closed = ["2015-01-01", "2015-01-19", "2015-02-16", "2015-04-03", "2015-05-25",
              "2015-07-03", "2015-09-07", "2015-11-26", "2015-12-25"]
    rows = [("NYSE", d, "holiday", True, None, None) for d in closed]
    rows += [("NYSE", "2015-11-27", "Thanksgiving Early Close", None, None, "13:00"),
             ("NYSE", "2015-12-24", "Christmas Early Close", None, None, "13:00")]
    return rows


def test_cross_check_clean(nyse_2015):
    issues = cross_check(nyse_2015.reset_index(), fmp_df(fmp_2015_correct()), "XNYS")
    assert issues == []


def test_cross_check_detects_differences(nyse_2015):
    rows = [r for r in fmp_2015_correct() if r[1] not in ("2015-04-03", "2015-12-24")]   # 少一个休市日、少一个提前收盘
    rows.append(("NYSE", "2015-06-01", "Fake holiday", True, None, None))               # 多一个休市日
    issues = cross_check(nyse_2015.reset_index(), fmp_df(rows), "XNYS")
    found = {(i["rule_id"], str(i["date"])) for i in issues}
    assert found == {("CAL001", "2015-06-01"), ("CAL002", "2015-04-03"), ("CAL003", "2015-12-24")}
    assert all(i["symbol"] == "XNYS" and i["severity"] == "warning" for i in issues)


def test_cross_check_early_close_cutoff(nyse_2015):
    rows = [r for r in fmp_2015_correct() if r[1] != "2015-12-24"]
    issues = cross_check(nyse_2015.reset_index(), fmp_df(rows), "XNYS", early_close_from="2016-01-01")
    assert issues == []                        # 2016 年以前的提前收盘不核对


def test_run_with_fake_fmp(store):
    class FakeFetchData:
        def __init__(self, symbols, endpoint_name, extra_params):
            self.symbols = symbols
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def fetch_fmp_batch(self):
            rows = fmp_2015_correct() + [("NASDAQ", r[1], r[2], r[3], r[4], r[5]) for r in fmp_2015_correct()]
            rows.append(("NYSE", "2015-06-01", "Fake holiday", True, None, None))
            df = fmp_df(rows)
            return df, {"success": 2, "missing": 0, "failed": 0, "missing_symbols": []}, self.symbols, []

    r = run(store=store, start=date(2015, 1, 1), end=date(2015, 12, 31), fetcher_cls=FakeFetchData)
    assert r["exchanges"]["XNYS"]["inserted"] == 365 and r["exchanges"]["XNAS"]["inserted"] == 365
    assert [(i["rule_id"], i["symbol"], str(i["date"])) for i in r["issues"]] == [("CAL001", "XNYS", "2015-06-01")]
    # 规则已同步、问题已落库、批次已记录
    assert set(store.active_rules("holidays_by_exchange")) == {"CAL001", "CAL002", "CAL003"}
    assert store.conn.execute("SELECT count(*) FROM meta.quality_issues WHERE rule_id = 'CAL001'").fetchone()[0] == 1
    assert store.get_ingestion(r["fmp_batch_id"])["status"] == "success"
