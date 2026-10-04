import numpy as np
import pandas as pd
import pytest

from clean.base_clean import CleanResult
from clean.clean_price_volume_fmp_dev import PriceVolume

"""

PriceVolume.validate() 单元测试：纯计算，不需要数据库

"""


def make_df(rows):
    """rows: (symbol, date, open, high, low, close, volume)；故意按日期倒序给，模拟 FMP"""
    df = pd.DataFrame(rows, columns=["symbol", "date", "adjOpen", "adjHigh", "adjLow", "adjClose", "volume"])
    return df.iloc[::-1].reset_index(drop=True)


NORMAL = [
    ("AAPL", "2022-01-03", 100.0, 102.0, 99.0, 101.0, 1000.0),
    ("AAPL", "2022-01-04", 101.0, 103.0, 100.0, 102.0, 1100.0),
    ("AAPL", "2022-01-05", 102.0, 104.0, 101.0, 103.0, 1200.0),
]


def issues_of(result: CleanResult, rule_id: str):
    return [i for i in result.issues if i["rule_id"] == rule_id]


# =========================
# ✅ 正常数据
# =========================
def test_all_health():
    result = PriceVolume(make_df(NORMAL)).validate()

    assert len(result.clean_df) == 3
    assert result.quarantine_df.empty
    assert result.issues == []
    assert (result.clean_df["quality_status"] == "health").all()
    # 只输出原始字段 + quality_status，辅助列 return / prev_return 不输出
    assert list(result.clean_df.columns) == PriceVolume.config["output_columns"] + ["quality_status"]
    # 按日期正序
    assert result.clean_df["date"].is_monotonic_increasing


def test_return_computed_in_ascending_order():
    """收益率按正序计算（FMP 给的是倒序）"""
    cleaner = PriceVolume(make_df(NORMAL))
    cleaner.validate()
    returns = cleaner.raw_df["return"].tolist()
    assert np.isnan(returns[0])
    assert returns[1] == pytest.approx(102 / 101 - 1)


# =========================
# ✅ danger：进隔离区
# =========================
@pytest.mark.parametrize("bad_row, rule_id", [
    (("AAPL", "2022-01-06", 0.0, 104.0, 0.0, 103.0, 1000.0), "PV001"),        # 价格为 0
    (("AAPL", "2022-01-06", 103.0, 104.0, 102.0, 103.0, -5.0), "PV001"),      # 成交量为负
    (("AAPL", "2022-01-06", 103.0, 101.0, 102.0, 103.0, 1000.0), "PV002"),    # 最高价 < 最低价
    (("AAPL", "2022-01-06", np.nan, 104.0, 102.0, 103.0, 1000.0), "PV005"),   # 价格缺失
    (("AAPL", "2022-01-06", 103.0, 104.0, 102.0, 103.0, np.nan), "PV005"),    # 成交量缺失
])
def test_danger_goes_to_quarantine(bad_row, rule_id):
    result = PriceVolume(make_df(NORMAL + [bad_row])).validate()

    assert len(result.clean_df) == 3
    assert len(result.quarantine_df) == 1
    assert result.quarantine_df.iloc[0]["date"] == pd.Timestamp("2022-01-06")
    found = issues_of(result, rule_id)
    assert len(found) == 1
    assert found[0]["severity"] == "danger"
    assert found[0]["symbol"] == "AAPL"


def test_zero_volume_is_ok():
    """成交量为 0（停牌）不算异常"""
    row = ("AAPL", "2022-01-06", 103.0, 104.0, 102.0, 103.0, 0.0)
    result = PriceVolume(make_df(NORMAL + [row])).validate()
    assert result.quarantine_df.empty
    assert issues_of(result, "PV001") == []


def test_one_row_multiple_danger_rules_quarantined_once():
    """一行同时触发 PV001 和 PV002：隔离一次，问题记两条"""
    row = ("AAPL", "2022-01-06", 0.0, 50.0, 60.0, 0.0, 1000.0)
    result = PriceVolume(make_df(NORMAL + [row])).validate()
    assert len(result.quarantine_df) == 1
    assert {i["rule_id"] for i in result.issues} >= {"PV001", "PV002"}


# =========================
# ✅ warning：进 golden，打标
# =========================
def test_big_range_is_warning():
    row = ("AAPL", "2022-01-06", 103.0, 130.0, 100.0, 120.0, 1000.0)    # 振幅 30%
    result = PriceVolume(make_df(NORMAL + [row])).validate()

    assert result.quarantine_df.empty
    flagged = result.clean_df[result.clean_df["quality_status"] == "warning"]
    assert flagged["date"].tolist() == [pd.Timestamp("2022-01-06")]
    [issue] = issues_of(result, "PV003")
    assert issue["severity"] == "warning"
    assert issue["detail"]["adjHigh"] == 130.0


def test_big_range_skips_non_positive_price():
    """PV003 v2：价格 <= 0 的行不做振幅检查（已由 PV001 隔离）"""
    row = ("AAPL", "2022-01-06", 0.0, 104.0, 0.0, 103.0, 1000.0)
    result = PriceVolume(make_df(NORMAL + [row])).validate()
    assert issues_of(result, "PV003") == []
    assert len(issues_of(result, "PV001")) == 1


def test_threshold_comes_from_rule_params(monkeypatch):
    """阈值来自 Rule.params：调大阈值后同一行不再触发"""
    row = ("AAPL", "2022-01-06", 103.0, 130.0, 100.0, 120.0, 1000.0)
    rules = [r if r.rule_id != "PV003" else type(r)(**{**r.__dict__, "params": {"max_range": 0.50}})
             for r in PriceVolume.rules]
    monkeypatch.setattr(PriceVolume, "rules", rules)
    result = PriceVolume(make_df(NORMAL + [row])).validate()
    assert issues_of(result, "PV003") == []


# =========================
# ✅ 行数守恒（对账依赖这个）
# =========================
def test_rows_conserved_with_duplicates():
    rows = NORMAL + [NORMAL[0], ("AAPL", "2022-01-06", 0.0, 1.0, 0.0, 1.0, 1.0)]
    result = PriceVolume(make_df(rows)).validate()
    s = result.stats
    assert s["rows_in"] == 5
    assert s["duplicates_dropped"] == 1
    assert s["rows_in"] == s["duplicates_dropped"] + s["health"] + s["warning"] + s["danger"]


# =========================
# ✅ 兼容旧接口
# =========================
def test_legacy_clean_still_works():
    """clean() 走新流程；errors / build_stats 仍可给 FeedErrorDeputy 用"""
    row = ("AAPL", "2022-01-06", 0.0, 104.0, 0.0, 103.0, 1000.0)
    cleaner = PriceVolume(make_df(NORMAL + [row]))
    clean_df = cleaner.clean()
    assert len(clean_df) == 3
    # 价格为 0 只触发 PV001；PV003 v2 跳过价格 <= 0 的行
    assert [e["error_name"] for e in cleaner.errors] == ["PV001"]
    cleaner.build_stats()
    assert "return_mean" in cleaner.stats_df.columns


def test_rule_definitions():
    defs = PriceVolume.rule_definitions()
    assert [d["rule_id"] for d in defs] == ["PV001", "PV002", "PV003", "PV004", "PV005"]
    assert all(d["endpoint"] == "price_volume" for d in defs)
    assert (defs[2]["rule_version"], defs[2]["params"]) == (2, {"max_range": 0.15})
    assert defs[2]["impl"] == "PriceVolume._check_daily_volatility"
