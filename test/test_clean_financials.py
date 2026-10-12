import pandas as pd
import pytest

from clean.registry import get_cleaner

"""

报表类、估值类等清洗规则：每个清洗类用一行干净数据 + 几行故意出错的数据，
确认干净行不被误报、出错行被对应规则抓到（danger 隔离、warning 保留但标记）

"""


def run(endpoint, rows):
    result = get_cleaner(endpoint)(pd.DataFrame(rows)).validate()
    by_symbol = {}
    for i in result.issues:
        by_symbol.setdefault(i["symbol"], set()).add(i["rule_id"])
    return result, by_symbol


def variant(base, symbol, **changes):
    return {**base, "symbol": symbol, **changes}


# =========================
# 利润表
# =========================
INCOME = {
    "date": "2024-09-28", "symbol": "OK", "period": "FY",
    "revenue": 1000.0, "costOfRevenue": 600.0, "grossProfit": 400.0, "operatingExpenses": 150.0,
    "costAndExpenses": 750.0, "operatingIncome": 250.0, "totalOtherIncomeExpensesNet": 10.0,
    "incomeBeforeTax": 260.0, "incomeTaxExpense": 60.0, "netIncomeFromContinuingOperations": 200.0,
    "netIncome": 200.0, "bottomLineNetIncome": 200.0, "ebit": 250.0, "depreciationAndAmortization": 50.0,
    "ebitda": 300.0, "eps": 2.0, "epsDiluted": 1.98, "weightedAverageShsOut": 100.0, "weightedAverageShsOutDil": 101.0,
}


def test_income_statement():
    result, issues = run("income_statement", [
        INCOME,
        variant(INCOME, "GP", grossProfit=500.0),                     # 毛利对不上 → danger
        variant(INCOME, "EPS", eps=3.0, epsDiluted=2.9),              # EPS 对不上（DUK 那种）→ warning
        variant(INCOME, "SHR", weightedAverageShsOut=0.0),            # 股数 0 → danger
        variant(INCOME, "DIL", epsDiluted=2.5),                       # 稀释 EPS 高于基本 → warning
    ])
    assert "OK" not in issues
    assert "IS003" in issues["GP"] and issues["EPS"] == {"IS009"} and "IS002" in issues["SHR"] and issues["DIL"] == {"IS010"}
    assert set(result.quarantine_df["symbol"]) == {"GP", "SHR"}
    assert dict(zip(result.clean_df["symbol"], result.clean_df["quality_status"])) == {"OK": "health", "EPS": "warning", "DIL": "warning"}


# =========================
# 资产负债表
# =========================
BALANCE = {
    "date": "2024-09-28", "symbol": "OK", "period": "FY",
    "cashAndCashEquivalents": 30.0, "shortTermInvestments": 20.0, "cashAndShortTermInvestments": 50.0,
    "inventory": 10.0, "goodwill": 5.0, "intangibleAssets": 5.0, "propertyPlantEquipmentNet": 40.0,
    "totalCurrentAssets": 100.0, "totalNonCurrentAssets": 200.0, "otherAssets": 0.0, "totalAssets": 300.0,
    "totalCurrentLiabilities": 80.0, "totalNonCurrentLiabilities": 120.0, "otherLiabilities": 0.0, "totalLiabilities": 200.0,
    "totalStockholdersEquity": 90.0, "minorityInterest": 10.0, "totalEquity": 100.0, "totalLiabilitiesAndTotalEquity": 300.0,
    "totalDebt": 70.0, "netDebt": 40.0,
}


def test_balance_sheet():
    result, issues = run("balance_sheet", [
        BALANCE,
        variant(BALANCE, "UNBAL", totalAssets=310.0),                 # 资产 ≠ 负债和权益 → danger
        variant(BALANCE, "MINOR", totalLiabilities=210.0),            # WBA：少数股东权益重复计入 → danger
        variant(BALANCE, "NEG", inventory=-1.0),                      # 存货为负 → warning
        variant(BALANCE, "ND", netDebt=10.0),                         # 净债务对不上 → warning
    ])
    assert "OK" not in issues
    assert "BS004" in issues["UNBAL"] and "BS005" in issues["MINOR"] and issues["NEG"] == {"BS003"} and issues["ND"] == {"BS009"}
    assert set(result.quarantine_df["symbol"]) == {"UNBAL", "MINOR"}


# =========================
# 现金流量表
# =========================
CASH = {
    "date": "2024-09-28", "symbol": "OK", "period": "FY",
    "netIncome": 100.0, "depreciationAndAmortization": 20.0, "deferredIncomeTax": 0.0, "stockBasedCompensation": 10.0,
    "changeInWorkingCapital": 5.0, "otherNonCashItems": -5.0, "netCashProvidedByOperatingActivities": 130.0,
    "investmentsInPropertyPlantAndEquipment": -30.0, "acquisitionsNet": 0.0, "purchasesOfInvestments": -20.0,
    "salesMaturitiesOfInvestments": 10.0, "otherInvestingActivities": 0.0, "netCashProvidedByInvestingActivities": -40.0,
    "netDebtIssuance": -10.0, "longTermNetDebtIssuance": -10.0, "shortTermNetDebtIssuance": 0.0,
    "netStockIssuance": -50.0, "netCommonStockIssuance": -50.0, "netPreferredStockIssuance": 0.0, "commonStockRepurchased": -50.0,
    "netDividendsPaid": -20.0, "commonDividendsPaid": -20.0, "preferredDividendsPaid": 0.0,
    "otherFinancingActivities": 0.0, "netCashProvidedByFinancingActivities": -80.0,
    "effectOfForexChangesOnCash": 0.0, "netChangeInCash": 10.0, "cashAtBeginningOfPeriod": 50.0, "cashAtEndOfPeriod": 60.0,
    "operatingCashFlow": 130.0, "capitalExpenditure": -30.0, "freeCashFlow": 100.0,
}


def test_cash_flow():
    result, issues = run("cash_flow", [
        CASH,
        variant(CASH, "END", cashAtEndOfPeriod=70.0),                 # 期末 ≠ 期初 + 变动 → danger
        variant(CASH, "CAPEX", capitalExpenditure=30.0, freeCashFlow=160.0),   # 资本开支为正 → warning
        variant(CASH, "DIVZ", commonDividendsPaid=0.0),               # 明细全 0、只有合计（O 那种）→ 不算矛盾
        variant(CASH, "DIV", commonDividendsPaid=-5.0),               # 明细不全为 0 且对不上 → warning
    ])
    assert "OK" not in issues and "DIVZ" not in issues
    assert "CF005" in issues["END"] and issues["CAPEX"] == {"CF003"} and issues["DIV"] == {"CF010"}
    assert set(result.quarantine_df["symbol"]) == {"END"}


# =========================
# 财务比率
# =========================
RATIO = {
    "symbol": "OK", "date": "2024-09-28", "period": "FY",
    "ebitMargin": 0.30, "ebitdaMargin": 0.34, "netProfitMargin": 0.24,
    "currentRatio": 0.87, "quickRatio": 0.83, "cashRatio": 0.17,
    "debtToAssetsRatio": 0.29, "debtToEquityRatio": 1.87, "financialLeverageRatio": 1.87 / 0.29,
    "dividendYield": 0.0044, "dividendYieldPercentage": 0.44,
    "revenuePerShare": 25.0, "netIncomePerShare": 6.0,
    "operatingCashFlowPerShare": 7.7, "capexPerShare": 0.6, "freeCashFlowPerShare": 7.1,
    "receivablesTurnover": 5.9, "payablesTurnover": 3.0, "inventoryTurnover": 28.9, "fixedAssetTurnover": 8.6, "assetTurnover": 1.07,
}


def test_financial_ratio():
    _, issues = run("financial_ratio", [
        RATIO,
        variant(RATIO, "LIQ", quickRatio=0.95),                       # 速动比率 > 流动比率
        variant(RATIO, "NPM", netProfitMargin=0.40),                  # 净利率 ≠ 每股净利润 / 每股收入
        variant(RATIO, "TURN", inventoryTurnover=-1.0),               # 周转率为负
    ])
    assert "OK" not in issues
    assert issues["LIQ"] == {"FR002"} and issues["NPM"] == {"FR004"} and issues["TURN"] == {"FR001"}


# =========================
# 市值 / 企业价值
# =========================
def test_market_cap_and_ev():
    _, issues = run("market_cap", [{"symbol": "OK", "date": "2025-10-24", "marketCap": 3.9e12},
                                    {"symbol": "ZERO", "date": "2025-10-24", "marketCap": 0}])
    assert issues == {"ZERO": {"MC001"}}

    ev = {"symbol": "OK", "date": "2025-09-27", "stockPrice": 255.46, "numberOfShares": 14948500000,
          "marketCapitalization": 3818743810000, "minusCashAndCashEquivalents": 35934000000,
          "addTotalDebt": 112377000000, "enterpriseValue": 3895186810000}
    _, issues = run("enterprise_values", [ev, variant(ev, "EV", enterpriseValue=3.0e12),
                                          variant(ev, "MC", marketCapitalization=3.0e12, enterpriseValue=3.0e12 + 76443000000)])
    assert "OK" not in issues and issues["EV"] == {"EV004"} and issues["MC"] == {"EV003"}


# =========================
# 公司概况
# =========================
PROFILE = {"symbol": "OK", "price": 262.82, "marketCap": 3.9e12, "range": "169.21-265.29", "change": 3.24,
           "changePercentage": 1.24817, "volume": 36725325, "averageVolume": 47424558, "lastDividend": 1.04,
           "cik": "0000320193", "isin": "US0378331005", "cusip": "037833100", "ipoDate": "1980-12-12"}


def test_profile():
    result, issues = run("profile", [
        PROFILE,
        variant(PROFILE, "RANGE", price=300.0, change=40.42, changePercentage=15.574),   # 超出 52 周区间
        variant(PROFILE, "CHG", changePercentage=5.0),                                    # 涨跌幅对不上
        variant(PROFILE, "CIK", cik="320193"),                                            # CIK 没补零
        variant(PROFILE, "PX", price=0),                                                  # 价格 0 → danger
    ])
    assert "OK" not in issues
    assert issues["RANGE"] == {"CP004"} and issues["CHG"] == {"CP005"} and issues["CIK"] == {"CP002"} and "CP001" in issues["PX"]
    assert result.clean_df["date"].notna().all()                     # 快照补上了拉取当天


# =========================
# 高管薪酬 / 一致预期 / 分析师评级
# =========================
def test_executive_compensation():
    row = {"symbol": "OK", "filingDate": "2026-01-08", "nameAndPosition": "CEO", "year": 2025, "salary": 3e6, "bonus": 0,
           "stockAward": 5e7, "optionAward": 0, "incentivePlanCompensation": 1.2e7, "allOtherCompensation": 1.7e6,
           "total": 6.67e7}
    result, issues = run("executive_compensation", [
        row,
        variant(row, "PENSION", total=7.0e7),                         # 总额 > 各项之和：少了养老金一列，正常
        variant(row, "LOW", total=5.0e7),                             # 总额 < 各项之和 → warning
        variant(row, "YEAR", year=2027),                              # 年份晚于提交年份 → warning
        row,                                                          # 重复行：去重
    ])
    assert set(issues) == {"LOW", "YEAR"} and issues["LOW"] == {"EC004"} and issues["YEAR"] == {"EC003"}
    assert result.stats["duplicates_dropped"] == 1


def test_financial_estimate():
    row = {"symbol": "OK", "date": "2029-09-28", "numAnalystsRevenue": 16, "numAnalystsEps": 6}
    for m, (lo, avg, hi) in {"revenue": (480, 483, 490), "ebitda": (150, 156, 160), "ebit": (135, 140, 145),
                             "netIncome": (139, 149, 157), "sgaExpense": (31, 32, 33), "eps": (9.05, 9.68, 10.2)}.items():
        row |= {f"{m}Low": lo, f"{m}Avg": avg, f"{m}High": hi}
    result, issues = run("financial_estimate", [
        row,
        variant(row, "ORDER", epsLow=11.0),                           # 低 > 平均 → danger
        variant(row, "SIGN", epsLow=-1.2, epsAvg=-1.0, epsHigh=-0.8), # EPS 与净利润符号相反 → warning
    ])
    assert "OK" not in issues and issues["ORDER"] == {"FE002"} and issues["SIGN"] == {"FE004"}
    assert set(result.quarantine_df["symbol"]) == {"ORDER"}


def test_analyst_grades():
    row = {"symbol": "OK", "date": "2026-05-01", "analystRatingsStrongBuy": 7, "analystRatingsBuy": 25,
           "analystRatingsHold": 14, "analystRatingsSell": 1, "analystRatingsStrongSell": 1}
    zero = {k: (0 if k.startswith("analyst") else v) for k, v in row.items()}
    _, issues = run("analyst_grades", [row, {**zero, "symbol": "ZERO"}, variant(row, "NEG", analystRatingsSell=-1)])
    assert issues == {"ZERO": {"AG002"}, "NEG": {"AG001"}}


@pytest.mark.parametrize("endpoint", ["income_statement", "balance_sheet", "cash_flow", "financial_ratio",
                                      "enterprise_values", "market_cap", "profile", "company_profile",
                                      "executive_compensation", "financial_estimate", "analyst_grades"])
def test_rule_definitions_valid(endpoint):
    """规则编号不重复、severity 合法、检查方法存在（同步到 meta.quality_rules 前的基本检查）"""
    cls = get_cleaner(endpoint)
    ids = [r.rule_id for r in cls.rules]
    assert len(ids) == len(set(ids))
    assert all(r.severity in ("warning", "danger") and hasattr(cls, r.check) for r in cls.rules)
