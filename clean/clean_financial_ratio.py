from clean.base_clean import CleanBasic, Rule, not_close

"""

财务比率（FMP ratios）

比率都是 FMP 用报表算出来的，所以这里主要查"同一行里的比率互相矛盾"：
    同一个量的两种算法对不上（净利率 vs 每股净利润 / 每股收入、财务杠杆 vs 负债权益比 / 负债资产比）
    有包含关系的比率顺序颠倒（现金比率 <= 速动比率 <= 流动比率；EBITDA 利润率 >= EBIT 利润率）
合规性：不能为负的比率为负（周转率、流动性比率，分子分母都不为负）

不查上下限：银行的毛利率可以 > 100%（实测 JPM 2021 年 1.03），亏损公司的有效税率、分红率可以为负

"""

NON_NEGATIVE = ["receivablesTurnover", "payablesTurnover", "inventoryTurnover", "fixedAssetTurnover",
                "assetTurnover", "currentRatio", "quickRatio", "cashRatio"]


class FinancialRatio(CleanBasic):

    endpoint_name = "financial_ratio"

    config = {
        "required_columns": [
            "symbol", "date", "period",
            "ebitMargin", "ebitdaMargin", "netProfitMargin",
            "currentRatio", "quickRatio", "cashRatio",
            "debtToAssetsRatio", "debtToEquityRatio", "financialLeverageRatio",
            "dividendYield", "dividendYieldPercentage",
            "revenuePerShare", "netIncomePerShare",
            "operatingCashFlowPerShare", "capexPerShare", "freeCashFlowPerShare",
        ] + NON_NEGATIVE[:5],
        "numeric_columns": [
            "grossProfitMargin", "ebitMargin", "ebitdaMargin", "operatingProfitMargin", "pretaxProfitMargin",
            "continuousOperationsProfitMargin", "netProfitMargin", "bottomLineProfitMargin",
            "receivablesTurnover", "payablesTurnover", "inventoryTurnover", "fixedAssetTurnover", "assetTurnover",
            "currentRatio", "quickRatio", "solvencyRatio", "cashRatio",
            "priceToEarningsRatio", "priceToEarningsGrowthRatio", "forwardPriceToEarningsGrowthRatio",
            "priceToBookRatio", "priceToSalesRatio", "priceToFreeCashFlowRatio", "priceToOperatingCashFlowRatio",
            "debtToAssetsRatio", "debtToEquityRatio", "debtToCapitalRatio", "longTermDebtToCapitalRatio",
            "financialLeverageRatio", "workingCapitalTurnoverRatio", "operatingCashFlowRatio",
            "operatingCashFlowSalesRatio", "freeCashFlowOperatingCashFlowRatio", "debtServiceCoverageRatio",
            "interestCoverageRatio", "shortTermOperatingCashFlowCoverageRatio", "operatingCashFlowCoverageRatio",
            "capitalExpenditureCoverageRatio", "dividendPaidAndCapexCoverageRatio", "dividendPayoutRatio",
            "dividendYield", "dividendYieldPercentage", "revenuePerShare", "netIncomePerShare",
            "interestDebtPerShare", "cashPerShare", "bookValuePerShare", "tangibleBookValuePerShare",
            "shareholdersEquityPerShare", "operatingCashFlowPerShare", "capexPerShare", "freeCashFlowPerShare",
            "netIncomePerEBT", "ebtPerEbit", "priceToFairValue", "debtToMarketCap", "effectiveTaxRate",
            "enterpriseValueMultiple",
        ],
        "date_columns": ["date"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date", "period"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("FR001", 1, "warning", "不能为负的比率为负：各项周转率、流动比率、速动比率、现金比率",
             check="_check_negative", params={"columns": NON_NEGATIVE}, detail_columns=tuple(NON_NEGATIVE)),
        # ---- 比率之间不矛盾 ----
        Rule("FR002", 1, "warning", "流动性比率顺序颠倒：应满足 现金比率 <= 速动比率 <= 流动比率（容差 0.01）",
             check="_check_liquidity_order", detail_columns=("cashRatio", "quickRatio", "currentRatio")),
        Rule("FR003", 1, "warning", "EBITDA 利润率 < EBIT 利润率（折旧摊销不能为负；容差 0.001）",
             check="_check_ebitda_margin", detail_columns=("ebitdaMargin", "ebitMargin")),
        Rule("FR004", 1, "warning", "净利率 ≠ 每股净利润 / 每股收入（误差 > 2%）",
             check="_check_ratio", params={"value": "netProfitMargin", "num": "netIncomePerShare", "den": "revenuePerShare"},
             detail_columns=("netProfitMargin", "netIncomePerShare", "revenuePerShare")),
        Rule("FR005", 1, "warning", "财务杠杆（资产 / 权益）≠ 负债权益比 / 负债资产比（误差 > 2%）",
             check="_check_ratio", params={"value": "financialLeverageRatio", "num": "debtToEquityRatio", "den": "debtToAssetsRatio"},
             detail_columns=("financialLeverageRatio", "debtToEquityRatio", "debtToAssetsRatio")),
        Rule("FR006", 1, "warning", "每股自由现金流 ≠ 每股经营现金流 - 每股资本开支（误差 > 2%）",
             check="_check_identity",
             params={"total": "freeCashFlowPerShare", "parts": ["operatingCashFlowPerShare", "-capexPerShare"], "rel": 0.02, "atol": 0.01},
             detail_columns=("freeCashFlowPerShare", "operatingCashFlowPerShare", "capexPerShare")),
        Rule("FR007", 1, "warning", "股息率百分比 ≠ 100 × 股息率（误差 > 1%）",
             check="_check_dividend_yield", detail_columns=("dividendYield", "dividendYieldPercentage")),
    ]

    def _check_liquidity_order(self, df):
        return (df["cashRatio"] > df["quickRatio"] + 0.01) | (df["quickRatio"] > df["currentRatio"] + 0.01)

    def _check_ebitda_margin(self, df):
        return df["ebitdaMargin"] < df["ebitMargin"] - 0.001

    def _check_ratio(self, df, value, num, den):
        # 分母为 0 时不检查（比率本身没有意义）
        return (df[den] != 0) & not_close(df[value], df[num] / df[den], rel=0.02, atol=0.001)

    def _check_dividend_yield(self, df):
        return not_close(df["dividendYieldPercentage"], 100 * df["dividendYield"], rel=0.01, atol=1e-4)


"""
FMP 返回样例（AAPL FY2024，节选）：
    ebitMargin 0.3151, ebitdaMargin 0.3444, netProfitMargin 0.2397,
    currentRatio 0.8673, quickRatio 0.8260, cashRatio 0.1698,
    debtToAssetsRatio 0.2922, debtToEquityRatio 1.8723, financialLeverageRatio 6.4088,
    dividendYield 0.004359, dividendYieldPercentage 0.4359,
    revenuePerShare 25.4849, netIncomePerShare 6.1091,
    operatingCashFlowPerShare 7.7070, capexPerShare 0.6157, freeCashFlowPerShare 7.0913
"""
