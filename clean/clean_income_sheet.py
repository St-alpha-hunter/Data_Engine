from clean.base_clean import CleanBasic, Rule, not_close

"""

利润表（FMP income-statement）

检查两类：
    合规性：关键字段缺失、股数 <= 0
    同一张表内部不冲突：毛利、税前利润、持续经营净利润、EBITDA、EPS 与各自的组成项对得上

实测（30 只股票 × 5 年，含银行、保险、公用事业）：
    毛利 / 税前 / 持续经营净利润 / EBITDA / 成本费用合计 全部对得上 → 对不上说明数据有错
    营业利润 = 毛利 - 营业费用：INTC 有重组费用等特殊项不在营业费用里，对不上 → 只报 warning
    EPS 要用归属普通股的 bottomLineNetIncome 算（银行、保险有优先股股息）；
        DUK 2023 年 EPS 5.43 是 FMP 的错误（真实 3.54），这条规则能抓到

"""

KEY = ["revenue", "netIncome", "weightedAverageShsOut"]


class IncomeSheetYear(CleanBasic):

    endpoint_name = "income_statement"

    config = {
        "required_columns": [
            "date", "symbol", "period",
            "revenue", "costOfRevenue", "grossProfit", "operatingExpenses", "costAndExpenses",
            "operatingIncome", "totalOtherIncomeExpensesNet", "incomeBeforeTax", "incomeTaxExpense",
            "netIncomeFromContinuingOperations", "netIncome", "bottomLineNetIncome",
            "ebitda", "ebit", "depreciationAndAmortization",
            "eps", "epsDiluted", "weightedAverageShsOut", "weightedAverageShsOutDil",
        ],
        "numeric_columns": [
            "revenue", "costOfRevenue", "grossProfit",
            "researchAndDevelopmentExpenses", "generalAndAdministrativeExpenses",
            "sellingAndMarketingExpenses", "sellingGeneralAndAdministrativeExpenses",
            "otherExpenses", "operatingExpenses", "costAndExpenses",
            "netInterestIncome", "interestIncome", "interestExpense",
            "depreciationAndAmortization", "ebitda", "ebit",
            "nonOperatingIncomeExcludingInterest", "operatingIncome",
            "totalOtherIncomeExpensesNet", "incomeBeforeTax", "incomeTaxExpense",
            "netIncomeFromContinuingOperations", "netIncomeFromDiscontinuedOperations",
            "otherAdjustmentsToNetIncome", "netIncome", "netIncomeDeductions", "bottomLineNetIncome",
            "eps", "epsDiluted", "weightedAverageShsOut", "weightedAverageShsOutDil",
        ],
        "date_columns": ["date", "filingDate", "acceptedDate"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date", "period"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("IS001", 1, "danger", "关键字段缺失：营业收入 / 净利润 / 加权平均股数为空",
             check="_check_missing", params={"columns": KEY}, detail_columns=tuple(KEY)),
        Rule("IS002", 1, "danger", "加权平均股数（基本 / 稀释）<= 0",
             check="_check_non_positive", params={"columns": ["weightedAverageShsOut", "weightedAverageShsOutDil"]},
             detail_columns=("weightedAverageShsOut", "weightedAverageShsOutDil")),
        # ---- 表内勾稽 ----
        Rule("IS003", 1, "danger", "毛利 ≠ 营业收入 - 营业成本（误差 > 1%）",
             check="_check_identity", params={"total": "grossProfit", "parts": ["revenue", "-costOfRevenue"]},
             detail_columns=("grossProfit", "revenue", "costOfRevenue")),
        Rule("IS004", 1, "warning", "营业利润 ≠ 毛利 - 营业费用（误差 > 1%；重组费用等特殊项可能导致）",
             check="_check_identity", params={"total": "operatingIncome", "parts": ["grossProfit", "-operatingExpenses"]},
             detail_columns=("operatingIncome", "grossProfit", "operatingExpenses")),
        Rule("IS005", 1, "warning", "成本费用合计 ≠ 营业成本 + 营业费用（误差 > 1%）",
             check="_check_identity", params={"total": "costAndExpenses", "parts": ["costOfRevenue", "operatingExpenses"]},
             detail_columns=("costAndExpenses", "costOfRevenue", "operatingExpenses")),
        Rule("IS006", 1, "danger", "税前利润 ≠ 营业利润 + 营业外收支净额（误差 > 1%）",
             check="_check_identity", params={"total": "incomeBeforeTax", "parts": ["operatingIncome", "totalOtherIncomeExpensesNet"]},
             detail_columns=("incomeBeforeTax", "operatingIncome", "totalOtherIncomeExpensesNet")),
        Rule("IS007", 1, "danger", "持续经营净利润 ≠ 税前利润 - 所得税（误差 > 1%）",
             check="_check_identity", params={"total": "netIncomeFromContinuingOperations", "parts": ["incomeBeforeTax", "-incomeTaxExpense"]},
             detail_columns=("netIncomeFromContinuingOperations", "incomeBeforeTax", "incomeTaxExpense")),
        Rule("IS008", 1, "warning", "EBITDA ≠ EBIT + 折旧摊销（误差 > 1%）",
             check="_check_identity", params={"total": "ebitda", "parts": ["ebit", "depreciationAndAmortization"]},
             detail_columns=("ebitda", "ebit", "depreciationAndAmortization")),
        Rule("IS009", 1, "warning", "EPS ≠ 归属普通股净利润 / 加权平均股数（误差 > 5%，且差额 > 0.02）",
             check="_check_eps", params={"rel": 0.05, "atol": 0.02},
             detail_columns=("eps", "bottomLineNetIncome", "weightedAverageShsOut")),
        Rule("IS010", 1, "warning", "稀释与基本矛盾：稀释股数 < 基本股数，或盈利时稀释 EPS > 基本 EPS",
             check="_check_dilution",
             detail_columns=("eps", "epsDiluted", "weightedAverageShsOut", "weightedAverageShsOutDil")),
    ]

    def _check_eps(self, df, rel, atol):
        return not_close(df["eps"], df["bottomLineNetIncome"] / df["weightedAverageShsOut"], rel, atol)

    def _check_dilution(self, df):
        fewer_diluted = df["weightedAverageShsOutDil"] < df["weightedAverageShsOut"] * (1 - 1e-6)
        higher_diluted_eps = (df["eps"] > 0) & (df["epsDiluted"] > df["eps"] + 0.01)
        return fewer_diluted | higher_diluted_eps


"""
FMP 返回样例（AAPL FY2024，节选）：
    revenue 391035000000, costOfRevenue 210352000000, grossProfit 180683000000,
    operatingExpenses 57467000000, costAndExpenses 267819000000, operatingIncome 123216000000,
    totalOtherIncomeExpensesNet 269000000, incomeBeforeTax 123485000000, incomeTaxExpense 29749000000,
    netIncomeFromContinuingOperations 93736000000, netIncome 93736000000, bottomLineNetIncome 93736000000,
    ebitda 134661000000, ebit 123216000000, depreciationAndAmortization 11445000000,
    eps 6.11, epsDiluted 6.08, weightedAverageShsOut 15343783000, weightedAverageShsOutDil 15408095000
"""
