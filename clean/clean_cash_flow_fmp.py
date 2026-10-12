from clean.base_clean import CleanBasic, Rule

"""

现金流量表（FMP cash-flow-statement）

检查两类：
    合规性：关键字段缺失、期末现金为负、只能是流出的科目为正（资本开支、支付分红、回购）
    同一张表内部不冲突：现金变动 = 三大活动 + 汇率影响，期末 = 期初 + 变动，各活动合计与组成项对得上

实测（30 只股票 × 5 年）：主干恒等式全部对得上 → 现金变动、期末现金对不上直接隔离（danger）
    分红 / 发债 / 股票发行的明细：有的公司明细全是 0、只有合计（如 O），这不算冲突，只在明细不全为 0 时核对

"""

KEY = ["netCashProvidedByOperatingActivities", "netCashProvidedByInvestingActivities",
       "netCashProvidedByFinancingActivities", "netChangeInCash", "cashAtEndOfPeriod"]
OUTFLOWS = ["capitalExpenditure", "investmentsInPropertyPlantAndEquipment", "commonStockRepurchased",
            "commonDividendsPaid", "preferredDividendsPaid", "netDividendsPaid"]


class CashFlowYear(CleanBasic):

    endpoint_name = "cash_flow"

    config = {
        "required_columns": [
            "date", "symbol", "period",
            "netIncome", "depreciationAndAmortization", "deferredIncomeTax", "stockBasedCompensation",
            "changeInWorkingCapital", "otherNonCashItems", "netCashProvidedByOperatingActivities",
            "investmentsInPropertyPlantAndEquipment", "acquisitionsNet", "purchasesOfInvestments",
            "salesMaturitiesOfInvestments", "otherInvestingActivities", "netCashProvidedByInvestingActivities",
            "netDebtIssuance", "longTermNetDebtIssuance", "shortTermNetDebtIssuance",
            "netStockIssuance", "netCommonStockIssuance", "netPreferredStockIssuance", "commonStockRepurchased",
            "netDividendsPaid", "commonDividendsPaid", "preferredDividendsPaid",
            "otherFinancingActivities", "netCashProvidedByFinancingActivities",
            "effectOfForexChangesOnCash", "netChangeInCash", "cashAtEndOfPeriod", "cashAtBeginningOfPeriod",
            "operatingCashFlow", "capitalExpenditure", "freeCashFlow",
        ],
        "numeric_columns": [
            "netIncome", "depreciationAndAmortization", "deferredIncomeTax", "stockBasedCompensation",
            "changeInWorkingCapital", "accountsReceivables", "inventory", "accountsPayables",
            "otherWorkingCapital", "otherNonCashItems", "netCashProvidedByOperatingActivities",
            "investmentsInPropertyPlantAndEquipment", "acquisitionsNet", "purchasesOfInvestments",
            "salesMaturitiesOfInvestments", "otherInvestingActivities", "netCashProvidedByInvestingActivities",
            "netDebtIssuance", "longTermNetDebtIssuance", "shortTermNetDebtIssuance", "netStockIssuance",
            "netCommonStockIssuance", "commonStockIssuance", "commonStockRepurchased",
            "netPreferredStockIssuance", "netDividendsPaid", "commonDividendsPaid", "preferredDividendsPaid",
            "otherFinancingActivities", "netCashProvidedByFinancingActivities", "effectOfForexChangesOnCash",
            "netChangeInCash", "cashAtEndOfPeriod", "cashAtBeginningOfPeriod", "operatingCashFlow",
            "capitalExpenditure", "freeCashFlow", "incomeTaxesPaid", "interestPaid",
        ],
        "date_columns": ["date", "filingDate", "acceptedDate"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date", "period"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("CF001", 1, "danger", "关键字段缺失：三大活动现金流 / 现金净变动 / 期末现金为空",
             check="_check_missing", params={"columns": KEY}, detail_columns=tuple(KEY)),
        Rule("CF002", 1, "warning", "期末 / 期初现金为负",
             check="_check_negative", params={"columns": ["cashAtEndOfPeriod", "cashAtBeginningOfPeriod"]},
             detail_columns=("cashAtEndOfPeriod", "cashAtBeginningOfPeriod")),
        Rule("CF003", 1, "warning", "只能是流出的科目为正：资本开支、购建固定资产、回购、支付分红",
             check="_check_positive", params={"columns": OUTFLOWS}, detail_columns=tuple(OUTFLOWS)),
        # ---- 表内勾稽 ----
        Rule("CF004", 1, "danger", "现金净变动 ≠ 经营 + 投资 + 筹资 + 汇率影响（误差 > 1%）",
             check="_check_identity",
             params={"total": "netChangeInCash", "parts": ["netCashProvidedByOperatingActivities", "netCashProvidedByInvestingActivities",
                                                           "netCashProvidedByFinancingActivities", "effectOfForexChangesOnCash"]},
             detail_columns=("netChangeInCash", "netCashProvidedByOperatingActivities", "netCashProvidedByInvestingActivities",
                             "netCashProvidedByFinancingActivities", "effectOfForexChangesOnCash")),
        Rule("CF005", 1, "danger", "期末现金 ≠ 期初现金 + 现金净变动（误差 > 1%）",
             check="_check_identity", params={"total": "cashAtEndOfPeriod", "parts": ["cashAtBeginningOfPeriod", "netChangeInCash"]},
             detail_columns=("cashAtEndOfPeriod", "cashAtBeginningOfPeriod", "netChangeInCash")),
        Rule("CF006", 1, "warning", "经营现金流 ≠ 净利润 + 折旧摊销 + 递延税 + 股权激励 + 营运资本变动 + 其他非现金项（误差 > 1%）",
             check="_check_identity",
             params={"total": "netCashProvidedByOperatingActivities",
                     "parts": ["netIncome", "depreciationAndAmortization", "deferredIncomeTax", "stockBasedCompensation",
                               "changeInWorkingCapital", "otherNonCashItems"]},
             detail_columns=("netCashProvidedByOperatingActivities", "netIncome", "depreciationAndAmortization",
                             "changeInWorkingCapital", "otherNonCashItems")),
        Rule("CF007", 1, "warning", "投资现金流 ≠ 购建固定资产 + 收购 + 购买投资 + 出售投资 + 其他（误差 > 1%）",
             check="_check_identity",
             params={"total": "netCashProvidedByInvestingActivities",
                     "parts": ["investmentsInPropertyPlantAndEquipment", "acquisitionsNet", "purchasesOfInvestments",
                               "salesMaturitiesOfInvestments", "otherInvestingActivities"]},
             detail_columns=("netCashProvidedByInvestingActivities", "investmentsInPropertyPlantAndEquipment", "acquisitionsNet",
                             "purchasesOfInvestments", "salesMaturitiesOfInvestments", "otherInvestingActivities")),
        Rule("CF008", 1, "warning", "筹资现金流 ≠ 净发债 + 净发股 + 分红 + 其他（误差 > 1%）",
             check="_check_identity",
             params={"total": "netCashProvidedByFinancingActivities",
                     "parts": ["netDebtIssuance", "netStockIssuance", "netDividendsPaid", "otherFinancingActivities"]},
             detail_columns=("netCashProvidedByFinancingActivities", "netDebtIssuance", "netStockIssuance",
                             "netDividendsPaid", "otherFinancingActivities")),
        Rule("CF009", 1, "warning", "自由现金流 ≠ 经营现金流 + 资本开支，或 operatingCashFlow ≠ 经营活动现金流净额（误差 > 1%）",
             check="_check_fcf", detail_columns=("freeCashFlow", "operatingCashFlow", "capitalExpenditure",
                                                 "netCashProvidedByOperatingActivities")),
        Rule("CF010", 1, "warning", "合计与明细矛盾（明细不全为 0 时）：净发债 ≠ 长期 + 短期；净发股 ≠ 普通股 + 优先股；分红 ≠ 普通股 + 优先股",
             check="_check_breakdowns",
             detail_columns=("netDebtIssuance", "longTermNetDebtIssuance", "shortTermNetDebtIssuance",
                             "netStockIssuance", "netCommonStockIssuance", "netPreferredStockIssuance",
                             "netDividendsPaid", "commonDividendsPaid", "preferredDividendsPaid")),
    ]

    def _check_fcf(self, df):
        return (self._check_identity(df, "freeCashFlow", ["operatingCashFlow", "capitalExpenditure"])
                | self._check_identity(df, "operatingCashFlow", ["netCashProvidedByOperatingActivities"]))

    def _check_breakdowns(self, df):
        pairs = [("netDebtIssuance", ["longTermNetDebtIssuance", "shortTermNetDebtIssuance"]),
                 ("netStockIssuance", ["netCommonStockIssuance", "netPreferredStockIssuance"]),
                 ("netDividendsPaid", ["commonDividendsPaid", "preferredDividendsPaid"])]
        bad = False
        for total, parts in pairs:
            bad = bad | self._check_identity(df, total, parts, skip_if_parts_zero=True)
        return bad


"""
FMP 返回样例（AAPL FY2024，节选）：
    netIncome 93736000000, depreciationAndAmortization 11445000000, stockBasedCompensation 11688000000,
    changeInWorkingCapital 3651000000, otherNonCashItems -2266000000, netCashProvidedByOperatingActivities 118254000000,
    netCashProvidedByInvestingActivities 2935000000, netCashProvidedByFinancingActivities -121983000000,
    effectOfForexChangesOnCash 0, netChangeInCash -794000000, cashAtBeginningOfPeriod 30737000000,
    cashAtEndOfPeriod 29943000000, capitalExpenditure -9447000000, freeCashFlow 108807000000,
    commonStockRepurchased -94949000000, commonDividendsPaid -15234000000
"""
