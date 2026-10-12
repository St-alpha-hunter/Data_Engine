from clean.base_clean import CleanBasic, Rule

"""

资产负债表（FMP balance-sheet-statement）

检查两类：
    合规性：关键字段缺失、总资产 <= 0、不能为负的科目为负
    同一张表内部不冲突：资产 = 负债 + 权益，以及各合计项与组成项对得上

实测（30 只股票 × 5 年，含银行、保险、REIT）：下面的恒等式全部对得上，
    所以"资产 ≠ 负债 + 权益"直接隔离（danger），明细合计对不上报 warning

"""

KEY = ["totalAssets", "totalLiabilities", "totalEquity", "totalLiabilitiesAndTotalEquity"]
NON_NEGATIVE = ["cashAndCashEquivalents", "shortTermInvestments", "inventory", "goodwill",
                "intangibleAssets", "propertyPlantEquipmentNet", "totalDebt", "totalLiabilities"]


class BalanceSheetYear(CleanBasic):

    endpoint_name = "balance_sheet"

    config = {
        "required_columns": [
            "date", "symbol", "period",
            "cashAndCashEquivalents", "shortTermInvestments", "cashAndShortTermInvestments",
            "inventory", "goodwill", "intangibleAssets", "propertyPlantEquipmentNet",
            "totalCurrentAssets", "totalNonCurrentAssets", "otherAssets", "totalAssets",
            "totalCurrentLiabilities", "totalNonCurrentLiabilities", "otherLiabilities", "totalLiabilities",
            "totalStockholdersEquity", "minorityInterest", "totalEquity", "totalLiabilitiesAndTotalEquity",
            "totalDebt", "netDebt",
        ],
        "numeric_columns": [
            "cashAndCashEquivalents", "shortTermInvestments", "cashAndShortTermInvestments",
            "netReceivables", "accountsReceivables", "otherReceivables", "inventory", "prepaids",
            "otherCurrentAssets", "totalCurrentAssets", "propertyPlantEquipmentNet", "goodwill",
            "intangibleAssets", "goodwillAndIntangibleAssets", "longTermInvestments", "taxAssets",
            "otherNonCurrentAssets", "totalNonCurrentAssets", "otherAssets", "totalAssets",
            "totalPayables", "accountPayables", "otherPayables", "accruedExpenses", "shortTermDebt",
            "capitalLeaseObligationsCurrent", "taxPayables", "deferredRevenue", "otherCurrentLiabilities",
            "totalCurrentLiabilities", "longTermDebt", "deferredRevenueNonCurrent",
            "deferredTaxLiabilitiesNonCurrent", "otherNonCurrentLiabilities", "totalNonCurrentLiabilities",
            "otherLiabilities", "capitalLeaseObligations", "totalLiabilities", "treasuryStock",
            "preferredStock", "commonStock", "retainedEarnings", "additionalPaidInCapital",
            "accumulatedOtherComprehensiveIncomeLoss", "otherTotalStockholdersEquity",
            "totalStockholdersEquity", "totalEquity", "minorityInterest", "totalLiabilitiesAndTotalEquity",
            "totalInvestments", "totalDebt", "netDebt",
        ],
        "date_columns": ["date", "filingDate", "acceptedDate"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date", "period"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("BS001", 1, "danger", "关键字段缺失：总资产 / 总负债 / 总权益 / 负债和权益合计为空",
             check="_check_missing", params={"columns": KEY}, detail_columns=tuple(KEY)),
        Rule("BS002", 1, "danger", "总资产 <= 0",
             check="_check_non_positive", params={"columns": ["totalAssets"]}, detail_columns=("totalAssets",)),
        Rule("BS003", 1, "warning", "不能为负的科目为负：现金、短期投资、存货、商誉、无形资产、固定资产、总债务、总负债",
             check="_check_negative", params={"columns": NON_NEGATIVE}, detail_columns=tuple(NON_NEGATIVE)),
        # ---- 表内勾稽 ----
        Rule("BS004", 1, "danger", "总资产 ≠ 负债和权益合计（误差 > 0.1%）",
             check="_check_identity", params={"total": "totalAssets", "parts": ["totalLiabilitiesAndTotalEquity"], "rel": 0.001},
             detail_columns=("totalAssets", "totalLiabilitiesAndTotalEquity")),
        Rule("BS005", 1, "danger", "负债和权益合计 ≠ 总负债 + 总权益（误差 > 0.1%）",
             check="_check_identity", params={"total": "totalLiabilitiesAndTotalEquity", "parts": ["totalLiabilities", "totalEquity"], "rel": 0.001},
             detail_columns=("totalLiabilitiesAndTotalEquity", "totalLiabilities", "totalEquity")),
        Rule("BS006", 1, "warning", "总资产 ≠ 流动资产 + 非流动资产 + 其他资产（误差 > 1%）",
             check="_check_identity", params={"total": "totalAssets", "parts": ["totalCurrentAssets", "totalNonCurrentAssets", "otherAssets"]},
             detail_columns=("totalAssets", "totalCurrentAssets", "totalNonCurrentAssets", "otherAssets")),
        Rule("BS007", 1, "warning", "总负债 ≠ 流动负债 + 非流动负债 + 其他负债（误差 > 1%）",
             check="_check_identity", params={"total": "totalLiabilities", "parts": ["totalCurrentLiabilities", "totalNonCurrentLiabilities", "otherLiabilities"]},
             detail_columns=("totalLiabilities", "totalCurrentLiabilities", "totalNonCurrentLiabilities", "otherLiabilities")),
        Rule("BS008", 1, "warning", "总权益 ≠ 归属股东权益 + 少数股东权益（误差 > 1%）",
             check="_check_identity", params={"total": "totalEquity", "parts": ["totalStockholdersEquity", "minorityInterest"]},
             detail_columns=("totalEquity", "totalStockholdersEquity", "minorityInterest")),
        Rule("BS009", 1, "warning", "净债务 ≠ 总债务 - 现金（误差 > 1%）",
             check="_check_identity", params={"total": "netDebt", "parts": ["totalDebt", "-cashAndCashEquivalents"]},
             detail_columns=("netDebt", "totalDebt", "cashAndCashEquivalents")),
        Rule("BS010", 1, "warning", "现金和短期投资 ≠ 现金 + 短期投资（误差 > 1%）",
             check="_check_identity", params={"total": "cashAndShortTermInvestments", "parts": ["cashAndCashEquivalents", "shortTermInvestments"]},
             detail_columns=("cashAndShortTermInvestments", "cashAndCashEquivalents", "shortTermInvestments")),
    ]


"""
FMP 返回样例（AAPL FY2024，节选）：
    cashAndCashEquivalents 29943000000, shortTermInvestments 35228000000, cashAndShortTermInvestments 65171000000,
    totalCurrentAssets 152987000000, totalNonCurrentAssets 211993000000, otherAssets 0, totalAssets 364980000000,
    totalCurrentLiabilities 176392000000, totalNonCurrentLiabilities 131638000000, otherLiabilities 0,
    totalLiabilities 308030000000, totalStockholdersEquity 56950000000, minorityInterest 0, totalEquity 56950000000,
    totalLiabilitiesAndTotalEquity 364980000000, totalDebt 106629000000, netDebt 76686000000
"""
