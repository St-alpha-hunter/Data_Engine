from clean.base_clean import CleanBasic, Rule, not_close

"""

企业价值（FMP enterprise-values）

    合规性：股价、股数缺失或 <= 0；现金、债务为负
    同一行不冲突：市值 = 股价 × 股数；企业价值 = 市值 - 现金 + 债务
    （minusCashAndCashEquivalents 存的是现金本身的正数，计算时减去）

"""

COLUMNS = ["stockPrice", "numberOfShares", "marketCapitalization", "minusCashAndCashEquivalents",
           "addTotalDebt", "enterpriseValue"]


class EnterpriseValue(CleanBasic):

    endpoint_name = "enterprise_values"

    config = {
        "required_columns": ["symbol", "date"] + COLUMNS,
        "numeric_columns": COLUMNS,
        "date_columns": ["date"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date"],
        "output_columns": ["symbol", "date"] + COLUMNS,
    }

    rules = [
        Rule("EV001", 1, "danger", "股价或股数缺失、<= 0",
             check="_check_price_shares", detail_columns=("stockPrice", "numberOfShares")),
        Rule("EV002", 1, "warning", "现金或总债务为负",
             check="_check_negative", params={"columns": ["minusCashAndCashEquivalents", "addTotalDebt"]},
             detail_columns=("minusCashAndCashEquivalents", "addTotalDebt")),
        Rule("EV003", 1, "warning", "市值 ≠ 股价 × 股数（误差 > 1%）",
             check="_check_market_cap", detail_columns=("marketCapitalization", "stockPrice", "numberOfShares")),
        Rule("EV004", 1, "danger", "企业价值 ≠ 市值 - 现金 + 总债务（误差 > 0.1%）",
             check="_check_identity",
             params={"total": "enterpriseValue", "parts": ["marketCapitalization", "-minusCashAndCashEquivalents", "addTotalDebt"],
                     "rel": 0.001},
             detail_columns=tuple(COLUMNS[2:])),
    ]

    def _check_price_shares(self, df):
        cols = ["stockPrice", "numberOfShares"]
        return self._check_missing(df, cols) | self._check_non_positive(df, cols)

    def _check_market_cap(self, df):
        return not_close(df["marketCapitalization"], df["stockPrice"] * df["numberOfShares"])


"""
FMP 返回样例（AAPL 2025-09-27）：
    stockPrice 255.46, numberOfShares 14948500000, marketCapitalization 3818743810000,
    minusCashAndCashEquivalents 35934000000, addTotalDebt 112377000000, enterpriseValue 3895186810000
"""
