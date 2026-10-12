from clean.base_clean import CleanBasic, Rule

"""

市值（FMP market-capitalization）：字段只有 symbol / date / marketCap，只查合规性

"""


class MarketCap(CleanBasic):

    endpoint_name = "market_cap"

    config = {
        "required_columns": ["symbol", "date", "marketCap"],
        "numeric_columns": ["marketCap"],
        "date_columns": ["date"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date"],
        "output_columns": ["symbol", "date", "marketCap"],
    }

    rules = [
        Rule("MC001", 1, "danger", "市值缺失或 <= 0",
             check="_check_bad_cap", detail_columns=("marketCap",)),
    ]

    def _check_bad_cap(self, df):
        return self._check_missing(df, ["marketCap"]) | self._check_non_positive(df, ["marketCap"])


"""
FMP 返回样例：{"symbol": "AAPL", "date": "2025-10-24", "marketCap": 3900351299800}
"""
