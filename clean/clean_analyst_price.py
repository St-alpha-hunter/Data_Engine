from clean.base_clean import CleanBasic, Rule

"""

分析师评级历史（FMP grades-historical）：每月一行，各评级的分析师人数

    合规性：人数缺失或为负
    同一行不冲突：各评级人数全为 0（这个月没有任何评级，数据可能缺失）

"""

GRADES = ["analystRatingsStrongBuy", "analystRatingsBuy", "analystRatingsHold",
          "analystRatingsSell", "analystRatingsStrongSell"]


class AnalystGrades(CleanBasic):

    endpoint_name = "analyst_grades"

    config = {
        "required_columns": ["symbol", "date"] + GRADES,
        "numeric_columns": GRADES,
        "date_columns": ["date"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date"],
        "output_columns": ["symbol", "date"] + GRADES,
    }

    rules = [
        Rule("AG001", 1, "danger", "评级人数缺失或为负",
             check="_check_bad_counts", detail_columns=tuple(GRADES)),
        Rule("AG002", 1, "warning", "各评级人数全为 0",
             check="_check_all_zero", detail_columns=tuple(GRADES)),
    ]

    def _check_bad_counts(self, df):
        return self._check_missing(df, GRADES) | self._check_negative(df, GRADES)

    def _check_all_zero(self, df):
        return (df[GRADES] == 0).all(axis=1)


"""
FMP 返回样例：
    {"symbol": "AAPL", "date": "2026-05-01", "analystRatingsStrongBuy": 7, "analystRatingsBuy": 25,
     "analystRatingsHold": 14, "analystRatingsSell": 1, "analystRatingsStrongSell": 1}
"""
