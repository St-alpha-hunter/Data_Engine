import pandas as pd

from clean.base_clean import CleanBasic, Rule

"""

高管薪酬（FMP governance-executive-compensation）：一行 = 一份委托书里一位高管一年的薪酬

注意：
    同一个人同一年会出现在连续几年的委托书里（数字可能略有修订），按 (代码, 年份, 人, 提交日) 去重，不算重复
    没有 date 字段：_prepare 里用提交日（filingDate）作为 date，用于记录问题
        问题记录按 (批次, 规则, 代码, 日期) 去重，同一份委托书里多位高管违反同一条规则只记一条
    FMP 少了 SEC 薪酬表里"养老金变动"这一列，所以总额 > 各项之和是正常的；总额 < 各项之和才是矛盾

"""

COMPONENTS = ["salary", "bonus", "stockAward", "optionAward", "incentivePlanCompensation", "allOtherCompensation"]


class ExecutiveCompensation(CleanBasic):

    endpoint_name = "executive_compensation"

    config = {
        "required_columns": ["symbol", "filingDate", "nameAndPosition", "year", "total"] + COMPONENTS,
        "numeric_columns": ["year", "total"] + COMPONENTS,
        "date_columns": ["filingDate", "acceptedDate"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "year", "nameAndPosition", "filingDate"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("EC001", 1, "danger", "关键字段缺失：年份、总额、提交日为空",
             check="_check_missing", params={"columns": ["year", "total", "filingDate"]},
             detail_columns=("year", "total", "nameAndPosition")),
        Rule("EC002", 1, "danger", "薪酬总额或某一项为负",
             check="_check_negative", params={"columns": ["total"] + COMPONENTS},
             detail_columns=("nameAndPosition", "total", *COMPONENTS)),
        Rule("EC003", 1, "warning", "薪酬年份晚于委托书提交年份",
             check="_check_year", detail_columns=("nameAndPosition", "year", "filingDate")),
        # ---- 同一行不冲突 ----
        Rule("EC004", 1, "warning", "薪酬总额 < 各项之和（误差 > 1%；FMP 少了养老金变动一列，所以只查小于）",
             check="_check_total", detail_columns=("nameAndPosition", "year", "total", *COMPONENTS)),
    ]

    def _prepare(self, df):
        df["date"] = df["filingDate"]
        return df

    def _check_year(self, df):
        return df["year"] > df["filingDate"].dt.year

    def _check_total(self, df):
        parts = df[COMPONENTS].sum(axis=1, min_count=1)
        return df["total"] < parts - (0.01 * parts.abs()).clip(lower=1.0)


"""
FMP 返回样例（AAPL，节选）：
    filingDate "2026-01-08", nameAndPosition "Tim Cook Chief Executive Officer", year 2025,
    salary 3000000, bonus 0, stockAward 57535293, optionAward 0, incentivePlanCompensation 12000000,
    allOtherCompensation 1759518, total 74294811
"""
