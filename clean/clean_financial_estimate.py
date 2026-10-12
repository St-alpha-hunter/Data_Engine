from clean.base_clean import CleanBasic, Rule

"""

分析师一致预期（FMP analyst-estimates）：每个财年（date = 财年结束日，可以在未来）一行，各指标有 低 / 平均 / 高

    合规性：分析师人数为负
    同一行不冲突：低 <= 平均 <= 高；EBITDA 平均 >= EBIT 平均；EPS 与净利润同号

"""

METRICS = ["revenue", "ebitda", "ebit", "netIncome", "sgaExpense", "eps"]
ESTIMATE_COLUMNS = [f"{m}{s}" for m in METRICS for s in ("Low", "Avg", "High")]


class FinancialEstimate(CleanBasic):

    endpoint_name = "financial_estimate"

    config = {
        "required_columns": ["symbol", "date", "numAnalystsRevenue", "numAnalystsEps"] + ESTIMATE_COLUMNS,
        "numeric_columns": ESTIMATE_COLUMNS + ["numAnalystsRevenue", "numAnalystsEps"],
        "date_columns": ["date"],
        "drop_columns": [],
        "duplicate_columns": ["symbol", "date"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("FE001", 1, "danger", "分析师人数为负",
             check="_check_negative", params={"columns": ["numAnalystsRevenue", "numAnalystsEps"]},
             detail_columns=("numAnalystsRevenue", "numAnalystsEps")),
        # ---- 同一行不冲突 ----
        Rule("FE002", 1, "danger", "预测区间颠倒：某个指标不满足 低 <= 平均 <= 高",
             check="_check_low_avg_high", detail_columns=tuple(ESTIMATE_COLUMNS)),
        Rule("FE003", 1, "warning", "EBITDA 平均预测 < EBIT 平均预测（折旧摊销不能为负）",
             check="_check_ebitda_ebit", detail_columns=("ebitdaAvg", "ebitAvg")),
        Rule("FE004", 1, "warning", "EPS 平均预测与净利润平均预测符号相反",
             check="_check_eps_sign", detail_columns=("epsAvg", "netIncomeAvg")),
    ]

    def _check_low_avg_high(self, df):
        bad = False
        for m in METRICS:
            lo, avg, hi = df[f"{m}Low"], df[f"{m}Avg"], df[f"{m}High"]
            tol = 1e-6 * hi.abs().clip(lower=1)
            bad = bad | (lo > avg + tol) | (avg > hi + tol)
        return bad

    def _check_ebitda_ebit(self, df):
        return df["ebitdaAvg"] < df["ebitAvg"] - 1e-6 * df["ebitAvg"].abs().clip(lower=1)

    def _check_eps_sign(self, df):
        return (df["epsAvg"] * df["netIncomeAvg"]) < 0


"""
FMP 返回样例（AAPL 2029-09-28，节选）：
    revenueLow 483092500000, revenueAvg 483093000000, revenueHigh 483093500000,
    ebitdaAvg 155952327446, ebitAvg 140628441297, netIncomeAvg 149150359609,
    epsLow 9.05024, epsAvg 9.68, epsHigh 10.20148, numAnalystsRevenue 16, numAnalystsEps 6
"""
