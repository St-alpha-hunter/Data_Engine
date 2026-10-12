import pandas as pd

from clean.base_clean import CleanBasic, Rule, not_close

"""

公司概况（FMP profile，按代码查；profile-cik 按 CIK 查，字段相同）

没有日期字段，是拉取当天的快照：_prepare 里补上 date = 拉取当天，用于记录问题
    合规性：价格缺失或 <= 0；CIK / ISIN / CUSIP 格式不对；市值、成交量、股息为负
    同一行不冲突：价格在 52 周区间（range = "低-高"）内；涨跌额与涨跌幅对得上

"""

NUMERIC = ["price", "marketCap", "beta", "lastDividend", "change", "changePercentage", "volume", "averageVolume"]


class CompanyProfile(CleanBasic):

    endpoint_name = "profile"

    config = {
        "required_columns": ["symbol", "price", "marketCap", "range", "change", "changePercentage",
                             "volume", "averageVolume", "lastDividend", "cik", "isin", "cusip"],
        "numeric_columns": NUMERIC,
        "date_columns": ["ipoDate"],
        "drop_columns": [],
        "duplicate_columns": ["symbol"],
        "output_columns": [],
    }

    rules = [
        # ---- 合规性 ----
        Rule("CP001", 1, "danger", "价格缺失或 <= 0",
             check="_check_price", detail_columns=("price",)),
        Rule("CP002", 1, "warning", "代码格式不对：CIK 不是 10 位数字、ISIN 不是 12 位、CUSIP 不是 9 位（为空不算）",
             check="_check_identifier_format", detail_columns=("cik", "isin", "cusip")),
        Rule("CP003", 1, "warning", "市值、成交量、平均成交量、最近一次股息为负",
             check="_check_negative", params={"columns": ["marketCap", "volume", "averageVolume", "lastDividend"]},
             detail_columns=("marketCap", "volume", "averageVolume", "lastDividend")),
        # ---- 同一行不冲突 ----
        Rule("CP004", 1, "warning", "价格不在 52 周区间内（容差 2%）",
             check="_check_price_in_range", detail_columns=("price", "range_low", "range_high")),
        Rule("CP005", 1, "warning", "涨跌额与涨跌幅对不上：涨跌幅 ≠ 涨跌额 / 昨收 × 100（误差 > 2%，且差额 > 0.05 个百分点）",
             check="_check_change", detail_columns=("price", "change", "changePercentage")),
    ]

    def _prepare(self, df):
        if "date" not in df.columns:
            df["date"] = pd.Timestamp.today().normalize()         # 快照：记为拉取当天
        bounds = df["range"].astype(str).str.extract(r"^\s*([0-9.]+)\s*-\s*([0-9.]+)\s*$")
        df["range_low"] = pd.to_numeric(bounds[0], errors="coerce")
        df["range_high"] = pd.to_numeric(bounds[1], errors="coerce")
        return df

    def _check_price(self, df):
        return self._check_missing(df, ["price"]) | self._check_non_positive(df, ["price"])

    def _check_identifier_format(self, df):
        def bad(col, pattern):
            s = df[col].astype("string").str.strip()
            return s.notna() & (s != "") & ~s.str.fullmatch(pattern).fillna(False)
        return bad("cik", r"\d{10}") | bad("isin", r"[A-Z]{2}[A-Z0-9]{9}\d") | bad("cusip", r"[A-Z0-9]{9}")

    def _check_price_in_range(self, df):
        return (df["price"] < df["range_low"] * 0.98) | (df["price"] > df["range_high"] * 1.02)

    def _check_change(self, df):
        prev = df["price"] - df["change"]
        implied = (df["change"] / prev * 100).where(prev > 0)
        return not_close(df["changePercentage"], implied, rel=0.02, atol=0.05)


class CompanyProfileByCik(CompanyProfile):
    """按 CIK 查的 profile-cik，字段和规则与 profile 相同"""
    endpoint_name = "company_profile"

    @classmethod
    def rule_definitions(cls) -> list[dict]:
        # 规则和 profile 共用同一组编号，同步时按 profile 登记，避免同一编号登记成两个接口
        return CompanyProfile.rule_definitions()


"""
FMP 返回样例（AAPL，节选）：
    price 262.82, marketCap 3900351299800, beta 1.109, lastDividend 1.04, range "169.21-265.29",
    change 3.24, changePercentage 1.24817, volume 36725325, averageVolume 47424558,
    cik "0000320193", isin "US0378331005", cusip "037833100", ipoDate "1980-12-12",
    isEtf false, isActivelyTrading true, isAdr false, isFund false
"""
