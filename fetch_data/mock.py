import pandas as pd
import numpy as np


# =========================
# 1. 输入数据字段要求
# =========================
# df 至少需要这些列：
# symbol
# sector
# market_cap
# adv              # 日均成交额，美元
# trading_days     # 近2年实际有交易数据的天数
# volatility       # 年化波动率
# financial_type   # normal / bank / energy / pharma / reit / mega_global 等


def filter_liquidity(df):
    """
    Step 1：流动性初筛
    条件：
    1. ADV > 5000万美元
    2. 近2年交易日覆盖率 > 98%
    """
    return df[
        (df["adv"] > 50_000_000) &
        (df["trading_days"] >= 252 * 2 * 0.98)
    ].copy()


def add_size_bucket(df):
    """
    Step 2：市值分桶
    """
    conditions = [
        df["market_cap"] >= 200_000_000_000,
        (df["market_cap"] >= 20_000_000_000) & (df["market_cap"] < 200_000_000_000),
        (df["market_cap"] >= 10_000_000_000) & (df["market_cap"] < 20_000_000_000),
    ]

    choices = ["mega", "large", "mid_large"]

    df["size_bucket"] = np.select(conditions, choices, default="small")
    return df


def add_volatility_bucket(df):
    """
    Step 3：波动率分桶
    按全市场分位数切成 low / mid / high
    """
    df["vol_bucket"] = pd.qcut(
        df["volatility"],
        q=3,
        labels=["low", "mid", "high"]
    )
    return df


def add_score(df):
    """
    Step 4：打分
    分数越高，越优先进入股票池
    """

    # 市值排名：市值越大，rank 越小
    df["market_cap_rank"] = df["market_cap"].rank(ascending=False)

    # 流动性分数：ADV 越大越好
    df["liquidity_score"] = np.log(df["adv"])

    # 行业覆盖 bonus
    df["sector_balance_bonus"] = 1

    # 波动分桶 bonus
    df["volatility_bucket_bonus"] = df["vol_bucket"].map({
        "low": 1.0,
        "mid": 1.2,
        "high": 1.1
    }).astype(float)

    # 财报复杂度 bonus
    df["financial_complexity_bonus"] = df["financial_type"].map({
        "normal": 1.0,
        "bank": 1.3,
        "energy": 1.2,
        "pharma": 1.2,
        "reit": 1.15,
        "mega_global": 1.25
    }).fillna(1.0)

    # 综合得分
    df["score"] = (
        -0.4 * np.log(df["market_cap_rank"])
        + 0.3 * df["liquidity_score"]
        + 0.1 * df["sector_balance_bonus"]
        + 0.1 * df["volatility_bucket_bonus"]
        + 0.1 * df["financial_complexity_bonus"]
    )

    return df


def select_top_by_sector(df, total_n=100):
    """
    Step 5：按行业分层抽样
    每个 sector 都要有样本，避免全是科技股、金融股
    """

    selected = []

    sector_counts = df["sector"].value_counts(normalize=True)

    for sector, weight in sector_counts.items():
        sector_df = df[df["sector"] == sector].copy()

        # 每个行业至少 3 只
        n = max(3, round(total_n * weight))

        top_sector = sector_df.sort_values("score", ascending=False).head(n)
        selected.append(top_sector)

    result = pd.concat(selected)

    # 如果超过100，只保留分数最高的100
    result = result.sort_values("score", ascending=False).head(total_n)

    return result


def build_sp500_stock_pool(df):
    """
    主流程
    """
    df = filter_liquidity(df)
    df = add_size_bucket(df)
    df = add_volatility_bucket(df)
    df = add_score(df)

    pool = select_top_by_sector(df, total_n=100)

    return pool.sort_values(["sector", "score"], ascending=[True, False])

