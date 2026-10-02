import os
import logging
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from config.endpoints import SYMBOL_URL,API_KEY
from config.paths import STOCKS_POOL_PATH
"""
抓取SP-500中，100支比较有代表性的成分股
step1 先抓取现股票代码，以及所属行业
step2 计算去年成交量
step3 计算去年波动率
step4  财报复杂度
step5  赋分选择
得出100支成分股
"""

"""
记得补充
增加cik列
"""

load_dotenv()
log = logging.getLogger(__name__)
if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")

##抓取symbol股票池子
def fetch_and_save_symbols():
    print("Fetching symbol list from CSV...")
    df = pd.read_csv(SYMBOL_URL)
    df = df[["Symbol","GICS Sector","GICS Sub-Industry"]]
    print("拉取成功")
    df = df.rename(columns={
        "Symbol": "symbol",
        "GICS Sector":"sector",
        "GICS Sub-Industry":"industrial"
    })

    print("查看列，第一次",df.columns)

    df["symbol"] = df["symbol"].str.replace(".","-", regex=False)
    df = df[["symbol","sector","industrial"]]
    symbols = [symbol for symbol in df["symbol"].tolist()]

    # df = df.set_index("symbol", inplace=False)
    df.to_parquet(STOCKS_POOL_PATH)

    print("查看列 第二次",df.columns)


    print("拉取完成")
    return symbols

##聚类计算 avg_volume
def compute_volume_vol(df):
   #compute_df = pd.read_parquet(Save_Volume)
    stats_df = df.groupby("symbol").agg(
        avg_volume = ("adv", "mean"),
        volatility = ("return", lambda x:x.std()*np.sqrt(252))
    )
    return stats_df

###加上市值,
def merge_data(df1, df2, key1:str, key2:None):
    full_data = pd.merge(df1, df2, on=key1)
    return full_data

##打分
def rank_and_score(df):

    #行业动态调整
    df["sector_balance_bonus"] = 1
    df["market_cap_score"] = np.log(df["marketCap"]).rank(pct=True)
    df["adv_score"] = np.log(df["avg_volume"]).rank(pct=True)

    #波动率越靠近中间越趋近于1，靠近两边趋近于0
    df["vol_rank"] = df["volatility"].rank(pct=True)
    df["vol_score"] = 1 - (df["vol_rank"] - 0.5).abs() * 2

    df["total_score"] = 0.3*df["market_cap_score"] + 0.3*df["adv_score"] + 0.4*df["vol_score"]
    return df

print("运行成功")


if __name__ == "__main__":
    fetch_and_save_symbols()

