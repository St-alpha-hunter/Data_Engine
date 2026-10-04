import logging
from datetime import datetime

import pandas as pd

from config.endpoints import API_KEY
from config.paths import ANALYST_GRADES
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

"""

分析师评级历史（FMP grades-historical）：空架子，清洗逻辑待实现。文件末尾是 FMP 返回的样例数据，作为字段参考。

"""


def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index(["symbol", "date"]).sort_index()
    ANALYST_GRADES.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ANALYST_GRADES / f"{datetime.now():%Y%m%d_%H%M%S}.parquet")
    return df


# 临时入口：拉取并保存，pipelines 完成后改由 pipeline 调用
if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("FMP_API_KEY NOT SET IN")

    with FetchData(
        symbols=["AAPL", "MSFT"],
        endpoint_name="analyst_grades",
        extra_params=None
    ) as fetchData:
        df_raw, summary, succeed, failed = fetchData.fetch_fmp_batch()

    print(summary)
    print("运行成功")


"""

[
  {
    "symbol": "AAPL",
    "date": "2026-05-01",
    "analystRatingsStrongBuy": 7,
    "analystRatingsBuy": 25,
    "analystRatingsHold": 14,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2026-04-01",
    "analystRatingsStrongBuy": 7,
    "analystRatingsBuy": 25,
    "analystRatingsHold": 15,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2026-03-01",
    "analystRatingsStrongBuy": 6,
    "analystRatingsBuy": 25,
    "analystRatingsHold": 16,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2026-02-01",
    "analystRatingsStrongBuy": 6,
    "analystRatingsBuy": 25,
    "analystRatingsHold": 16,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2026-01-01",
    "analystRatingsStrongBuy": 6,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 17,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 3
  },
  {
    "symbol": "AAPL",
    "date": "2025-12-01",
    "analystRatingsStrongBuy": 5,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 15,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 3
  },
  {
    "symbol": "AAPL",
    "date": "2025-11-01",
    "analystRatingsStrongBuy": 5,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 15,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 3
  },
  {
    "symbol": "AAPL",
    "date": "2025-10-01",
    "analystRatingsStrongBuy": 6,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 15,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 3
  },
  {
    "symbol": "AAPL",
    "date": "2025-09-01",
    "analystRatingsStrongBuy": 5,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 15,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 3
  },
  {
    "symbol": "AAPL",
    "date": "2025-08-01",
    "analystRatingsStrongBuy": 5,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 15,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2025-07-01",
    "analystRatingsStrongBuy": 6,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 18,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2025-06-01",
    "analystRatingsStrongBuy": 7,
    "analystRatingsBuy": 22,
    "analystRatingsHold": 17,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2025-05-01",
    "analystRatingsStrongBuy": 7,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 16,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2025-04-01",
    "analystRatingsStrongBuy": 8,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 16,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2025-03-01",
    "analystRatingsStrongBuy": 7,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 14,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2025-02-01",
    "analystRatingsStrongBuy": 8,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 14,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2025-01-01",
    "analystRatingsStrongBuy": 8,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 13,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-12-01",
    "analystRatingsStrongBuy": 8,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-11-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-10-01",
    "analystRatingsStrongBuy": 12,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-09-01",
    "analystRatingsStrongBuy": 12,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-08-01",
    "analystRatingsStrongBuy": 12,
    "analystRatingsBuy": 25,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2024-07-01",
    "analystRatingsStrongBuy": 12,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-06-01",
    "analystRatingsStrongBuy": 12,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 12,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-05-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-04-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-03-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-02-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2024-01-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-12-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-11-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-10-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-09-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-08-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-07-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-06-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 20,
    "analystRatingsHold": 10,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-05-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-04-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-03-01",
    "analystRatingsStrongBuy": 10,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-02-01",
    "analystRatingsStrongBuy": 13,
    "analystRatingsBuy": 20,
    "analystRatingsHold": 8,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2023-01-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-12-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-11-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-10-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-09-01",
    "analystRatingsStrongBuy": 14,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 8,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-08-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-07-01",
    "analystRatingsStrongBuy": 14,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 8,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-06-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-05-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 23,
    "analystRatingsHold": 7,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-04-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-03-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-02-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2022-01-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-12-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-11-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-10-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-09-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-08-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-07-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-06-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-05-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-04-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-03-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 21,
    "analystRatingsHold": 6,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2021-02-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 19,
    "analystRatingsHold": 8,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2021-01-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 19,
    "analystRatingsHold": 8,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 1
  },
  {
    "symbol": "AAPL",
    "date": "2020-12-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 16,
    "analystRatingsHold": 9,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2020-11-01",
    "analystRatingsStrongBuy": 11,
    "analystRatingsBuy": 16,
    "analystRatingsHold": 9,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 2
  },
  {
    "symbol": "AAPL",
    "date": "2020-09-01",
    "analystRatingsStrongBuy": 13,
    "analystRatingsBuy": 24,
    "analystRatingsHold": 7,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-08-01",
    "analystRatingsStrongBuy": 14,
    "analystRatingsBuy": 20,
    "analystRatingsHold": 8,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-07-01",
    "analystRatingsStrongBuy": 14,
    "analystRatingsBuy": 20,
    "analystRatingsHold": 8,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-06-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-05-01",
    "analystRatingsStrongBuy": 14,
    "analystRatingsBuy": 20,
    "analystRatingsHold": 8,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-04-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-03-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-02-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2020-01-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-12-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-11-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-10-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-09-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-08-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-07-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 13,
    "analystRatingsHold": 19,
    "analystRatingsSell": 3,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-06-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 10,
    "analystRatingsHold": 20,
    "analystRatingsSell": 2,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-05-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 10,
    "analystRatingsHold": 22,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-04-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 10,
    "analystRatingsHold": 22,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-03-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 10,
    "analystRatingsHold": 22,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-02-01",
    "analystRatingsStrongBuy": 15,
    "analystRatingsBuy": 10,
    "analystRatingsHold": 22,
    "analystRatingsSell": 1,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2019-01-01",
    "analystRatingsStrongBuy": 17,
    "analystRatingsBuy": 14,
    "analystRatingsHold": 18,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  },
  {
    "symbol": "AAPL",
    "date": "2018-12-01",
    "analystRatingsStrongBuy": 17,
    "analystRatingsBuy": 14,
    "analystRatingsHold": 18,
    "analystRatingsSell": 0,
    "analystRatingsStrongSell": 0
  }
]

"""