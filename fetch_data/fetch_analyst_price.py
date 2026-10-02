import pandas as pd
import logging
from dotenv import load_dotenv
from config.endpoints import API_KEY
from config.paths import ANALYST_GRADES
from fetch_data.fetch import fetch_fmp_batch

load_dotenv()
log = logging.getLogger(__name__)
if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")

df_raw, summary, succeed, failed = fetch_fmp_batch(
    symbols = ["AAPL", "MSFT"],
    endpoint_name = "cash_flow",
    extra_params={
    }
)

def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["date"] = pd.to_datetime(df["datetime"])
    df = df.set_index(["symbols","date"], inplace=False)
    df = df.sort_values(by=["date"], ascending=True)
    df.to_parquet(ANALYST_GRADES)
    return df


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