import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
from config.paths import FINANCIAL_ESTIMATE
from config.endpoints import API_KEY
from fetch_data.fetch import FetchData

load_dotenv()
log = logging.getLogger(__name__)
if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")


def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index(["symbol","date"]).sort_index()
    FINANCIAL_ESTIMATE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(FINANCIAL_ESTIMATE / f"{datetime.now():%Y%m%d_%H%M%S}.parquet")
    return df


if __name__ == "__main__":
    with FetchData(
        symbols=["AAPL","MSFT"],
        endpoint_name="financial_estimate",
        extra_params={
            "period":"annual",
            "page":0,
            "limit":10
        }
    ) as fetchData:
        df_raw, summary, succeed, failed = fetchData.fetch_fmp_batch()

    print(summary)
    if not df_raw.empty:
        save_file(df_raw)
    print("运行成功")

"""

[
	{
		"symbol": "AAPL",
		"date": "2029-09-28",
		"revenueLow": 483092500000,
		"revenueHigh": 483093500000,
		"revenueAvg": 483093000000,
		"ebitdaLow": 155952166036,
		"ebitdaHigh": 155952488856,
		"ebitdaAvg": 155952327446,
		"ebitLow": 140628295747,
		"ebitHigh": 140628586847,
		"ebitAvg": 140628441297,
		"netIncomeLow": 139446957701,
		"netIncomeHigh": 157185372990,
		"netIncomeAvg": 149150359609,
		"sgaExpenseLow": 31694652812,
		"sgaExpenseHigh": 31694718420,
		"sgaExpenseAvg": 31694685616,
		"epsAvg": 9.68,
		"epsHigh": 10.20148,
		"epsLow": 9.05024,
		"numAnalystsRevenue": 16,
		"numAnalystsEps": 6
	}
]

"""