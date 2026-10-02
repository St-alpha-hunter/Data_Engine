import pandas as pd
import logging
from dotenv import load_dotenv
from config.endpoints import API_KEY
from config.paths import EXECUTE_PATH
from fetch_data.fetch import fetch_fmp_batch

load_dotenv()
log = logging.getLogger(__name__)
if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")

df_raw, summary, succeed, failed = fetch_fmp_batch(
    symbols=["AAPL", "MSFT"],
    endpoint_name="executive_compensation",
    extra_params={
    }
)

def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["filingDate"] = pd.to_datetime(df["filingDate"])
    df["acceptedDate"] = pd.to_datetime(df["acceptedDate"])
    df = df.set_index(["symbols","acceptDate"], inplace=False)
    df = df.sort_values(by=["acceptedDate"], ascending=True)
    df.to_parquet(EXECUTE_PATH)
    return df

print("运行成功")

"""

[
	{
		"cik": "0000320193",
		"symbol": "AAPL",
		"companyName": "Apple Inc.",
		"filingDate": "2026-01-08",
		"acceptedDate": "2026-01-08 16:31:36",
		"nameAndPosition": "Tim Cook Chief Executive Officer",
		"year": 2025,
		"salary": 3000000,
		"bonus": 0,
		"stockAward": 57535293,
		"optionAward": 0,
		"incentivePlanCompensation": 12000000,
		"allOtherCompensation": 1759518,
		"total": 74294811,
		"link": "https://www.sec.gov/Archives/edgar/data/320193/000130817926000008/0001308179-26-000008-index.htm"
	}
]

"""
