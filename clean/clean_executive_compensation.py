import logging
from datetime import datetime

import pandas as pd

from config.endpoints import API_KEY
from config.paths import EXECUTE_PATH
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

"""

高管薪酬（FMP governance-executive-compensation）：空架子，清洗逻辑待实现。文件末尾是 FMP 返回的样例数据，作为字段参考。

"""


def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["filingDate"] = pd.to_datetime(df["filingDate"])
    df["acceptedDate"] = pd.to_datetime(df["acceptedDate"])
    df = df.set_index(["symbol", "acceptedDate"]).sort_index()
    EXECUTE_PATH.mkdir(parents=True, exist_ok=True)
    df.to_parquet(EXECUTE_PATH / f"{datetime.now():%Y%m%d_%H%M%S}.parquet")
    return df


# 临时入口：拉取并保存，pipelines 完成后改由 pipeline 调用
if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("FMP_API_KEY NOT SET IN")

    with FetchData(
        symbols=["AAPL", "MSFT"],
        endpoint_name="executive_compensation",
        extra_params=None
    ) as fetchData:
        df_raw, summary, succeed, failed = fetchData.fetch_fmp_batch()

    print(summary)
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
