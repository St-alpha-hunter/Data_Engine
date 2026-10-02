import pandas as pd
import logging
from dotenv import load_dotenv
from config.endpoints import API_KEY
from config.paths import ENTERPRISE_PATH
from fetch_data.fetch import fetch_fmp_batch

load_dotenv()
log = logging.getLogger(__name__)
if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")


df_raw, summary, succeed, failed = fetch_fmp_batch(
    symbols = ["AAPL", "MSFT"],
    endpoint_name = "enterprise_values",
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
    df.to_parquet(ENTERPRISE_PATH)
    return df

print("运行成功")

"""



"""