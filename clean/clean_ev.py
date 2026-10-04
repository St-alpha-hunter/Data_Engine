import logging
from datetime import datetime

import pandas as pd

from config.endpoints import API_KEY
from config.paths import ENTERPRISE_PATH
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

"""

企业价值（FMP enterprise-values）：空架子，清洗逻辑待实现。文件末尾是 FMP 返回的样例数据，作为字段参考。

"""


def clean(df:pd.DataFrame):
    pass

def compute(df:pd.DataFrame):
    pass

def save_file(df:pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index(["symbol", "date"]).sort_index()
    ENTERPRISE_PATH.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ENTERPRISE_PATH / f"{datetime.now():%Y%m%d_%H%M%S}.parquet")
    return df


# 临时入口：拉取并保存，pipelines 完成后改由 pipeline 调用
if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("FMP_API_KEY NOT SET IN")

    with FetchData(
        symbols=["AAPL", "MSFT"],
        endpoint_name="enterprise_values",
        extra_params=None
    ) as fetchData:
        df_raw, summary, succeed, failed = fetchData.fetch_fmp_batch()

    print(summary)
    print("运行成功")

"""



"""