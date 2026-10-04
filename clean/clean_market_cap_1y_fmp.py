import numpy as np
import pandas as pd
from datetime import datetime
from config.paths import TEM_SYMBOL
from fetch_data.fetch import FetchData
from monitoring.feed_error_deputy import FeedErrorDeputy
from clean.base_clean import CleanBasic


class MarketCap(CleanBasic):
    endpoint_name = "market_cap"

    config = {
        "required_columns":[
            "symbol","date","marketCap"
        ],
        "numeric_columns":[
            "marketCap"
        ],
        "date_columns":[
            "date"
        ],
        "drop_columns":[
            "date","symbol"
        ],
        "duplicate_columns":[
            "date","symbol"
        ]
    }

    def _custom_validate(self):
        self._check_positive_price()

    def _check_positive_price(self):
        mask = ~(
            self.raw_df["marketCap"]>0
        )
        self._add_error(mask, "abnormal")

    def _check_others(self):
        pass


if __name__ == "__main__":
    df = pd.read_csv(TEM_SYMBOL, header=None)
    symbols_pool = [symbol for symbol in df[0].tolist()]
    with FetchData(
        symbols=symbols_pool,
        endpoint_name="market_cap",
        extra_params=None
    ) as fetchData:
        full_df, summary, succeed_list, failed_list = fetchData.fetch_fmp_batch()

    cleaner = MarketCap(full_df)
    ###一键优雅调用清洗函数
    cleaner.clean()
    #cleaner.build_stats() ##加一下股票symbol

    print("初步清洗入库完成")

    feedError = FeedErrorDeputy(cleaner)
    file_path = feedError.generate_report()
    print("生成错误报告，并已保存")




# import logging
# import pandas as pd
# from datetime import datetime
# from config.endpoints import API_KEY, FMP_ENDPOINTS
# from fetch_data.fetch import fetch_fmp_batch
# from config.paths import MARKET_CAP_PATH
# from dotenv import load_dotenv
#
# load_dotenv()
# log = logging.getLogger(__name__)
# if not API_KEY:
#     raise ValueError("FMP_API_KEY NOT SET IN")
#
# df_raw, summary, succeed, failed = fetch_fmp_batch(
#     symbols = ["AAPL", "MSFT"],
#     endpoint_name = "cash_flow",
#     extra_params={
#     }
# )
#
# def clean(df:pd.DataFrame) -> tuple:
#     if df is None:
#         raise ValueError("收到空表m请检查错误")
#     error_df_list = []
#     required_columns = ["symbol", "date", "marketCap"]
#     missing_columns = list(set(required_columns)-set(df.columns))
#     if missing_columns:
#         raise KeyError(f"存在遗漏")
#
#     df["date"]  = pd.to_datetime(df["date"], errors = "coerce")
#     df["marketCap"] = pd.to_numeric(df["marketCap"], errors = "coerce")
#
#     df = df.dropna(subset=["symbol","date"])
#     df = df.drop_duplicates(subset=["symbol","date"])
#
#     #异常值
#     mask_zero = (df["marketCap"]>0)
#     error_df_list.append(df.loc[mask_zero])
#
#     ##基本逻辑关系，没有
#
#     return df, error_df_list
#
#
# def feed_error_to_deputy(error:list[pd.DataFrame], df:pd.DataFrame, window:int)->tuple[pd.DataFrame, dict] | None:
#     error_dict = {
#         "错误子集回测"
#     }
#
# """
# 1,错误报告单
# 2,附近窗口数据
# 告诉 AI：这个错误是不是孤立异常
# 3. 全局统计特征,上下文诊断
# 告诉 AI：这个值在整体分布里有多离谱
# """
#
#
# def compute(df):
#     pass
#
# def save_file(df:pd.DataFrame):
#     df["date"] = pd.to_datetime(df["date"])
#     df = df.set_index(["symbol","date"],inplace=False)
#     df = df.sort_values(by=["date"], ascending=True)
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     df.to_parquet(MARKET_CAP_PATH/f"{timestamp}.parquet")
#     return df
#
# print("运行成功")
#
# """
#
# [
# 	{
# 		"symbol": "AAPL",
# 		"date": "2025-10-24",
# 		"marketCap": 3900351299800
# 	}
# ]
#
# """