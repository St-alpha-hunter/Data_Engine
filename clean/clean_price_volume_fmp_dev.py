import numpy as np
import pandas as pd
from datetime import datetime

from config.paths import TEM_SYMBOL
from fetch_data.fetch import FetchData
from monitoring.feed_error_deputy import FeedErrorDeputy
from clean.base_clean import CleanBasic

class PriceVolume(CleanBasic):

    endpoint_name = "price_volume"

    config = {
        "required_columns": [
            "date", "symbol",
            "adjOpen", "adjHigh",
            "adjLow", "adjClose",
            "volume"
        ],

        "numeric_columns": [
            "adjOpen", "adjHigh",
            "adjLow", "adjClose",
            "volume"
        ],

        "date_columns": ["date"],

        "drop_columns": [
            "date", "symbol", "adjClose"
        ],

        "duplicate_columns": [
            "date", "symbol"
        ]
    }
    ##现在采取声明式，这一段选择注销
    # def __init__(self,df):
    #     super().__init__(
    #         df = df,
    #         endpoint_name=self.endpoint_name,
    #         config = self.default_config
    #     )

    def _custom_validate(self):
        # FMP 返回按日期倒序（最新在前），先转成每只股票内按日期正序，后面的收益率等时序计算才正确
        self.raw_df = self.raw_df.sort_values(["symbol", "date"]).reset_index(drop=True)
        self._check_positive_price()
        self._check_logic_relation()
        self._check_daily_volatility()
        self._compute_return()      # 必须在 _check_jump_soar 之前，后者依赖 return 列
        self._check_jump_soar()
        self.clean_df = self.raw_df  ##把处理过的值返回给clean_df
        return self.clean_df

    def _check_positive_price(self):
        # 价格必须 > 0；成交量可以为 0（停牌），但不能为负。mask 为 True 的行才是异常
        price_cols = ["adjOpen", "adjHigh", "adjLow", "adjClose"]
        mask = (self.raw_df[price_cols] <= 0).any(axis=1) | (self.raw_df["volume"] < 0)
        self._add_error(mask, "abnormal")

    def _check_logic_relation(self):

        mask = (
            (self.raw_df["adjOpen"] > self.raw_df["adjHigh"])
            | (self.raw_df["adjOpen"] < self.raw_df["adjLow"])
            | (self.raw_df["adjHigh"] < self.raw_df["adjLow"])
            | (self.raw_df["adjClose"] > self.raw_df["adjHigh"])
            | (self.raw_df["adjClose"] < self.raw_df["adjLow"])
        )

        self._add_error(mask, "logic_error")

    def _check_daily_volatility(self):

        price_max = self.raw_df[
            ["adjOpen", "adjClose", "adjHigh", "adjLow"]
        ].max(axis=1)

        price_min = self.raw_df[
            ["adjOpen", "adjClose", "adjHigh", "adjLow"]
        ].min(axis=1)

        mask = price_max > price_min * 1.15
        self._add_error(mask, "daily_big_vol")

    def _compute_return(self):
        self.raw_df["return"] = self.raw_df.groupby("symbol")["adjClose"].pct_change()

    def _check_jump_soar(self):
        self.raw_df["prev_return"] = self.raw_df.groupby("symbol")["return"].shift()
        mask = (((self.raw_df["prev_return"] - self.raw_df["return"]).abs()) > 0.15)
        self.raw_df["jump_soar"] = np.where(mask, 1, 0)
        self._add_error(mask,"big_soar_jump")

    def build_stats(self):
        self.stats_df = self.raw_df.groupby("symbol").agg(
            adjClose_mean=("adjClose", "mean"),
            adjClose_std=("adjClose", "std"),
            return_mean=("return", "mean"),
            return_std=("return", "std"),
            return_max=("return", "max"),
            return_min=("return", "min"),
            volume_mean=("volume", "mean"),
            volume_std=("volume", "std"),
        )
    ###想办法加上股票名称


if __name__ == "__main__":
    df = pd.read_csv(TEM_SYMBOL, header=None)
    symbols_pool = [symbol for symbol in df[0].tolist()]
    with FetchData(
        symbols=symbols_pool,
        endpoint_name="price_volume",
        extra_params={
            "from_date": datetime(2022, 1, 1),
            "to_date": datetime(2025, 1, 1)
        }
    ) as fetchData:
        full_df, summary, succeed_list, failed_list = fetchData.fetch_fmp_batch()

    cleaner = PriceVolume(full_df)
    ###一键优雅调用清洗函数
    clean_df = cleaner.clean()
    cleaner.build_stats() ##加一下股票symbol

    #df_clean, error_df_list = clean(full_df)
    print("初步清洗入库完成")

    feedError = FeedErrorDeputy(cleaner)
    file_path = feedError.generate_report()
    print("生成错误报告，并已保存")