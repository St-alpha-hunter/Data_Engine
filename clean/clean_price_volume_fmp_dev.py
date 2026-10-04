import pandas as pd
from datetime import datetime
import logging

from config.paths import TEM_SYMBOL
from fetch_data.fetch import FetchData
from monitoring.feed_error_deputy import FeedErrorDeputy
from clean.base_clean import CleanBasic, Rule


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
        ],

        # validate() 输出的原始字段（辅助列 return / prev_return 不输出）
        "output_columns": [
            "symbol", "date",
            "adjOpen", "adjHigh",
            "adjLow", "adjClose",
            "volume"
        ]
    }

    PRICE_COLS = ["adjOpen", "adjHigh", "adjLow", "adjClose"]

    # 规则以代码为准；改了 severity / params 必须把版本号 +1
    rules = [
        Rule("PV001", 1, "danger",  "价格 <= 0 或成交量 < 0",
             check="_check_positive_price"),
        Rule("PV002", 1, "danger",  "OHLC 逻辑矛盾：开/收盘价不在最高最低价之间，或最高价 < 最低价",
             check="_check_logic_relation"),
        # v2：跳过价格 <= 0 的行（这类行已由 PV001 隔离，最小值为 0 会让振幅检查必然触发）
        Rule("PV003", 2, "warning", "单日振幅过大：OHLC 最大值 > 最小值 × (1 + max_range)；价格 <= 0 的行不检查",
             check="_check_daily_volatility", params={"max_range": 0.15}),
        Rule("PV004", 1, "warning", "收益率跳变：相邻两日收益率之差的绝对值 > max_jump",
             check="_check_jump_soar", params={"max_jump": 0.15},
             detail_columns=("adjClose", "return", "prev_return")),
        Rule("PV005", 1, "danger",  "价格或成交量缺失：adjOpen/adjHigh/adjLow/adjClose/volume 有空值",
             check="_check_missing_values"),
    ]
    ##现在采取声明式，这一段选择注销
    # def __init__(self,df):
    #     super().__init__(
    #         df = df,
    #         endpoint_name=self.endpoint_name,
    #         config = self.default_config
    #     )

    def _prepare(self, df):
        # FMP 返回按日期倒序（最新在前），先转成每只股票内按日期正序，后面的收益率等时序计算才正确
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
        df["return"] = df.groupby("symbol")["adjClose"].pct_change(fill_method=None)
        df["prev_return"] = df.groupby("symbol")["return"].shift()
        return df

    # 每个检查返回布尔 Series，True 表示这一行违反规则
    def _check_positive_price(self, df):
        # 价格必须 > 0；成交量可以为 0（停牌），但不能为负
        return (df[self.PRICE_COLS] <= 0).any(axis=1) | (df["volume"] < 0)

    def _check_logic_relation(self, df):
        return (
            (df["adjOpen"] > df["adjHigh"])
            | (df["adjOpen"] < df["adjLow"])
            | (df["adjHigh"] < df["adjLow"])
            | (df["adjClose"] > df["adjHigh"])
            | (df["adjClose"] < df["adjLow"])
        )

    def _check_daily_volatility(self, df, max_range):
        price_max = df[self.PRICE_COLS].max(axis=1)
        price_min = df[self.PRICE_COLS].min(axis=1)
        all_positive = (df[self.PRICE_COLS] > 0).all(axis=1)
        return all_positive & (price_max > price_min * (1 + max_range))

    def _check_jump_soar(self, df, max_jump):
        return (df["prev_return"] - df["return"]).abs() > max_jump

    def _check_missing_values(self, df):
        # NaN 参与比较永远是 False，其他规则抓不到，单独拦截
        return df[self.PRICE_COLS + ["volume"]].isna().any(axis=1)

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


# 临时入口：拉取后直接清洗，pipelines 完成后删除
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

    log = logging.getLogger(__name__)
    cleaner = PriceVolume(full_df)
    ###一键优雅调用清洗函数
    clean_df = cleaner.clean()
    cleaner.build_stats() ##加一下股票symbol

    #df_clean, error_df_list = clean(full_df)
    log.info("初步清洗入库完成")

    feedError = FeedErrorDeputy(cleaner)
    file_path = feedError.generate_report()
    log.info("生成错误报告，并已保存")