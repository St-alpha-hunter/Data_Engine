import json
from datetime import datetime
import logging
import pandas as pd
from config.paths import ERRORS_REPORT


class FeedErrorDeputy:
    def __init__(self, cleaner, window:int=10):
        self.cleaner = cleaner
        self.endpoint_name = cleaner.endpoint_name
        self.errors = cleaner.errors
        self.raw_df = cleaner.raw_df ##未清洗的函数
        self.clean_df = cleaner.clean_df    ##已经清洗的函数
        self.stats_df = getattr(cleaner, "stats_df", None) ##对清洗之后的表统计
        self.window = window
        self.log = logging.getLogger(__name__)

    def generate_report(self):

        self._build_full_error_df() ###
        if self.full_error_df is None:
            return None
        ##定位错误于原数据
        self._build_context_windows()
        ##加载报告
        payload = self._build_payload()
        ##写入目录
        file_path = self._write_report(payload, len(self.full_error_df))
        self.log.info(f"JSON报告已保存: {file_path}")
        return file_path

    def _build_full_error_df(self):
        errors = [x["data错误地址"] for x in self.errors if x is not None and len(x) > 0]
        if not errors:
            self.full_error_df = None
            return
        ##返回错误大表
        self.full_error_df = pd.concat(errors, axis=0)
        #return self.full_error_df

    def _build_context_windows(self):
        self.context_windows = []
        for idx, row in self.full_error_df.iterrows():
            if idx not in self.raw_df.index:
                continue

            pos = self.raw_df.index.get_loc(idx)
            start = max(0, pos - self.window)
            end = pos + self.window + 1

            self.context_windows.append(self.raw_df.iloc[start:end])

        #return self.context_windows

    def _build_payload(self) ->dict:
        return {
            "summary报表总览": {
                "total_error_rows": len(self.full_error_df),
                "symbols": self.full_error_df["symbol"].nunique()
                if "symbol" in self.full_error_df.columns else None,
            },
            "global_stats": self._build_global_stats(),
            "context_windows": [
                x.to_dict(orient="records")
                for x in self.context_windows
            ],
            "error_rows": self.full_error_df.to_dict(orient="records"),
        }

    def _build_global_stats(self):
        if self.stats_df is None:
            return {}

        return self.stats_df.to_dict(orient="records")

    def _write_report(self, payload, error_count):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = (
            f"ai_error_report_"
            f"{error_count}_"
            f"{self.endpoint_name}_"
            f"{timestamp}.json"
        )

        ERRORS_REPORT.mkdir(parents=True, exist_ok=True)
        file_path = ERRORS_REPORT / file_name

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

        return file_path

# class FeedErrorDeputy:
#     def __init__(self, endpoint_name:str, errors:list[pd.DataFrame], df:pd.DataFrame, stats_df:pd.DataFrame, window:int):
#         #endpoint数据组名称
#         ##errors错误列表
#         ##df原表
#         ##stats统计
#         ##window截取窗口
#         ##error_dict用于生成错误报告的字典
#         self.endpoint_name = endpoint_name
#         self.errors = errors
#         self.df = df
#         self.stats_df = stats_df
#         self.window = window
#         self.error_dict = {
#         "错误子集回溯原表位置": [],
#         "全局统计特征": {}
#         }
#         self.log = logging.getLogger(__name__)
#
#     def generate_report(self):
#
#         full_error_df = self._build_full_error_df()
#         if full_error_df is None:
#             return None
#         ##定位错误于原数据
#         self.error_dict["错误子集回溯原表位置"] = self._build_context_windows(full_error_df)
#         ##加载报告
#         payload = self._build_payload(full_error_df)
#         ##写入目录
#         file_path = self._write_report(payload, len(full_error_df))
#
#         self.log.info(f"JSON报告已保存: {file_path}")
#         return file_path
#
#     def _build_full_error_df(self):
#         errors = [x for x in self.errors if x is not None and not x.empty]
#         if not errors:
#             return None
#         ##返回错误大表
#         return pd.concat(errors, axis=0)
#
#     def _build_context_windows(self, full_error_df):
#
#         for idx, row in full_error_df.iterrows():
#             if idx not in self.df.index:
#                 continue
#
#             pos = self.df.index.get_loc(idx)
#             start = max(0, pos - self.window)
#             end = pos + self.window + 1
#
#             self.error_dict["错误子集回溯原表位置"].append(self.df.iloc[start:end])
#
#         return self.error_dict["错误子集回溯原表位置"]
#
#     def _build_payload(self, full_error_df) ->dict:
#         return {
#             "summary报表总览": {
#                 "total_error_rows": len(full_error_df),
#                 "symbols": full_error_df["symbol"].nunique()
#                 if "symbol" in full_error_df.columns else None,
#             },
#             "global_stats": self._build_global_stats(),
#             "context_windows": [
#                 x.to_dict(orient="records")
#                 for x in self.error_dict["错误子集回溯原表位置"]
#             ],
#             "error_rows": full_error_df.to_dict(orient="records"),
#         }
#
#     def _build_global_stats(self):
#         if self.stats_df is None:
#             return {}
#
#         return self.stats_df.to_dict(orient="records")
#
#     def _write_report(self, payload, error_count):
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         file_name = (
#             f"ai_error_report_"
#             f"{error_count}_"
#             f"{self.endpoint_name}_"
#             f"{timestamp}.json"
#         )
#
#         file_path = ERRORS_REPORT / file_name
#
#         with open(file_path, "w", encoding="utf-8") as f:
#             json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
#
#         return file_path
