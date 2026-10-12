import time
import requests
import logging
import pandas as pd
from config.endpoints import API_KEY, FMP_ENDPOINTS
from config.symbol_map import to_fmp_symbol
from datetime import datetime, date, timedelta

log = logging.getLogger(__name__)



class FetchData:

    def __init__(self, symbols:list[str] ,endpoint_name:str, extra_params:dict|None):
        self.symbols = symbols
        self.endpoint_name = endpoint_name
        self.extra_params = extra_params
        self.retries = 3
        self.rate_delay = 0.35
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _normalize_params(self)->dict:
        """把 extra_params 里的日期类参数统一转成字符串，避免 format 时报错。"""
        if not self.extra_params:
            return {}

        normalized = {}

        for key, value in self.extra_params.items():
            if isinstance(value, (datetime, date)):
                normalized[key] = value.strftime("%Y-%m-%d")
            elif isinstance(value, pd.Timestamp):
                normalized[key] = value.strftime("%Y-%m-%d")
            else:
                normalized[key] = value

        return normalized

    def _validate_endpoint(self, extra_params: dict) -> str:
        """检查 endpoint 是否存在、必填参数是否齐全，返回 URL 模板。"""
        if self.endpoint_name not in FMP_ENDPOINTS:
            raise ValueError(
                f"未知 endpoint: {self.endpoint_name}，可选: {list(FMP_ENDPOINTS)}"
            )

        endpoint = FMP_ENDPOINTS[self.endpoint_name]
        missing_params = [p for p in endpoint.get("required_params", []) if p not in extra_params]
        if missing_params:
            raise ValueError(f"endpoint {self.endpoint_name} 缺少必填参数: {missing_params}")

        return endpoint["url"]

    def _failure(self, symbol: str, error_type: str, attempts: int,
                 status_code: int | None = None, error: str | None = None) -> dict:
        """统一失败记录的字段，方便后续写报告和重跑。"""
        return {
            "symbol": symbol,
            "endpoint": self.endpoint_name,
            "error_type": error_type,
            "status_code": status_code,
            "attempts": attempts,
            "error": error,
        }

    def _get_json(self, url: str, symbol: str) -> tuple[str, list | dict | None]:
        """
        请求一次（含重试）。返回：
            ("ok", 数据列表) / ("empty", None) 没有数据 / ("fail", 失败记录)
        """
        failure = None
        for i in range(self.retries):
            attempt = i + 1
            is_last = attempt == self.retries
            time.sleep(self.rate_delay)

            try:
                response = self.session.get(url, timeout=30)
            except requests.exceptions.Timeout:
                log.warning(f"[{self.endpoint_name}] {symbol} 请求超时，第 {attempt} 次")
                failure = self._failure(symbol, "timeout", attempt)
                if not is_last:
                    time.sleep(self.rate_delay * attempt * 2)
                continue
            except requests.exceptions.RequestException as e:
                log.warning(f"[{self.endpoint_name}] {symbol} 请求失败，第 {attempt} 次: {e}")
                failure = self._failure(symbol, "request_error", attempt, error=str(e))
                if not is_last:
                    time.sleep(self.rate_delay * attempt * 2)
                continue

            status = response.status_code

            # 鉴权 / 套餐问题：重试无意义
            if status in (401, 402, 403):
                return "fail", self._failure(symbol, "auth_error", attempt, status, response.text[:300])

            # 限流：退避后重试
            if status == 429:
                log.warning(f"[{self.endpoint_name}] {symbol} 触发限流，第 {attempt} 次")
                failure = self._failure(symbol, "rate_limited", attempt, status, response.text[:300])
                if not is_last:
                    time.sleep(self.rate_delay * attempt * 5)
                continue

            # 服务端错误：退避后重试
            if status >= 500:
                log.warning(f"[{self.endpoint_name}] {symbol} 服务端错误 {status}，第 {attempt} 次")
                failure = self._failure(symbol, "server_error", attempt, status, response.text[:300])
                if not is_last:
                    time.sleep(self.rate_delay * attempt * 2)
                continue

            # 其他 4xx：参数或 symbol 有问题，不重试
            if status >= 400:
                return "fail", self._failure(symbol, "client_error", attempt, status, response.text[:300])

            try:
                data = response.json()
            except ValueError as e:
                return "fail", self._failure(symbol, "invalid_json", attempt, status, f"{e}: {response.text[:300]}")

            # FMP 出错时有时返回 200 + {"Error Message": ...}
            if isinstance(data, dict) and ("Error Message" in data or "error" in data):
                return "fail", self._failure(symbol, "api_error", attempt, status, str(data)[:300])

            if not isinstance(data, list) or len(data) == 0:
                return "empty", None
            return "ok", data
        return "fail", failure

    @staticmethod
    def _date_windows(params: dict, chunk_days: int | None) -> list[tuple[date, date] | None]:
        """把 [from_date, to_date] 切成互不重叠的几段（两头都含）；不分段时返回 [None]"""
        if not chunk_days or "from_date" not in params or "to_date" not in params:
            return [None]
        start, end = date.fromisoformat(str(params["from_date"])), date.fromisoformat(str(params["to_date"]))
        windows = []
        while start <= end:
            stop = min(start + timedelta(days=chunk_days - 1), end)
            windows.append((start, stop))
            start = stop + timedelta(days=1)
        return windows

    def _fetch_unit(self, url_template: str, params: dict, symbol: str, max_rows: int | None,
                    chunk_days: int | None) -> tuple[str, list | dict | None]:
        """
        拉一个请求单位（一只股票 / 一页）的全部数据。按日期分段时：
            某段返回的行数达到 max_rows（可能被截断）→ 对半拆开重拉，直到不再顶到上限
            任一段失败、或顶到上限又没法再拆（没配分段 / 已经只剩 1 天）→ 整个单位失败，不返回部分数据
        返回 (状态, 数据或失败记录)
        """
        rows = []
        stack = list(reversed(self._date_windows(params, chunk_days)))
        while stack:
            window = stack.pop()
            p = params if window is None else {**params, "from_date": window[0].isoformat(), "to_date": window[1].isoformat()}
            try:
                url = url_template.format(apikey=API_KEY, **p)
            except KeyError as e:
                # URL 模板里的占位符没有对应参数，重试也没用
                return "fail", self._failure(symbol, "url_format_error", 1, error=f"缺少 URL 参数 {e}")
            status, data = self._get_json(url, symbol)
            if status == "fail":
                return "fail", data
            if status == "empty":
                continue
            if max_rows and len(data) >= max_rows:
                if window is not None and window[0] < window[1]:
                    mid = window[0] + (window[1] - window[0]) / 2
                    log.info(f"[{self.endpoint_name}] {symbol} {window[0]}~{window[1]} 返回 {len(data)} 行，达到上限，拆开重拉")
                    stack += [(mid + timedelta(days=1), window[1]), (window[0], mid)]      # 先拉前半段
                    continue
                return "fail", self._failure(symbol, "truncated", 1,
                                             error=f"返回 {len(data)} 行，达到单次上限 {max_rows}，数据可能被截断")
            rows.extend(data)
        return ("ok", rows) if rows else ("empty", None)

    def fetch_fmp_batch(self):
        df_list = []
        succeed_list = []
        failed_list = []
        missing_symbols = []

        extra_params = self._normalize_params()
        url_template = self._validate_endpoint(extra_params)
        endpoint = FMP_ENDPOINTS[self.endpoint_name]
        # 请求方式（config/endpoints.py 的 iterate）：
        #   symbol（默认）：self.symbols 是股票代码，每只一个请求，数据打上请求的代码
        #   none：不按股票请求，self.symbols 只是请求的标签（如 ["ALL"]），数据自带的 symbol 不覆盖
        #   page：self.symbols 是页码，按页请求，遇到空页就停止
        iterate = endpoint.get("iterate", "symbol")
        # 单次返回上限（max_rows）、按日期分段（chunk_days）：FMP 超过上限会悄悄只返回最近的部分
        max_rows, chunk_days = endpoint.get("max_rows"), endpoint.get("chunk_days")

        for symbol in self.symbols:
            # 按股票请求时 URL 用 FMP 的代码写法（BRK.B → BRK-B），数据仍打上项目内部写法
            unit = {"symbol": {"symbol": to_fmp_symbol(symbol)}, "page": {"page": symbol}}.get(iterate, {})
            status, data = self._fetch_unit(url_template, {**extra_params, **unit}, symbol, max_rows, chunk_days)

            if status == "fail":
                log.error(f"[{self.endpoint_name}] {symbol} 最终失败: {data['error_type']}")
                failed_list.append(data)
                continue
            if status == "empty":
                missing_symbols.append(symbol)
                if iterate == "page":
                    log.info(f"[{self.endpoint_name}] 第 {symbol} 页为空，翻页结束")
                    break                                   # 翻到空页，后面的页不再请求
                log.warning(f"[{self.endpoint_name}] 没有数据 for {symbol}")
                continue

            df = pd.DataFrame(data)
            if iterate == "symbol":
                df["symbol"] = symbol                       # 按股票请求：打上请求的代码；其他方式保留数据自带的 symbol
            df["endpoint"] = self.endpoint_name
            df_list.append(df)
            succeed_list.append(symbol)
            log.info(f"[{self.endpoint_name}] 成功拉取 {symbol}，{len(df)} 行")

        # session 由创建者负责关闭（with 语句或手动 close()），这里不关，便于同一实例多次调用复用连接
        full_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

        summary = {
            "endpoint": self.endpoint_name,
            "total": len(self.symbols),
            "success": len(succeed_list),
            "missing": len(missing_symbols),
            "failed": len(failed_list),
            "missing_symbols": missing_symbols,
        }

        return full_df, summary, succeed_list, failed_list


if __name__ == "__main__":
    with FetchData(
        symbols=["AAPL"],
        endpoint_name="price_volume",
        extra_params={
            "from_date": datetime(2022, 1, 1),
            "to_date": datetime(2025, 1, 1),
        }
    ) as fetch:
        df_raw, summary, succeed, failed = fetch.fetch_fmp_batch()
    print("拉取数据成功")