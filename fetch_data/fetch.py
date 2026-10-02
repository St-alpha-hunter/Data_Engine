import time
import requests
import logging
import pandas as pd
from config.endpoints import API_KEY, FMP_ENDPOINTS
from datetime import datetime, date

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

    def fetch_fmp_batch(self):
        df_list = []
        succeed_list = []
        failed_list = []
        missing_symbols = []

        extra_params = self._normalize_params()
        url_template = self._validate_endpoint(extra_params)

        for symbol in self.symbols:
            failure = None

            for i in range(self.retries):
                attempt = i + 1
                is_last = attempt == self.retries
                time.sleep(self.rate_delay)

                try:
                    url = url_template.format(
                        symbol=symbol,
                        apikey=API_KEY,
                        **extra_params
                    )
                except KeyError as e:
                    # URL 模板里的占位符没有对应参数，重试也没用
                    failure = self._failure(symbol, "url_format_error", attempt, error=f"缺少 URL 参数 {e}")
                    break

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
                    failure = self._failure(symbol, "auth_error", attempt, status, response.text[:300])
                    break

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
                    failure = self._failure(symbol, "client_error", attempt, status, response.text[:300])
                    break

                try:
                    data = response.json()
                except ValueError as e:
                    failure = self._failure(symbol, "invalid_json", attempt, status, f"{e}: {response.text[:300]}")
                    break

                # FMP 出错时有时返回 200 + {"Error Message": ...}
                if isinstance(data, dict) and ("Error Message" in data or "error" in data):
                    failure = self._failure(symbol, "api_error", attempt, status, str(data)[:300])
                    break

                if not isinstance(data, list) or len(data) == 0:
                    log.warning(f"[{self.endpoint_name}] 没有数据 for {symbol}")
                    missing_symbols.append(symbol)
                    failure = None
                    break

                df = pd.DataFrame(data)
                df["symbol"] = symbol
                df["endpoint"] = self.endpoint_name
                df_list.append(df)
                succeed_list.append(symbol)
                failure = None
                log.info(f"[{self.endpoint_name}] 成功拉取 {symbol}，{len(df)} 行")
                break

            if failure is not None:
                log.error(f"[{self.endpoint_name}] {symbol} 最终失败: {failure['error_type']}")
                failed_list.append(failure)

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