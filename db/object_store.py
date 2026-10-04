"""
中间态 parquet 的存储

默认：存本地，每个 endpoint 用 config/paths.py 里 ENDPOINT_DATA_DIRS 登记的目录
    price_volume -> data/volume_data_1y/{batch_id}.parquet
    market_cap   -> data/market_cap_data_1y_fmp/{batch_id}.parquet
    没登记的     -> data/{endpoint}/{batch_id}.parquet

以后换成对象存储（S3 兼容：RustFS / SeaweedFS / 阿里云 OSS / AWS S3 ...），只改 .env，代码不用改：
    INTERMEDIATE_URL=s3://data-engine/intermediate     # 设置后按 {INTERMEDIATE_URL}/{endpoint}/{batch_id}.parquet 存
    S3_ENDPOINT=http://localhost:9010                  # 对象存储的地址（AWS S3 可不填）
    S3_ACCESS_KEY=...
    S3_SECRET_KEY=...

重跑同一个批次会覆盖同名文件。
"""
import os
from pathlib import Path

import pandas as pd

from config.paths import BASE_DIR, DATA_DIR, ENDPOINT_DATA_DIRS
import db.config  # noqa: F401  导入时加载 .env


class IntermediateStore:

    def __init__(self, base_url: str | None = None, storage_options: dict | None = None):
        base_url = base_url or os.getenv("INTERMEDIATE_URL")
        self.base_url = None                      # None 表示按 ENDPOINT_DATA_DIRS 存本地
        self.is_remote = False
        if base_url:
            self.is_remote = "://" in base_url
            if self.is_remote:
                self.base_url = base_url.rstrip("/")
            else:
                path = Path(base_url)
                self.base_url = str(path if path.is_absolute() else BASE_DIR / path)
        self.storage_options = storage_options if storage_options is not None else self._options_from_env()

    def _options_from_env(self) -> dict | None:
        if not (self.base_url or "").startswith("s3://"):
            return None
        options = {"key": os.getenv("S3_ACCESS_KEY"), "secret": os.getenv("S3_SECRET_KEY")}
        if os.getenv("S3_ENDPOINT"):
            options["client_kwargs"] = {"endpoint_url": os.getenv("S3_ENDPOINT")}
        return options

    def location(self, endpoint: str, batch_id: str) -> str:
        if self.base_url is None:
            folder = ENDPOINT_DATA_DIRS.get(endpoint, DATA_DIR / endpoint)
            return str(Path(folder) / f"{batch_id}.parquet")
        if self.is_remote:
            return f"{self.base_url}/{endpoint}/{batch_id}.parquet"
        return str(Path(self.base_url) / endpoint / f"{batch_id}.parquet")

    def write(self, df: pd.DataFrame, endpoint: str, batch_id: str) -> str:
        """写入 parquet，返回文件地址"""
        loc = self.location(endpoint, batch_id)
        if not self.is_remote:
            Path(loc).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(loc, index=False, storage_options=self.storage_options)
        return loc

    def read(self, location: str) -> pd.DataFrame:
        return pd.read_parquet(location, storage_options=self.storage_options if "://" in location else None)
