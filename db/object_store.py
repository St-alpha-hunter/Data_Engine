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

路径可移植：项目目录里的文件记相对路径（data/volume_data_1y/xxx.parquet），Windows 本机和 Docker 容器（Airflow）里都能读；
以前记下的 Windows 绝对路径（E:\data_process\data\...）读取时自动换算成当前项目目录下的 data/...
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
        """文件地址：远程是 URL；本地在项目目录里的记相对路径（跨系统可用），其他记绝对路径"""
        if self.is_remote:
            return f"{self.base_url}/{endpoint}/{batch_id}.parquet"
        if self.base_url is None:
            path = Path(ENDPOINT_DATA_DIRS.get(endpoint, DATA_DIR / endpoint)) / f"{batch_id}.parquet"
        else:
            path = Path(self.base_url) / endpoint / f"{batch_id}.parquet"
        try:
            return path.resolve().relative_to(BASE_DIR.resolve()).as_posix()
        except ValueError:
            return str(path)

    @staticmethod
    def resolve(location: str) -> str:
        """记录的地址 → 当前环境下能打开的地址"""
        if "://" in location:
            return location
        normalized = location.replace("\\", "/")
        path = Path(normalized)
        if not path.is_absolute() and not (len(normalized) > 1 and normalized[1] == ":"):
            return str(BASE_DIR / path)
        if path.exists():
            return str(path)
        # 别的系统上记下的项目内绝对路径（如 Windows 的 E:/data_process/data/...）：按 data/ 之后的部分换算
        parts = normalized.split("/")
        if "data" in parts:
            return str(BASE_DIR.joinpath(*parts[parts.index("data"):]))
        return location

    def write(self, df: pd.DataFrame, endpoint: str, batch_id: str) -> str:
        """写入 parquet，返回文件地址（记进 meta.load_log）"""
        loc = self.location(endpoint, batch_id)
        target = self.resolve(loc)
        if not self.is_remote:
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(target, index=False, storage_options=self.storage_options)
        return loc

    def read(self, location: str) -> pd.DataFrame:
        target = self.resolve(location)
        return pd.read_parquet(target, storage_options=self.storage_options if "://" in target else None)
