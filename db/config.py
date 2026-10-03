"""
数据库连接配置：统一从项目根目录的 .env 读取
"""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

from config.paths import BASE_DIR

# 显式指定路径：find_dotenv() 在 stdin / 交互环境下会失败
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class PGConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls, dbname: str | None = None) -> "PGConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            dbname=dbname or os.getenv("POSTGRES_DB", "data_engine"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )


@dataclass(frozen=True)
class CHConfig:
    host: str
    port: int
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "CHConfig":
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
