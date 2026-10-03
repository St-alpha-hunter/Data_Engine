"""
连接工厂：一个进程内复用同一个连接，断开后自动重连
"""
import logging

import clickhouse_connect
import psycopg

from db.config import CHConfig, PGConfig

log = logging.getLogger(__name__)

_pg_conn: psycopg.Connection | None = None
_ch_client = None


def connect_pg(config: PGConfig | None = None) -> psycopg.Connection:
    """新建一个 Postgres 连接。autocommit=True：单条语句立即生效，多条语句用 conn.transaction() 包成事务。"""
    cfg = config or PGConfig.from_env()
    return psycopg.connect(
        host=cfg.host, port=cfg.port, dbname=cfg.dbname,
        user=cfg.user, password=cfg.password,
        autocommit=True,
    )


def get_pg() -> psycopg.Connection:
    """进程内共享的 Postgres 连接"""
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = connect_pg()
        log.info("已连接 PostgreSQL")
    return _pg_conn


def get_ch():
    """进程内共享的 ClickHouse 客户端"""
    global _ch_client
    if _ch_client is None:
        cfg = CHConfig.from_env()
        _ch_client = clickhouse_connect.get_client(
            host=cfg.host, port=cfg.port, username=cfg.user, password=cfg.password,
        )
        log.info("已连接 ClickHouse")
    return _ch_client


def close_all():
    global _pg_conn, _ch_client
    if _pg_conn is not None and not _pg_conn.closed:
        _pg_conn.close()
    if _ch_client is not None:
        _ch_client.close()
    _pg_conn = None
    _ch_client = None
