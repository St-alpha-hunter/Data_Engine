import re

import psycopg
import pytest

from config.paths import BASE_DIR
from db.config import PGConfig
from db.connections import connect_pg
from db.meta_store import MetaStore

"""

共享的数据库测试夹具：
- PostgreSQL：独立测试库 data_engine_test，每次运行按 sql/postgres/ 迁移文件从零重建
- ClickHouse：独立测试库 raw_test，每次运行按 sql/clickhouse/ 建表语句重建（raw. 替换成 raw_test.）
都不会影响正式库。数据库没启动时，依赖它的测试自动跳过。

"""

PG_TEST_DB = "data_engine_test"
PG_MIGRATIONS = sorted((BASE_DIR / "sql" / "postgres").glob("*.sql"))
META_TABLES = ("ref.trading_calendar, meta.load_log, meta.data_corrections, meta.quarantine_records, "
               "meta.quality_issues, meta.ingestion, meta.quality_rules")

CH_TEST_DB = "raw_test"
CH_DDL = sorted((BASE_DIR / "sql" / "clickhouse").glob("*.sql"))


def _seed_rules_sql() -> str:
    """002 里写入初始规则（PV001–PV004）的 INSERT 语句"""
    text = (BASE_DIR / "sql" / "postgres" / "002_meta_quality.sql").read_text(encoding="utf-8")
    return re.search(r"INSERT INTO meta\.quality_rules.*?;", text, re.S).group(0)


# =========================
# PostgreSQL
# =========================
@pytest.fixture(scope="session")
def test_db_config():
    try:
        admin = connect_pg()
    except (psycopg.OperationalError, KeyError) as e:
        pytest.skip(f"PostgreSQL 不可用：{e}")

    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {PG_TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {PG_TEST_DB}")

    cfg = PGConfig.from_env(dbname=PG_TEST_DB)
    with connect_pg(cfg) as conn:
        for path in PG_MIGRATIONS:
            conn.execute(path.read_text(encoding="utf-8"))
    return cfg


@pytest.fixture
def conn(test_db_config):
    """每个测试一个干净的库：清空业务数据，规则表恢复成初始的 PV001–PV004"""
    with connect_pg(test_db_config) as c:
        # TRUNCATE 不触发行级触发器，所以能清空只追加的 data_corrections
        c.execute(f"TRUNCATE {META_TABLES} RESTART IDENTITY CASCADE")
        c.execute(_seed_rules_sql())
        yield c


@pytest.fixture
def store(conn):
    return MetaStore(conn)


# =========================
# ClickHouse
# =========================
@pytest.fixture(scope="session")
def ch_session():
    try:
        from db.ch_store import ClickHouseStore
        ch = ClickHouseStore()
        ch.command("SELECT 1")
    except Exception as e:
        pytest.skip(f"ClickHouse 不可用：{e}")

    ch.command(f"DROP DATABASE IF EXISTS {CH_TEST_DB} SYNC")
    ch.command(f"CREATE DATABASE {CH_TEST_DB}")
    for path in CH_DDL:
        ddl = path.read_text(encoding="utf-8").replace("raw.", f"{CH_TEST_DB}.")
        ch.command(ddl)
    yield ch
    ch.command(f"DROP DATABASE IF EXISTS {CH_TEST_DB} SYNC")


@pytest.fixture
def ch(ch_session):
    """每个测试前清空 raw_test 里的表"""
    tables = ch_session.query_df(
        "SELECT name FROM system.tables WHERE database = {db:String}", {"db": CH_TEST_DB})["name"]
    for t in tables:
        ch_session.command(f"TRUNCATE TABLE {CH_TEST_DB}.{t}")
    return ch_session
