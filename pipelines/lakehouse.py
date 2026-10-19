"""
golden（ClickHouse）→ 湖仓（Delta Lake 开放表格式），用 DuckDB 查询

为什么要这一层：golden 只在 ClickHouse 里，只有 ClickHouse 能读；导出成 Delta（parquet 文件 + 事务日志 _delta_log/）后
    · DuckDB / Spark / Databricks / Trino 都能直接读，不绑定某个数据库
    · 每次导出是一次原子提交 = 一个版本，可以按版本 / 时间点回看（时间旅行）
    · 放在本地目录或对象存储（gs:// / s3://）都一样，上云只改 LAKEHOUSE_URL

湖里的表都是"当前状态"：每个主键一行（golden 的当前版本）；历史靠 Delta 的版本回看
    price_daily        主键 (security_id, date)                     按年分区
    adjustment_events  主键 (security_id, ex_date, kind, event_id)  含已撤回的（is_active = false），用的时候过滤
    securities         整表；内容变了才覆盖

增量：golden 只追加、loaded_at 递增，上次导出到哪（水位线）记在 Delta 提交信息里，不另建表；
    每次取 loaded_at >= 水位线的行，同一主键取最大版本，MERGE 进湖：版本更大才更新，没有就插入
    → 重跑不会重复（版本相同不更新），没有新数据不提交（不产生空版本）

维护（每周一次，在导出时顺带检查）：合并小文件（每天 MERGE 会产生小文件）、写检查点、
    清理 30 天前已不用的文件 → 时间旅行最多回看 30 天
"""
import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import CommitProperties, DeltaTable, write_deltalake

from config.paths import BASE_DIR, DATA_DIR
from db import ClickHouseStore
import db.config  # noqa: F401  导入时加载 .env

log = logging.getLogger(__name__)

GOLDEN_DB = "golden"
WATERMARK_KEY = "golden_watermark"
RETENTION = timedelta(days=30)
MAINTAIN_EVERY = timedelta(days=7)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_TS = pa.timestamp("us", tz="UTC")
TABLES = {
    "price_daily": {
        "keys": ["security_id", "date"],
        "partition_by": ["year"],
        "schema": pa.schema([
            ("security_id", pa.int64()), ("date", pa.date32()), ("year", pa.int32()),
            ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
            ("volume", pa.float64()), ("quality_status", pa.string()), ("source", pa.string()),
            ("source_symbol", pa.string()), ("batch_id", pa.string()), ("source_logic_version", pa.int32()),
            ("version", pa.int64()), ("loaded_at", _TS),
        ]),
    },
    "adjustment_events": {
        "keys": ["security_id", "ex_date", "kind", "event_id"],
        "partition_by": None,
        "schema": pa.schema([
            ("security_id", pa.int64()), ("ex_date", pa.date32()), ("kind", pa.string()), ("value", pa.float64()),
            ("prev_close", pa.float64()), ("price_factor", pa.float64()), ("volume_factor", pa.float64()),
            ("is_active", pa.bool_()), ("event_id", pa.int64()), ("event_revision", pa.int64()),
            ("known_at", _TS), ("version", pa.int64()), ("loaded_at", _TS),
        ]),
    },
    "securities": {
        "schema": pa.schema([
            ("security_id", pa.int64()), ("symbol", pa.string()), ("name", pa.string()), ("status", pa.string()),
            ("security_type", pa.string()), ("primary_exchange", pa.string()), ("sector", pa.string()),
            ("industry", pa.string()), ("first_trade_date", pa.date32()), ("last_trade_date", pa.date32()),
        ]),
    },
}


# 不是从 golden 导出的湖表（分钟线 demo：bronze 由 pipelines/intraday.py 写，silver 由 spark_jobs/bars.py 写），connect() 一起挂上
OTHER_TABLES = ["bars_5min", "bars"]


# =========================
# 位置
# =========================
def lake_url() -> str:
    """LAKEHOUSE_URL：不设 → data/lakehouse；相对路径按项目目录；gs:// / s3:// 原样用"""
    url = os.getenv("LAKEHOUSE_URL")
    if not url:
        return str(DATA_DIR / "lakehouse")
    if "://" in url:
        return url.rstrip("/")
    path = Path(url)
    return str(path if path.is_absolute() else BASE_DIR / path)


def storage_options() -> dict | None:
    """对象存储的认证；本地目录不需要。GCS 用服务账号密钥文件（GOOGLE_APPLICATION_CREDENTIALS）"""
    if lake_url().startswith("gs://") and os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return {"google_service_account_path": os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}
    return None


def table_uri(name: str, base: str | None = None) -> str:
    base = base or lake_url()
    return f"{base}/{name}" if "://" in base else str(Path(base) / name)


def open_table(name: str, base: str | None = None, version: int | None = None) -> DeltaTable | None:
    uri = table_uri(name, base)
    if not DeltaTable.is_deltatable(uri, storage_options=storage_options()):
        return None
    return DeltaTable(uri, version=version, storage_options=storage_options())


def watermark(dt: DeltaTable | None) -> datetime:
    """最近一次带水位线的提交里记下的 golden loaded_at；没有导出过 → 1970"""
    if dt is None:
        return EPOCH
    for commit in dt.history():                      # 从新到旧
        if WATERMARK_KEY in commit:
            return datetime.fromisoformat(commit[WATERMARK_KEY])
    return EPOCH


# =========================
# 从 golden 取数
# =========================
def _arrow(ch: ClickHouseStore, sql: str, params: dict, schema: pa.Schema) -> pa.Table:
    t = ch.client.query_arrow(sql, parameters=params)
    return t.select(schema.names).cast(schema)


def changed_prices(ch: ClickHouseStore, since: datetime, db: str = GOLDEN_DB) -> pa.Table:
    # 版本号越大 loaded_at 越晚，所以新写入的行里每个主键的最大版本 = 当前版本
    # 先在子查询里过滤：argMax(loaded_at, …) AS loaded_at 这个别名会被 ClickHouse 代进同一层的 WHERE
    cols = ["open", "high", "low", "close", "volume", "quality_status", "source", "source_symbol",
            "batch_id", "source_logic_version", "loaded_at"]
    sql = f"""
        SELECT security_id, date, toYear(date) AS year,
               {", ".join(f"argMax({c}, version) AS {c}" for c in cols)}, max(version) AS v
        FROM (SELECT * FROM {db}.price_daily WHERE loaded_at >= {{since:DateTime64(3, 'UTC')}})
        GROUP BY security_id, date"""
    t = ch.client.query_arrow(sql, parameters={"since": since})
    t = t.rename_columns(["version" if c == "v" else c for c in t.column_names])
    return t.select(TABLES["price_daily"]["schema"].names).cast(TABLES["price_daily"]["schema"])


def changed_adjustments(ch: ClickHouseStore, since: datetime, db: str = GOLDEN_DB) -> pa.Table:
    cols = ["value", "prev_close", "price_factor", "volume_factor", "is_active", "event_revision", "known_at", "loaded_at"]
    sql = f"""
        SELECT security_id, ex_date, kind, event_id,
               {", ".join(f"argMax({c}, version) AS {c}" for c in cols)}, max(version) AS v
        FROM (SELECT * FROM {db}.adjustment_events WHERE loaded_at >= {{since:DateTime64(3, 'UTC')}})
        GROUP BY security_id, ex_date, kind, event_id"""
    t = ch.client.query_arrow(sql, parameters={"since": since})
    t = t.rename_columns(["version" if c == "v" else c for c in t.column_names])
    schema = TABLES["adjustment_events"]["schema"]
    t = t.set_column(t.column_names.index("is_active"), "is_active",
                     pc.not_equal(t["is_active"], 0))
    return t.select(schema.names).cast(schema)


# =========================
# 写入
# =========================
def _merge(name: str, source: pa.Table, new_watermark: datetime, base: str | None) -> dict:
    spec = TABLES[name]
    props = CommitProperties(custom_metadata={WATERMARK_KEY: new_watermark.isoformat(), "source": f"golden.{name}"})
    dt = open_table(name, base)
    if dt is None:                                   # 第一次：直接建表
        write_deltalake(table_uri(name, base), source, partition_by=spec["partition_by"],
                        storage_options=storage_options(), commit_properties=props)
        return {"inserted": source.num_rows, "updated": 0}
    on = " AND ".join(f"t.{k} = s.{k}" for k in spec["keys"])
    if spec["partition_by"]:                         # 只扫涉及到的年份分区
        years = sorted(set(source["year"].to_pylist()))
        on += f" AND t.year IN ({', '.join(map(str, years))}) AND t.year = s.year"
    m = (dt.merge(source, on, source_alias="s", target_alias="t", commit_properties=props)
         .when_matched_update_all(predicate="s.version > t.version")
         .when_not_matched_insert_all()
         .execute())
    return {"inserted": m.get("num_target_rows_inserted", 0), "updated": m.get("num_target_rows_updated", 0)}


def only_newer(dt: DeltaTable | None, source: pa.Table, keys: list[str], partition_by: list[str] | None) -> pa.Table:
    """去掉湖里已经有、且版本不比它旧的行：边界上重复取到的行、重跑时的行都会被去掉"""
    if dt is None or source.num_rows == 0:
        return source
    con = duckdb.connect()
    con.register("s", source)
    con.register("t", dt.to_pyarrow_dataset())
    scope = ""
    if partition_by:                                 # 只读涉及到的年份分区
        scope = f"WHERE year IN ({', '.join(map(str, sorted(set(source['year'].to_pylist()))))})"
    on = " AND ".join(f"s.{k} = t.{k}" for k in keys)
    return con.execute(f"""
        SELECT s.* FROM s LEFT JOIN (SELECT {", ".join(keys)}, version FROM t {scope}) t ON {on}
        WHERE t.version IS NULL OR s.version > t.version""").to_arrow_table().cast(source.schema)


def export_versioned(name: str, *, ch: ClickHouseStore, db: str = GOLDEN_DB, base: str | None = None) -> dict:
    spec = TABLES[name]
    dt = open_table(name, base)
    since = watermark(dt)
    source = (changed_prices if name == "price_daily" else changed_adjustments)(ch, since, db)
    source = only_newer(dt, source, spec["keys"], spec["partition_by"])
    if source.num_rows == 0:                         # 没有变化不提交，不产生空版本
        return {"rows": 0}
    new_wm = pc.max(source["loaded_at"]).as_py()
    out = {"rows": source.num_rows, **_merge(name, source, new_wm, base)}
    log.info(f"[lakehouse] {name}：{out}（水位线 {since:%Y-%m-%d %H:%M:%S} → {new_wm:%Y-%m-%d %H:%M:%S}）")
    return out


def export_securities(*, ch: ClickHouseStore, db: str = GOLDEN_DB, base: str | None = None) -> dict:
    schema = TABLES["securities"]["schema"]
    source = _arrow(ch, f"SELECT * FROM {db}.securities ORDER BY security_id", {}, schema)
    dt = open_table("securities", base)
    if dt is not None and dt.to_pyarrow_table().sort_by("security_id").equals(source):
        return {"rows": source.num_rows, "changed": False}
    write_deltalake(table_uri("securities", base), source, mode="overwrite", storage_options=storage_options(),
                    commit_properties=CommitProperties(custom_metadata={"source": f"{db}.securities"}))
    return {"rows": source.num_rows, "changed": True}


# =========================
# 核对：湖 vs golden 当前版本
# =========================
def verify(*, ch: ClickHouseStore, db: str = GOLDEN_DB, base: str | None = None) -> list[dict]:
    """行数、版本号之和、收盘价 / 因子之和；不一致返回提醒项（交给 alerts 的 summary['checks']）"""
    queries = {
        "price_daily": (
            f"SELECT count(), sum(version), round(sum(ifNull(close, 0)), 2) FROM {db}.v_price_daily",
            "SELECT count(*), sum(version), round(sum(coalesce(close, 0)), 2) FROM t"),
        "adjustment_events": (
            f"""SELECT count(), sum(v), round(sum(pf), 6) FROM (SELECT max(version) AS v,
                    argMax(price_factor, version) AS pf FROM {db}.adjustment_events
                    GROUP BY security_id, ex_date, kind, event_id)""",
            "SELECT count(*), sum(version), round(sum(price_factor), 6) FROM t"),
    }
    items = []
    for name, (ch_sql, lake_sql) in queries.items():
        dt = open_table(name, base)
        expected = tuple(ch.client.query(ch_sql).result_rows[0])
        if dt is None:
            got = (0, 0, 0.0)
        else:
            con = duckdb.connect()
            con.register("t", dt.to_pyarrow_dataset())
            got = con.execute(lake_sql).fetchone()
        if tuple(int(x) if i < 2 else float(x) for i, x in enumerate(got)) != \
                tuple(int(x) if i < 2 else float(x) for i, x in enumerate(expected)):
            items.append({"code": "lakehouse_mismatch",
                          "message": f"湖仓 {name} 和 golden 不一致：golden {expected}，湖 {got}（行数, 版本和, 数值和）"})
    return items


# =========================
# 维护
# =========================
def maintain(name: str, base: str | None = None, now: datetime | None = None, force: bool = False) -> dict | None:
    """上次合并小文件超过 7 天才做：合并小文件 → 检查点 → 清理 30 天前已不用的文件"""
    dt = open_table(name, base)
    if dt is None:
        return None
    now = now or datetime.now(timezone.utc)
    last = next((c["timestamp"] for c in dt.history() if c.get("operation") == "OPTIMIZE"), None)
    created = dt.history()[-1]["timestamp"]
    if not force and now - datetime.fromtimestamp((last or created) / 1000, timezone.utc) < MAINTAIN_EVERY:
        return None
    compact = dt.optimize.compact()
    dt.create_checkpoint()
    removed = dt.vacuum(retention_hours=int(RETENTION.total_seconds() // 3600), dry_run=False)
    out = {"files_added": compact.get("numFilesAdded"), "files_removed": compact.get("numFilesRemoved"),
           "vacuumed": len(removed)}
    log.info(f"[lakehouse] 维护 {name}：{out}")
    return out


def export_all(*, ch: ClickHouseStore | None = None, db: str = GOLDEN_DB, base: str | None = None) -> dict:
    ch = ch or ClickHouseStore()
    out = {name: export_versioned(name, ch=ch, db=db, base=base) for name in ("price_daily", "adjustment_events")}
    out["securities"] = export_securities(ch=ch, db=db, base=base)
    out["maintenance"] = {n: m for n in TABLES if (m := maintain(n, base))}
    out["checks"] = verify(ch=ch, db=db, base=base)
    return out


# =========================
# 查询（演示 / 研究用）
# =========================
def connect(version: dict | None = None, base: str | None = None) -> duckdb.DuckDBPyConnection:
    """
    DuckDB 连接，湖里的表（golden 导出的三张 + 分钟线 bars_5min / bars）注册成同名视图；version={"price_daily": 3} 可以回看某个版本（时间旅行）
        con = connect(); con.sql("SELECT * FROM price_daily WHERE security_id = 1 ORDER BY date DESC LIMIT 5")
    """
    con = duckdb.connect()
    for name in [*TABLES, *OTHER_TABLES]:
        dt = open_table(name, base, (version or {}).get(name))
        if dt is not None:
            con.register(name, dt.to_pyarrow_dataset())
    return con


def main():
    parser = argparse.ArgumentParser(description="golden → 湖仓（Delta Lake）")
    parser.add_argument("command", choices=["export", "verify", "maintain", "history"])
    parser.add_argument("--table", default="price_daily")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "export":
        print(export_all())
    elif args.command == "verify":
        print(verify(ch=ClickHouseStore()) or "一致")
    elif args.command == "maintain":
        print({n: maintain(n, force=True) for n in TABLES})
    else:
        for c in open_table(args.table).history():
            print(c.get("version"), datetime.fromtimestamp(c["timestamp"] / 1000), c.get("operation"),
                  c.get(WATERMARK_KEY, ""), c.get("operationMetrics", {}).get("num_target_rows_inserted", ""))


if __name__ == "__main__":
    main()
