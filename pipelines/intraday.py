"""
5 分钟线：FMP → 湖的 bronze 层（lakehouse/bars_5min，Delta）

Kafka + Spark 合成 demo 的数据源：
    ① 本模块：拉 5 分钟线 → bronze（只追加，和 ClickHouse raw 一样的语义）
    ② streaming/replay_producer.py：按时间顺序把 bronze 回放进 Kafka
    ③ spark_jobs/bars_stream.py：边消费边合成 15min / 1h / 1d → silver（lakehouse/bars）

批次照旧登记 meta.ingestion；写入记在 meta.load_log（layer = raw，target_table = lakehouse.bars_5min，location = 表地址）。
断点续跑：同样的参数里，已经写进 bronze 的股票、确认没数据的股票跳过。

时间：FMP 给的是纽约当地时间（K 线开始时刻）→ 存 UTC 时间戳 bar_start，另存纽约日期 trade_date（分区列）。

用法：
    python -m pipelines.intraday fetch --from 2026-10-01 --to 2026-10-17
"""
import argparse
import json
import logging
from datetime import date

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from config.endpoints import FMP_ENDPOINTS
from db import MetaStore
from fetch_data.fetch import FetchData
from pipelines import lakehouse
from pipelines.backfill import DEFAULT_CHUNK, build_universe, transient_failures

log = logging.getLogger(__name__)

ENDPOINT = "price_5min"
TABLE = FMP_ENDPOINTS[ENDPOINT]["bronze_table"]
NY = "America/New_York"

SCHEMA = pa.schema([
    ("security_id", pa.int64()), ("symbol", pa.string()),
    ("bar_start", pa.timestamp("us", tz="UTC")), ("trade_date", pa.date32()),
    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("volume", pa.float64()),
    ("batch_id", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
])


def to_bronze(df: pd.DataFrame, security_of: dict[str, int], batch_id: str) -> pa.Table:
    """FMP 返回的 DataFrame → bronze 的行；纽约当地时间 → UTC；同一根 K 线重复返回的只留一条"""
    local = pd.to_datetime(df["date"]).dt.tz_localize(NY)
    out = pd.DataFrame({
        "security_id": df["symbol"].map(security_of),
        "symbol": df["symbol"],
        "bar_start": local.dt.tz_convert("UTC"),
        "trade_date": local.dt.date,
        **{c: pd.to_numeric(df[c], errors="coerce") for c in ["open", "high", "low", "close", "volume"]},
        "batch_id": batch_id,
        "ingested_at": pd.Timestamp.now(tz="UTC"),
    })
    missing = out["security_id"].isna()
    if missing.any():                                # 证券主表里找不到的代码不写（不该发生：名单就是从主表来的）
        log.warning(f"[intraday] 证券主表里找不到：{sorted(out.loc[missing, 'symbol'].unique())[:10]}")
        out = out[~missing]
    out = out.drop_duplicates(["symbol", "bar_start"]).sort_values(["symbol", "bar_start"])
    return pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False)


def done_symbols(store: MetaStore, params: dict, base: str | None = None) -> set[str]:
    """同样参数的成功批次里：已写进 bronze 的 + 确认没数据的"""
    rows = store.conn.execute("""
        SELECT i.batch_id, i.missing_symbols FROM meta.ingestion i
        JOIN meta.load_log l ON l.batch_id = i.batch_id AND l.status = 'success' AND l.target_table = %s
        WHERE i.endpoint = %s AND i.params = %s::jsonb""",
        (f"lakehouse.{TABLE}", ENDPOINT, json.dumps(params))).fetchall()
    done = {s for _, missing in rows for s in (missing or [])}
    dt = lakehouse.open_table(TABLE, base)
    if rows and dt is not None:
        con = duckdb.connect()
        con.register("b", dt.to_pyarrow_dataset())
        done |= {r[0] for r in con.execute("SELECT DISTINCT symbol FROM b WHERE batch_id IN (SELECT unnest(?))",
                                           [[b for b, _ in rows]]).fetchall()}
    return done


def ingest_chunk(symbols: list[str], params: dict, security_of: dict[str, int], *, store: MetaStore,
                 base: str | None = None, fetcher_cls=FetchData) -> str:
    uri = lakehouse.table_uri(TABLE, base)
    with store.ingestion_run(ENDPOINT, symbols, params) as run:
        with fetcher_cls(symbols, ENDPOINT, params) as fetcher:
            df, summary, _, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))
        with store.load_step(run.batch_id, "raw", f"lakehouse.{TABLE}", location=uri) as step:
            if not df.empty:
                rows = to_bronze(df, security_of, run.batch_id)
                write_deltalake(uri, rows, mode="append", partition_by=["trade_date"],
                                storage_options=lakehouse.storage_options())
                step.rows = rows.num_rows
    return run.batch_id


def fetch(from_date: date, to_date: date, *, store: MetaStore | None = None, symbols: list[str] | None = None,
          base: str | None = None, chunk_size: int = DEFAULT_CHUNK, fetcher_cls=FetchData) -> dict:
    """股票池（证券主表里有数据的，含期间退市的）在 [from_date, to_date] 的 5 分钟线 → bronze"""
    store = store or MetaStore()
    universe = build_universe(store.conn, today=to_date)
    symbols = symbols or sorted(universe.symbols)
    params = {"from_date": from_date.isoformat(), "to_date": to_date.isoformat()}
    todo = [s for s in symbols if s not in done_symbols(store, params, base)]
    log.info(f"[intraday] {params}：共 {len(symbols)} 只，待拉 {len(todo)}")
    batches, transient = [], []
    for i in range(0, len(todo), chunk_size):
        b = ingest_chunk(todo[i:i + chunk_size], params, universe.security_of, store=store, base=base,
                         fetcher_cls=fetcher_cls)
        batches.append(b)
        transient += transient_failures(store, b)
        log.info(f"[intraday] 进度 {min(i + chunk_size, len(todo))}/{len(todo)}")
    out = {"symbols": len(symbols), "todo": len(todo), "batches": len(batches), "transient_failures": transient}
    if transient:                                    # 重跑时断点续跑只补拉这些
        raise RuntimeError(f"[intraday] {len(transient)} 只暂时性拉取失败，重跑会补拉：{transient[:10]}")
    return out


def fetch_day(trade_date: date, *, store: MetaStore | None = None, base: str | None = None) -> dict:
    """每日流程用：前一交易日的 5 分钟线；名单和日线一样（仍在交易的 + 回看窗口内退市的，早就退市的不拉）"""
    from pipelines.daily import daily_symbols
    store = store or MetaStore()
    return fetch(trade_date, trade_date, store=store, symbols=daily_symbols(store.conn, trade_date), base=base)


def main():
    parser = argparse.ArgumentParser(description="5 分钟线 → 湖的 bronze 层")
    parser.add_argument("command", choices=["fetch"])
    parser.add_argument("--from", dest="from_date", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="to_date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("db").setLevel(logging.WARNING)
    print(fetch(args.from_date, args.to_date))


if __name__ == "__main__":
    main()
