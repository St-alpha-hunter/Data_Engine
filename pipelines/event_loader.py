"""
事件导入：拉取 → 整理 → 写库

    拉取：FetchData（FMP）或直接下载（GitHub CSV），记一个 meta.ingestion 批次
    整理：fetch_data.events 的整理函数（纯函数）
    写库：db.event_store.EventStore（record_xxx 写事件；被拒的行记为质量问题）

每类事件一个入口：
    load_sp500_index_changes()          GitHub sp500_ticker_start_end.csv
    load_symbol_changes()               FMP symbol-change（全市场，一次请求）
    load_acquisitions(max_pages=)       FMP mergers-acquisitions-latest（按页翻，空页停止）
    load_dividends(symbols)             FMP dividends?symbol=（每只股票的全部历史）
    load_splits(symbols)                FMP splits?symbol=

重复导入是安全的：内容相同的事件判为 duplicate，不会重复写入。

按股票请求的（分红、拆股）分批写库：每拉完 chunk_size 只股票就整理、写一次库，
同一个 ingestion 批次；能看到进度，中途中断时已写入的部分保留，重跑只会把它们判为 duplicate。

命令行：
    python -m pipelines.event_loader sp500
    python -m pipelines.event_loader symbol_change
    python -m pipelines.event_loader acquisition
    python -m pipelines.event_loader dividend --universe sp500     # 股票池：2010 年以后进过 S&P 500 的代码
    python -m pipelines.event_loader split --symbols AAPL MSFT
"""
import argparse
import io
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from db.event_store import EventStore
from db.meta_store import MetaStore
from fetch_data.events import (
    normalize_acquisitions, normalize_dividends, normalize_sp500_index_changes,
    normalize_splits, normalize_symbol_changes,
)
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

SP500_START_END_URL = "https://raw.githubusercontent.com/fja05680/sp500/master/sp500_ticker_start_end.csv"
UNIVERSE_START = date(2010, 1, 1)
DEFAULT_CHUNK_SIZE = 100


@dataclass
class EventLoadResult:
    event_type: str
    batch_id: str
    rows_fetched: int
    normalize: dict          # NormalizeResult.summary()
    write: dict              # EventStore.load() 的统计


def _write(event_type, result, batch_id, rows_fetched, event_store) -> EventLoadResult:
    stats = event_store.load(event_type, result, batch_id)
    return EventLoadResult(event_type, batch_id, rows_fetched, result.summary(), stats)


def _fetch_fmp(meta, endpoint, units, params=None, fetcher_cls=FetchData):
    """在 ingestion_run 里拉取；返回 (run, rows)。run 退出后批次状态已写好"""
    with meta.ingestion_run(endpoint, units, params) as run:
        with fetcher_cls(units, endpoint, params) as fetcher:
            df, summary, succeed, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))
    rows = df.drop(columns=["endpoint"], errors="ignore").to_dict(orient="records") if not df.empty else []
    return run, rows


def _load_chunked(meta, event_store, endpoint, event_type, normalize, symbols, chunk_size, fetcher_cls):
    """按股票分批：每批 拉取 → 整理 → 写库，同一个 ingestion 批次；最后汇总各批统计"""
    n_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    fetch_totals, failed_all, missing_all = Counter(), [], []
    norm_totals = {"events": 0, "rejected": Counter(), "skipped": Counter()}
    write_totals = Counter()
    rows_total = 0

    with meta.ingestion_run(endpoint, symbols) as run:
        for i in range(n_chunks):
            chunk = symbols[i * chunk_size:(i + 1) * chunk_size]
            with fetcher_cls(chunk, endpoint, None) as fetcher:
                df, summary, succeed, failed = fetcher.fetch_fmp_batch()
            rows = df.drop(columns=["endpoint"], errors="ignore").to_dict(orient="records") if not df.empty else []

            result = normalize(rows)
            stats = event_store.load(event_type, result, run.batch_id)

            rows_total += len(rows)
            fetch_totals.update({k: summary.get(k, 0) for k in ("success", "missing", "failed")})
            failed_all += failed
            missing_all += summary.get("missing_symbols", [])
            norm_totals["events"] += len(result.events)
            norm_totals["rejected"].update(r["reason"] for r in result.rejected)
            norm_totals["skipped"].update(r["reason"] for r in result.skipped)
            write_totals.update(stats)
            log.info(f"[{event_type}] 第 {i + 1}/{n_chunks} 批（{len(chunk)} 只）：拉到 {len(rows)} 行，"
                     f"新增 {stats['inserted']}，重复 {stats['duplicate']}，被拒 {stats['rejected'] + stats['db_rejected']}")

        run.finish({**fetch_totals, "missing_symbols": missing_all}, failed_all, rows_fetched=rows_total)

    normalize_summary = {"event_type": event_type, "events": norm_totals["events"],
                         "rejected": dict(norm_totals["rejected"]), "skipped": dict(norm_totals["skipped"])}
    return EventLoadResult(event_type, run.batch_id, rows_total, normalize_summary, dict(write_totals))


# =========================
# 各类事件的导入入口
# =========================
def load_sp500_index_changes(*, meta=None, event_store=None, url=SP500_START_END_URL, csv_text=None) -> EventLoadResult:
    meta = meta or MetaStore()
    event_store = event_store or EventStore(meta.conn, meta)
    with meta.ingestion_run("sp500_github", ["SP500"], {"url": url}) as run:
        text = csv_text if csv_text is not None else requests.get(url, timeout=60).text
        rows = pd.read_csv(io.StringIO(text)).to_dict(orient="records")
        run.finish({"success": 1, "missing": 0, "failed": 0}, [], rows_fetched=len(rows))
    return _write("index_change", normalize_sp500_index_changes(rows), run.batch_id, len(rows), event_store)


def load_symbol_changes(*, meta=None, event_store=None, fetcher_cls=FetchData) -> EventLoadResult:
    meta = meta or MetaStore()
    event_store = event_store or EventStore(meta.conn, meta)
    run, rows = _fetch_fmp(meta, "symbol_change", ["ALL"], fetcher_cls=fetcher_cls)
    return _write("symbol_change", normalize_symbol_changes(rows), run.batch_id, len(rows), event_store)


def load_acquisitions(max_pages: int = 60, *, meta=None, event_store=None, fetcher_cls=FetchData) -> EventLoadResult:
    meta = meta or MetaStore()
    event_store = event_store or EventStore(meta.conn, meta)
    run, rows = _fetch_fmp(meta, "mergers_acquisitions", list(range(max_pages)), fetcher_cls=fetcher_cls)
    return _write("acquisition", normalize_acquisitions(rows), run.batch_id, len(rows), event_store)


def load_dividends(symbols: list[str], chunk_size: int = DEFAULT_CHUNK_SIZE, *, meta=None, event_store=None,
                   fetcher_cls=FetchData) -> EventLoadResult:
    meta = meta or MetaStore()
    event_store = event_store or EventStore(meta.conn, meta)
    return _load_chunked(meta, event_store, "dividends", "dividend", normalize_dividends,
                         list(symbols), chunk_size, fetcher_cls)


def load_splits(symbols: list[str], chunk_size: int = DEFAULT_CHUNK_SIZE, *, meta=None, event_store=None,
                fetcher_cls=FetchData) -> EventLoadResult:
    meta = meta or MetaStore()
    event_store = event_store or EventStore(meta.conn, meta)
    return _load_chunked(meta, event_store, "splits", "split", normalize_splits,
                         list(symbols), chunk_size, fetcher_cls)


def sp500_universe(since: date = UNIVERSE_START, url: str = SP500_START_END_URL) -> list[str]:
    """2010 年以后进过 S&P 500 的代码（社区数据集里的代码，可能是公司后来的代码）"""
    df = pd.read_csv(io.StringIO(requests.get(url, timeout=60).text))
    keep = df["end_date"].isna() | (pd.to_datetime(df["end_date"], errors="coerce") >= pd.Timestamp(since))
    return sorted(df.loc[keep, "ticker"].dropna().astype(str).str.strip().str.upper().unique())


def main():
    parser = argparse.ArgumentParser(description="导入公司事件：拉取 → 整理 → 写库")
    parser.add_argument("event", choices=["sp500", "symbol_change", "acquisition", "dividend", "split"])
    parser.add_argument("--symbols", nargs="*", help="分红 / 拆股：指定股票代码")
    parser.add_argument("--universe", choices=["sp500"], help="分红 / 拆股：用 2010 年以后进过 S&P 500 的代码")
    parser.add_argument("--max-pages", type=int, default=60, help="收购：最多翻多少页")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="分红 / 拆股：每批多少只股票写一次库")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("fetch_data.fetch").setLevel(logging.ERROR)     # 只看失败，不刷"没有数据"
    logging.getLogger("db").setLevel(logging.WARNING)

    if args.event == "sp500":
        r = load_sp500_index_changes()
    elif args.event == "symbol_change":
        r = load_symbol_changes()
    elif args.event == "acquisition":
        r = load_acquisitions(args.max_pages)
    else:
        symbols = sp500_universe() if args.universe == "sp500" else (args.symbols or [])
        if not symbols:
            parser.error("分红 / 拆股需要 --symbols 或 --universe sp500")
        r = (load_dividends if args.event == "dividend" else load_splits)(symbols, args.chunk_size)

    print(f"[{r.event_type}] 批次 {r.batch_id}：拉到 {r.rows_fetched} 行")
    print(f"  整理：{r.normalize}")
    print(f"  写库：{r.write}")


if __name__ == "__main__":
    main()
