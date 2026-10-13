"""
全量补数据：按证券主表的股票名单，把 FMP 各接口的历史拉进 raw（可选：每批写完接着清洗）

股票名单：证券主表里每只证券最后使用的代码（FMP 按"现在的代码"给历史）。跳过两种：
    · 这个代码现在被主表里另一只证券使用（如拆出来的老 DOW）——拉回来的是那只证券的数据
    · 这个代码现在被主表以外的公司使用（FMP profile 仍在交易、主表里现在没人用，如 BBBY）——拉回来的是别家的数据

分批：每 chunk_size 只股票调用一次 raw_loader.ingest()，一个 batch_id
断点续跑：同一个任务（接口 + 参数相同）里，已经写进 raw 的股票、确认没有数据的股票自动跳过；失败的下次重跑
    日线 / 市值的 to_date 默认是今天：隔天重跑参数变了，会从头拉。要接着前一天的进度，用 --to-date 指定同一天

命令行：
    python -m pipelines.backfill --dry-run                     # 只看名单和计划
    python -m pipelines.backfill --max-symbols 5 --clean       # 小规模试跑
    python -m pipelines.backfill --clean                       # 全部任务
    python -m pipelines.backfill --jobs price income_annual    # 只跑指定任务
"""
import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date

from config.endpoints import FMP_ENDPOINTS
from db import ClickHouseStore, MetaStore
from pipelines.raw_loader import SchemaError, ingest
from pipelines.security_master import CACHE_PATH

log = logging.getLogger(__name__)

HISTORY_START = date(2010, 1, 1)
ANNUAL_LIMIT = 17                      # 2010 年以来的年报期数（多一两期没关系，清洗 / golden 再按日期截）
QUARTER_LIMIT = 68                     # 2010 年以来的季报期数
DEFAULT_CHUNK = 50


def jobs(to_date: date) -> dict[str, tuple[str, dict | None]]:
    """任务名 → (接口, 参数)"""
    dated = {"from_date": HISTORY_START.isoformat(), "to_date": to_date.isoformat()}
    out = {"price": ("price_volume", dated)}
    for name, ep in [("income", "income_statement"), ("balance", "balance_sheet"), ("cash", "cash_flow")]:
        out[f"{name}_annual"] = (ep, {"period": "annual", "limit": ANNUAL_LIMIT})
        out[f"{name}_quarter"] = (ep, {"period": "quarter", "limit": QUARTER_LIMIT})
    out.update({
        "ratio": ("financial_ratio", {"limit": ANNUAL_LIMIT}),
        "enterprise_value": ("enterprise_values", {"limit": ANNUAL_LIMIT}),
        "market_cap": ("market_cap", dated),
        "estimate": ("financial_estimate", {"page": 0, "limit": 10}),
        "grades": ("analyst_grades", None),
        "profile": ("profile", None),
        "exec_comp": ("executive_compensation", None),
    })
    return out


# =========================
# 股票名单
# =========================
@dataclass
class Universe:
    symbols: list[str]
    skipped: dict[str, str] = field(default_factory=dict)       # 代码 → 跳过原因


def build_universe(conn, today: date | None = None, reused: set[str] | None = None) -> Universe:
    """
    每只证券最后使用的代码；跳过代码现在被别的证券 / 主表以外的公司使用的
    reused：代码现在被主表以外的公司使用的集合；不传时从证券主表生成时的 profile 缓存推断
    """
    today = today or date.today()
    rows = conn.execute("""
        SELECT DISTINCT ON (i.security_id) i.security_id, i.id_value,
               ref.resolve_security('symbol', i.id_value, %s) AS holder_now
        FROM ref.security_identifiers i
        WHERE i.id_type = 'symbol'
        ORDER BY i.security_id, i.valid_from DESC""", (today,)).fetchall()
    if reused is None:
        reused = _reused_from_cache()
    symbols, skipped = [], {}
    for sid, sym, holder in rows:
        if holder is not None and holder != sid:
            skipped[sym] = f"代码现在属于证券 {holder}"
        elif holder is None and sym in reused:
            skipped[sym] = "代码现在被主表以外的公司使用"
        else:
            symbols.append(sym)
    return Universe(sorted(set(symbols)), skipped)


def _reused_from_cache() -> set[str]:
    try:
        profiles = json.loads(CACHE_PATH.read_text(encoding="utf-8"))["profiles"]
    except (OSError, ValueError, KeyError):
        log.warning(f"读不到 {CACHE_PATH}，无法识别被主表以外的公司重复使用的代码")
        return set()
    return {s for s, p in profiles.items() if p.get("isActivelyTrading")}


# =========================
# 断点续跑
# =========================
def done_symbols(store: MetaStore, ch: ClickHouseStore, endpoint: str, params: dict | None) -> set[str]:
    """同一接口、同样参数的批次里，已经写进 raw 的股票 + 确认没有数据的股票"""
    rows = store.conn.execute("""
        SELECT i.batch_id, i.missing_symbols
        FROM meta.ingestion i
        JOIN meta.load_log l ON l.batch_id = i.batch_id AND l.layer = 'raw' AND l.status = 'success'
        WHERE i.endpoint = %s AND i.params = %s::jsonb""",
        (endpoint, json.dumps(params or {}))).fetchall()
    if not rows:
        return set()
    done = {s for _, missing in rows for s in (missing or [])}
    table = FMP_ENDPOINTS[endpoint]["raw_table"]
    written = ch.query_df(f"SELECT DISTINCT symbol FROM {table} WHERE batch_id IN {{ids:Array(String)}}",
                          {"ids": [b for b, _ in rows]})
    return done | set(written["symbol"])


# =========================
# 执行
# =========================
@dataclass
class JobResult:
    job: str
    todo: int
    batches: list[str] = field(default_factory=list)
    failed_batches: list[str] = field(default_factory=list)
    cleaned: int = 0
    clean_errors: list[str] = field(default_factory=list)


def run_job(name: str, endpoint: str, params: dict | None, symbols: list[str], *, store: MetaStore,
            ch: ClickHouseStore, chunk_size: int = DEFAULT_CHUNK, clean: bool = False) -> JobResult:
    done = done_symbols(store, ch, endpoint, params)
    todo = [s for s in symbols if s not in done]
    result = JobResult(name, len(todo))
    log.info(f"[{name}] {endpoint} {params}：共 {len(symbols)} 只，已完成 {len(symbols) - len(todo)}，待拉 {len(todo)}")
    for i in range(0, len(todo), chunk_size):
        chunk = todo[i:i + chunk_size]
        try:
            batch_id = ingest(endpoint, chunk, params, store=store, ch=ch)
        except SchemaError as e:
            log.error(f"[{name}] 第 {i // chunk_size + 1} 批结构检查不通过：{e}")
            result.failed_batches.append(str(e)[:200])
            continue
        result.batches.append(batch_id)
        if clean:
            try:
                from pipelines.clean_runner import run_clean
                run_clean(batch_id, store=store, ch=ch)
                result.cleaned += 1
            except Exception as e:                     # 清洗失败不影响继续拉取，批次可以单独重跑清洗
                log.error(f"[{name}] 批次 {batch_id} 清洗失败：{type(e).__name__}: {e}")
                result.clean_errors.append(batch_id)
        log.info(f"[{name}] 进度 {min(i + chunk_size, len(todo))}/{len(todo)}")
    return result


def main():
    all_jobs_preview = list(jobs(date.today()))
    parser = argparse.ArgumentParser(description="全量补数据（FMP → raw，可选清洗）")
    parser.add_argument("--jobs", nargs="*", choices=all_jobs_preview, help="只跑这些任务（默认全部）")
    parser.add_argument("--to-date", type=date.fromisoformat, default=date.today(), help="日线 / 市值的截止日（默认今天）")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK, help="每批股票数")
    parser.add_argument("--max-symbols", type=int, help="只取名单前 N 只（试跑用）")
    parser.add_argument("--symbols", nargs="*", help="指定股票（不用证券主表名单）")
    parser.add_argument("--clean", action="store_true", help="每批写完 raw 接着清洗")
    parser.add_argument("--dry-run", action="store_true", help="只看名单和计划，不拉取")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("fetch_data.fetch").setLevel(logging.ERROR)
    logging.getLogger("db").setLevel(logging.WARNING)

    store, ch = MetaStore(), ClickHouseStore()
    if args.symbols:
        universe = Universe(sorted(set(args.symbols)))
    else:
        universe = build_universe(store.conn)
    symbols = universe.symbols[:args.max_symbols] if args.max_symbols else universe.symbols
    print(f"股票名单：{len(universe.symbols)} 只（跳过 {len(universe.skipped)} 只），本次 {len(symbols)} 只")
    for sym, why in sorted(universe.skipped.items()):
        print(f"  跳过 {sym}：{why}")

    plan = {k: v for k, v in jobs(args.to_date).items() if not args.jobs or k in args.jobs}
    if args.dry_run:
        for name, (ep, params) in plan.items():
            done = done_symbols(store, ch, ep, params)
            print(f"  {name:<17} {ep:<23} {params}  待拉 {sum(s not in done for s in symbols)}")
        return

    started = time.time()
    results = [run_job(name, ep, params, symbols, store=store, ch=ch, chunk_size=args.chunk, clean=args.clean)
               for name, (ep, params) in plan.items()]
    print(f"\n完成，用时 {(time.time() - started) / 60:.1f} 分钟")
    for r in results:
        print(f"  {r.job:<17} 待拉 {r.todo:>4}  批次 {len(r.batches):>3}  结构检查失败 {len(r.failed_batches)}"
              + (f"  清洗成功 {r.cleaned}  清洗失败 {len(r.clean_errors)}" if args.clean else ""))


if __name__ == "__main__":
    main()
