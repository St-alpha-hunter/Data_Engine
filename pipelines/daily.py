"""
每日增量：Airflow 每天调用（也可以手动跑）

日期一律由调用方传入，不用 date.today()：
    Airflow 的执行时间（北京时间早上 8 点）→ 换成纽约时间的日期 = 要处理的美股交易日（trade_date）
    不是交易日（周末、美股假日）→ 整个流程跳过
所以补跑过去某一天，和当天跑的结果一样（幂等）；每一步都可以重复执行：
    拉取：同一个 (接口, 参数) 已经写进 raw 的股票自动跳过（参数里带 trade_date，所以每天是新的任务）
    写库：事件按 (来源, 事件键, 内容) 去重；证券主表维护、回填 security_id、golden 都是有变化才写

步骤（每日 DAG 按这个顺序）：
    1. load_events          代码变更、S&P 500 成分变更、收购（最近几页）、分红 / 拆股日历（过去 7 天到未来 14 天）
    2. maintain_securities  证券主表增量维护（改名、新调入、退市）；新调入的证券补全历史
    3. backfill_security_id 新事件回填 security_id
    4. ingest_prices        日线：不复权（golden 用）+ 复权（核对用），只拉 [trade_date − 7 天, trade_date]，拉完清洗
    5. load_golden          还没写进 golden 的批次 → golden；重算复权因子；和 FMP 核对
    6. summarize            当天的批次、失败、问题、隔离统计

命令行（不经过 Airflow 手动跑某一天）：
    python -m pipelines.daily --date 2026-10-13
    python -m pipelines.daily --date 2026-10-13 --steps ingest_prices load_golden
"""
import argparse
import json
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from db import ClickHouseStore, MetaStore
from pipelines import event_loader, event_security_backfill, golden_loader, security_maintenance
from pipelines.backfill import HISTORY_START, build_universe, run_job

log = logging.getLogger(__name__)

NY = ZoneInfo("America/New_York")
LOOKBACK_DAYS = 7              # 日线多拉前 7 个自然日：补前几天失败、或 FMP 延迟更新的数据
CALENDAR_BACK, CALENDAR_AHEAD = 7, 14  # 分红 / 拆股日历：回看 7 天（补漏）、往前看 14 天（提前公告的）
ACQUISITION_PAGES = 3          # 收购：只翻最新的 3 页（每页 100 条，按时间倒序）
PRICE_JOBS = {"price_unadjusted": "price_volume_unadjusted",   # golden 主数据
              "price": "price_volume"}                         # FMP 复权价，只用来核对


# =========================
# 日期
# =========================
def trade_date_for(logical: datetime, conn) -> date | None:
    """
    执行时间 → 要处理的美股交易日：换成纽约时间取日期；那天不是交易日返回 None（整个流程跳过）
    北京时间早上 8 点 = 纽约前一天晚上 8 点（夏令时）/ 7 点（冬令时），都在收盘之后
    """
    if logical.tzinfo is None:
        raise ValueError("执行时间必须带时区")
    ny_date = logical.astimezone(NY).date()
    row = conn.execute("SELECT is_trading_day FROM ref.trading_calendar WHERE exchange = 'XNYS' AND date = %s",
                       (ny_date,)).fetchone()
    if row is None:
        raise RuntimeError(f"交易日历里没有 {ny_date}，先运行 python -m pipelines.trading_calendar 更新日历")
    return ny_date if row[0] else None


# =========================
# 1. 事件
# =========================
def load_events(trade_date: date, *, store: MetaStore | None = None) -> dict:
    store = store or MetaStore()
    out = {}
    for name, fn in [
        ("symbol_change", lambda: event_loader.load_symbol_changes(meta=store)),
        ("sp500", lambda: event_loader.load_sp500_index_changes(meta=store)),
        ("acquisition", lambda: event_loader.load_acquisitions(ACQUISITION_PAGES, meta=store)),
        ("dividends_calendar", lambda: event_loader.load_dividends_calendar(
            trade_date - timedelta(days=CALENDAR_BACK), trade_date + timedelta(days=CALENDAR_AHEAD), meta=store)),
        ("splits_calendar", lambda: event_loader.load_splits_calendar(
            trade_date - timedelta(days=CALENDAR_BACK), trade_date + timedelta(days=CALENDAR_AHEAD), meta=store)),
    ]:
        r = fn()
        out[name] = {"batch_id": r.batch_id, "rows": r.rows_fetched, **r.write}
        log.info(f"[daily] 事件 {name}：{out[name]}")
    return out


# =========================
# 2. 证券主表 + 新证券补历史
# =========================
def maintain_securities(trade_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    actions = security_maintenance.run(meta=store, apply=True, today=trade_date)
    new_symbols = [a.symbol for a in actions if a.kind == "new_security"]     # 调入事件里的代码（现在的代码）
    out = {"actions": {k: sum(a.kind == k for a in actions) for k in ("rename", "new_security", "delist", "skip")},
           "new_symbols": new_symbols}
    if new_symbols:
        out["backfill"] = backfill_new_securities(new_symbols, trade_date, store=store, ch=ch)
    log.info(f"[daily] 证券主表：{out}")
    return out


def backfill_new_securities(symbols: list[str], trade_date: date, *, store: MetaStore, ch: ClickHouseStore) -> dict:
    """新调入的证券：补 2010 年以来的日线（两种）和分红 / 拆股历史（日历只管最近几天）"""
    params = {"from_date": HISTORY_START.isoformat(), "to_date": trade_date.isoformat()}
    out = {name: len(run_job(name, ep, params, symbols, store=store, ch=ch, clean=True).batches)
           for name, ep in PRICE_JOBS.items()}
    out["dividends"] = event_loader.load_dividends(symbols, meta=store).write
    out["splits"] = event_loader.load_splits(symbols, meta=store).write
    return out


# =========================
# 3. 回填 security_id
# =========================
def backfill_security_id(trade_date: date, *, store: MetaStore | None = None) -> dict:
    store = store or MetaStore()
    result, acquirers = event_security_backfill.compute(store.conn, today=trade_date)
    written = event_security_backfill.apply(store.conn, result, acquirers)
    log.info(f"[daily] 回填 security_id：{written}")
    return written


# =========================
# 4. 日线
# =========================
def daily_symbols(conn, trade_date: date) -> list[str]:
    """要拉日线的代码：补数据名单里，仍在交易、或回看窗口内才退市的（早就退市的不再每天拉）"""
    universe = build_universe(conn, today=trade_date)
    keep = {r[0] for r in conn.execute(
        "SELECT security_id FROM ref.securities WHERE status = 'active' OR last_trade_date >= %s",
        (trade_date - timedelta(days=LOOKBACK_DAYS),)).fetchall()}
    return sorted(sym for sym, sid in universe.security_of.items() if sid in keep)


def ingest_prices(trade_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    symbols = daily_symbols(store.conn, trade_date)
    params = {"from_date": (trade_date - timedelta(days=LOOKBACK_DAYS)).isoformat(), "to_date": trade_date.isoformat()}
    out = {"symbols": len(symbols)}
    for name, ep in PRICE_JOBS.items():
        r = run_job(name, ep, params, symbols, store=store, ch=ch, clean=True)
        out[name] = {"todo": r.todo, "batches": r.batches, "failed_batches": len(r.failed_batches),
                     "clean_errors": r.clean_errors}
        if r.failed_batches or r.clean_errors:
            raise RuntimeError(f"[daily] {name} 有批次失败：结构检查 {r.failed_batches}，清洗 {r.clean_errors}")
    log.info(f"[daily] 日线：{out}")
    return out


# =========================
# 5. golden
# =========================
def load_golden(trade_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    out = golden_loader.load_all(store=store, ch=ch, today=trade_date)
    log.info(f"[daily] golden：{out}")
    return out


# =========================
# 6. 汇总
# =========================
def summarize(trade_date: date, since: datetime, *, store: MetaStore | None = None) -> dict:
    """since 之后（这次运行开始以来）的批次、问题、隔离；新增事件修订数用来监控 FMP 字段是否每天在变"""
    store = store or MetaStore()
    q = lambda sql, *a: store.conn.execute(sql, a).fetchall()
    out = {
        "trade_date": str(trade_date),
        "batches": dict(q("SELECT status, count(*) FROM meta.ingestion WHERE started_at >= %s GROUP BY 1", since)),
        "issues": dict(q("""SELECT q.severity, count(*) FROM meta.quality_issues q JOIN meta.ingestion i USING (batch_id)
                            WHERE i.started_at >= %s GROUP BY 1""", since)),
        "gd001_total": q("SELECT count(*) FROM meta.quality_issues WHERE rule_id = 'GD001'")[0][0],
        "quarantine_pending": q("SELECT count(*) FROM meta.quarantine_records WHERE status = 'pending'")[0][0],
        "event_revisions": q("SELECT count(*) FROM events.corporate_events WHERE revision > 1 AND known_at >= %s",
                             since)[0][0],
    }
    log.info(f"[daily] 汇总：{json.dumps(out, ensure_ascii=False)}")
    return out


STEPS = ["load_events", "maintain_securities", "backfill_security_id", "ingest_prices", "load_golden"]


def run(trade_date: date, steps: list[str] | None = None) -> dict:
    """不经过 Airflow 手动跑某一天（按顺序执行各步）"""
    started = datetime.now(tz=NY)
    store, ch = MetaStore(), ClickHouseStore()
    out = {}
    for step in steps or STEPS:
        fn = globals()[step]
        out[step] = fn(trade_date, store=store) if step in ("load_events", "backfill_security_id") \
            else fn(trade_date, store=store, ch=ch)
    out["summary"] = summarize(trade_date, started, store=store)
    return out


def main():
    parser = argparse.ArgumentParser(description="每日增量（手动跑某一天；平时由 Airflow 调用）")
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="美股交易日（纽约日期）")
    parser.add_argument("--steps", nargs="*", choices=STEPS, help="只跑这些步骤（默认全部，按顺序）")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("fetch_data.fetch").setLevel(logging.ERROR)
    logging.getLogger("db").setLevel(logging.WARNING)
    store = MetaStore()
    row = store.conn.execute("SELECT is_trading_day FROM ref.trading_calendar WHERE exchange = 'XNYS' AND date = %s",
                             (args.date,)).fetchone()
    if not row or not row[0]:
        parser.error(f"{args.date} 不是美股交易日")
    print(json.dumps(run(args.date, args.steps), ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
