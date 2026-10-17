"""
每周 / 每月流程（Airflow 调用；日期由调用方传入，不用 date.today()）

每周（周六）：报表、比率、企业价值、市值、一致预期、评级 → raw → 清洗
    财报一季度一份，每周拉最近几期就够：既能拿到新发布的，也能发现最近几期被重述（raw 只追加，和上次比就知道）
每月（1 号）：公司概况、和 SEC 代码表核对证券主表、更新交易日历

参数里的 as_of（本次运行日期）不出现在请求 URL 里，只用来区分"哪一次拉取"：
断点续跑按 (接口, 参数) 判断哪些股票已经拉过，报表的 period / limit 每周都一样，不加 as_of 第二周就会全部跳过；
同一次运行里重试时 as_of 相同，照样能接着上次的进度
"""
import csv
import logging
from datetime import date, timedelta

from config.paths import DATA_DIR
from db import ClickHouseStore, MetaStore
from pipelines import trading_calendar
from pipelines.backfill import run_job
from pipelines.daily import daily_symbols
from pipelines.security_master import fetch_sec_ciks

log = logging.getLogger(__name__)

REPORT_DIR = DATA_DIR / "reports"
RECENT_ANNUAL, RECENT_QUARTER = 3, 8        # 每周拉最近 3 个年报、8 个季报（新发布的 + 可能被重述的）
MARKET_CAP_DAYS = 14                        # 市值每周补最近两周（日线流程不拉市值）

# 任务名 → (接口, 参数)；as_of 在运行时加
WEEKLY_JOBS = {
    "income_annual": ("income_statement", {"period": "annual", "limit": RECENT_ANNUAL}),
    "income_quarter": ("income_statement", {"period": "quarter", "limit": RECENT_QUARTER}),
    "balance_annual": ("balance_sheet", {"period": "annual", "limit": RECENT_ANNUAL}),
    "balance_quarter": ("balance_sheet", {"period": "quarter", "limit": RECENT_QUARTER}),
    "cash_annual": ("cash_flow", {"period": "annual", "limit": RECENT_ANNUAL}),
    "cash_quarter": ("cash_flow", {"period": "quarter", "limit": RECENT_QUARTER}),
    "ratio": ("financial_ratio", {"limit": RECENT_ANNUAL}),
    "enterprise_value": ("enterprise_values", {"limit": RECENT_ANNUAL}),
    "market_cap": ("market_cap", None),                         # 日期区间在运行时算
    "estimate": ("financial_estimate", {"page": 0, "limit": 10}),
    "grades": ("analyst_grades", {}),
}


def _ingest(name: str, endpoint: str, params: dict, run_date: date, store: MetaStore, ch: ClickHouseStore) -> dict:
    symbols = daily_symbols(store.conn, run_date)
    r = run_job(name, endpoint, params, symbols, store=store, ch=ch, clean=True)
    out = {"symbols": len(symbols), "todo": r.todo, "batches": len(r.batches),
           "failed_batches": len(r.failed_batches), "clean_errors": r.clean_errors}
    log.info(f"[periodic] {name}：{out}")
    if r.failed_batches or r.clean_errors:
        raise RuntimeError(f"[periodic] {name} 有批次失败：结构检查 {r.failed_batches}，清洗 {r.clean_errors}")
    return out


# =========================
# 每周
# =========================
def weekly_job(name: str, run_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    endpoint, base = WEEKLY_JOBS[name]
    if name == "market_cap":
        params = {"from_date": (run_date - timedelta(days=MARKET_CAP_DAYS)).isoformat(), "to_date": run_date.isoformat()}
    else:
        params = {**base, "as_of": run_date.isoformat()}
    return _ingest(name, endpoint, params, run_date, store, ch)


# =========================
# 每月
# =========================
def monthly_profiles(run_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    return _ingest("profile", "profile", {"as_of": run_date.isoformat()}, run_date, store, ch)


def check_against_sec(run_date: date, *, store: MetaStore | None = None, sec_ciks: dict | None = None) -> dict:
    """
    证券主表里仍在交易的证券 vs SEC 官方代码表：
        代码不在 SEC 表里 → 可能已退市 / 改名，主表维护没跟上
        CIK 不一致        → 公司重组、或主表的 CIK 有误
    结果写 data/reports/sec_check_<日期>.csv；返回统计和提醒用的检查项
    """
    store = store or MetaStore()
    sec = sec_ciks if sec_ciks is not None else fetch_sec_ciks()
    rows = store.conn.execute("""
        SELECT s.security_id, s.name, sym.id_value AS symbol, cik.id_value AS cik
        FROM ref.securities s
        JOIN ref.security_identifiers sym ON sym.security_id = s.security_id AND sym.id_type = 'symbol'
             AND sym.valid_from <= %s AND (sym.valid_to IS NULL OR sym.valid_to > %s)
        LEFT JOIN ref.security_identifiers cik ON cik.security_id = s.security_id AND cik.id_type = 'cik'
             AND cik.valid_from <= %s AND (cik.valid_to IS NULL OR cik.valid_to > %s)
        WHERE s.status = 'active'
        ORDER BY sym.id_value""", (run_date,) * 4).fetchall()
    findings = []
    for sid, name, symbol, cik in rows:
        if symbol not in sec:
            findings.append((sid, symbol, name, cik, None, "SEC 代码表里没有这个代码：可能已退市或改名"))
        elif cik and sec[symbol] and cik != sec[symbol]:
            findings.append((sid, symbol, name, cik, sec[symbol], "CIK 和 SEC 不一致"))
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"sec_check_{run_date.isoformat()}.csv"
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["security_id", "symbol", "name", "cik_master", "cik_sec", "finding"])
        w.writerows(findings)
    missing = sum(f[4] is None for f in findings)
    out = {"active": len(rows), "not_in_sec": missing, "cik_mismatch": len(findings) - missing, "report": str(path)}
    log.info(f"[periodic] SEC 核对：{out}")
    return out


def update_trading_calendar(run_date: date, *, store: MetaStore | None = None) -> dict:
    """交易日历延伸到 run_date 之后两年底，并和 FMP 节假日交叉核对"""
    store = store or MetaStore()
    r = trading_calendar.run(store=store, end=trading_calendar.default_end(run_date))
    out = {"range": r["range"], "exchanges": r["exchanges"], "fmp_issues": len(r.get("issues", []))}
    log.info(f"[periodic] 交易日历：{out}")
    return out


def monthly_checks(sec: dict, calendar: dict) -> list[dict]:
    """月度结果里需要提醒的项（交给 alerts.check 的 summary['checks']）"""
    items = []
    if sec.get("not_in_sec") or sec.get("cik_mismatch"):
        items.append({"code": "sec_mismatch",
                      "message": f"证券主表和 SEC 代码表不一致：{sec['not_in_sec']} 只代码 SEC 里没有、"
                                 f"{sec['cik_mismatch']} 只 CIK 不同，明细见 {sec['report']}"})
    if calendar.get("fmp_issues"):
        items.append({"code": "calendar_mismatch",
                      "message": f"交易日历和 FMP 节假日有 {calendar['fmp_issues']} 处差异（meta.quality_issues 里的 CAL00x）"})
    return items
