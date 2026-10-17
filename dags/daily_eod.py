"""
### 每日流程（美股收盘后）

北京时间周二到周六早上 8 点运行，处理**纽约前一天**的交易日（换算见 `pipelines/daily.py` 的 `trade_date_for`）；
那天不是美股交易日（周末、假日）→ 整个流程跳过。

| 步骤 | 做什么 |
|---|---|
| trade_date | 执行时间 → 美股交易日；不是交易日就跳过后面所有步骤 |
| load_events | 代码变更、S&P 500 成分变更、收购、分红 / 拆股日历 |
| maintain_securities | 证券主表增量维护（改名、新调入、退市）；新调入的补全历史 |
| backfill_security_id | 新事件回填 security_id |
| ingest_prices | 日线（不复权 + 复权）最近 7 天 → raw → 清洗 |
| load_golden | 新批次 → golden；重算复权因子；和 FMP 核对 |
| summarize_and_alert | 当天的批次、问题、隔离统计；有问题就提醒（见 `pipelines/alerts.py`） |

**每一步都可以重跑**（幂等）：任务失败自动重试 2 次；补跑过去某一天和当天跑的结果一样。
手动指定日期：触发时传 `{"trade_date": "2026-10-13"}`。
请求 FMP 的步骤共用资源池 `fmp`（1 个位置），不会同时打 FMP。
"""
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

FMP_POOL = "fmp"


def _alert_on_failure(context):
    """任务重试用完、最终失败时记一条提醒（data/alerts/alerts.jsonl）"""
    from pipelines.alerts import on_task_failure
    on_task_failure(context)


def _trade_date(value: str) -> date:
    return date.fromisoformat(value)


@dag(
    schedule=CronTriggerTimetable("0 8 * * 2-6", timezone="Asia/Shanghai"),
    start_date=pendulum.datetime(2026, 10, 13, tz="Asia/Shanghai"),
    catchup=False,                     # 不自动补历史；需要补哪几天用 airflow backfill 指定
    max_active_runs=1,                 # 同一时间只跑一天，补跑时按日期顺序一天一天来
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(hours=2),
                  "on_failure_callback": _alert_on_failure},
    params={"trade_date": Param(None, type=["null", "string"], description="手动指定美股交易日（YYYY-MM-DD）；不填按执行时间换算")},
    tags=["daily"],
    doc_md=__doc__,
)
def daily_eod():

    @task.short_circuit
    def trade_date() -> str | bool:
        """执行时间 → 美股交易日；不是交易日返回 False，后面的步骤全部跳过"""
        from db.meta_store import MetaStore
        from pipelines.daily import trade_date_for
        ctx = get_current_context()
        if ctx["params"].get("trade_date"):
            return ctx["params"]["trade_date"]
        when = ctx.get("logical_date") or ctx["dag_run"].run_after       # 手动触发时 logical_date 可能为空
        d = trade_date_for(when, MetaStore().conn)
        return d.isoformat() if d else False

    @task(pool=FMP_POOL)
    def load_events(d: str) -> dict:
        from pipelines.daily import load_events as run
        return run(_trade_date(d))

    @task(pool=FMP_POOL)
    def maintain_securities(d: str) -> dict:
        from pipelines.daily import maintain_securities as run
        return run(_trade_date(d))

    @task
    def backfill_security_id(d: str) -> dict:
        from pipelines.daily import backfill_security_id as run
        return run(_trade_date(d))

    @task(pool=FMP_POOL)
    def ingest_prices(d: str) -> dict:
        from pipelines.daily import ingest_prices as run
        return run(_trade_date(d))

    @task
    def load_golden(d: str) -> dict:
        from pipelines.daily import load_golden as run
        out = run(_trade_date(d))
        return {"batches": out["batches"], "check": out["check"]}       # XCom 只放小的汇总

    @task(retries=0)
    def summarize_and_alert(d: str) -> dict:
        """汇总 + 提醒：有批次失败 → 任务标红；其他问题只记录（data/alerts/alerts.jsonl）"""
        from pipelines.alerts import check
        from pipelines.daily import summarize
        ctx = get_current_context()
        summary = summarize(_trade_date(d), ctx["dag_run"].start_date)
        alerts = check("daily_eod", ctx["run_id"], summary)
        if any(a["level"] == "error" for a in alerts):
            raise RuntimeError("; ".join(a["message"] for a in alerts if a["level"] == "error"))
        return {"summary": summary, "alerts": alerts}

    d = trade_date()
    events = load_events(d)
    securities = maintain_securities(d)
    ids = backfill_security_id(d)
    prices = ingest_prices(d)
    golden = load_golden(d)
    events >> securities >> ids >> prices >> golden >> summarize_and_alert(d)


daily_eod()
