"""
### 每月流程：参考数据

北京时间每月 1 号中午 12 点运行：

| 步骤 | 做什么 |
|---|---|
| profiles | 公司概况（行业、交易所、是否仍在交易等）→ raw → 清洗 |
| check_sec | 证券主表里仍在交易的证券 vs SEC 官方代码表（代码还在不在、CIK 是否一致），明细写 `data/reports/sec_check_<日期>.csv` |
| trading_calendar | 交易日历延伸到两年后的年底，并和 FMP 节假日交叉核对 |
| summarize_and_alert | 汇总；核对发现不一致就提醒 |

手动指定日期：触发时传 `{"run_date": "2026-11-01"}`。
"""
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

FMP_POOL = "fmp"


def _alert_on_failure(context):
    from pipelines.alerts import on_task_failure
    on_task_failure(context)


@dag(
    schedule=CronTriggerTimetable("0 12 1 * *", timezone="Asia/Shanghai"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 4, "retry_delay": timedelta(minutes=10),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(hours=1), "execution_timeout": timedelta(hours=1),
                  "on_failure_callback": _alert_on_failure},
    params={"run_date": Param(None, type=["null", "string"], description="手动指定运行日期（YYYY-MM-DD）")},
    tags=["monthly"],
    doc_md=__doc__,
)
def monthly_reference():

    @task
    def run_date() -> str:
        ctx = get_current_context()
        if ctx["params"].get("run_date"):
            return ctx["params"]["run_date"]
        when = ctx.get("logical_date") or ctx["dag_run"].run_after
        return when.in_timezone("Asia/Shanghai").date().isoformat()

    @task(pool=FMP_POOL)
    def profiles(d: str) -> dict:
        from pipelines.periodic import monthly_profiles
        return monthly_profiles(date.fromisoformat(d))

    @task
    def check_sec(d: str) -> dict:
        from pipelines.periodic import check_against_sec
        return check_against_sec(date.fromisoformat(d))

    @task(pool=FMP_POOL)
    def trading_calendar(d: str) -> dict:
        from pipelines.periodic import update_trading_calendar
        return update_trading_calendar(date.fromisoformat(d))

    @task(trigger_rule="all_done", retries=0)
    def summarize_and_alert(d: str, sec: dict | None, calendar: dict | None) -> dict:
        from pipelines.alerts import check
        from pipelines.daily import summarize
        from pipelines.periodic import MONTHLY_ENDPOINTS, monthly_checks
        ctx = get_current_context()
        summary = summarize(date.fromisoformat(d), ctx["dag_run"].start_date, endpoints=MONTHLY_ENDPOINTS)
        summary["checks"] = monthly_checks(sec or {}, calendar or {})
        alerts = check("monthly_reference", ctx["run_id"], summary)
        if any(a["level"] == "error" for a in alerts):
            raise RuntimeError("; ".join(a["message"] for a in alerts if a["level"] == "error"))
        return {"summary": summary, "alerts": alerts}

    d = run_date()
    p = profiles(d)
    sec = check_sec(d)
    cal = trading_calendar(d)
    p >> sec >> cal
    summarize_and_alert(d, sec, cal)


monthly_reference()
