"""
### 每周流程：基本面数据

北京时间每周六上午 10 点运行：三张报表（年报 + 季报）、比率、企业价值、市值、一致预期、分析师评级，
每样拉最近几期 → raw → 清洗。财报一季度一份，每周拉最近几期既能拿到新发布的，也能发现最近几期被重述。

各任务都请求 FMP，共用资源池 `fmp`（一个接一个跑）；单个任务失败不影响其他任务（`trigger_rule=all_done`），
最后统一汇总、提醒。手动指定日期：触发时传 `{"run_date": "2026-10-17"}`。
"""
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

FMP_POOL = "fmp"
JOBS = ["income_annual", "income_quarter", "balance_annual", "balance_quarter", "cash_annual", "cash_quarter",
        "ratio", "enterprise_value", "market_cap", "estimate", "grades"]


def _alert_on_failure(context):
    from pipelines.alerts import on_task_failure
    on_task_failure(context)


@dag(
    schedule=CronTriggerTimetable("0 10 * * 6", timezone="Asia/Shanghai"),
    start_date=pendulum.datetime(2026, 10, 17, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(hours=2),
                  "on_failure_callback": _alert_on_failure},
    params={"run_date": Param(None, type=["null", "string"], description="手动指定运行日期（YYYY-MM-DD）")},
    tags=["weekly"],
    doc_md=__doc__,
)
def weekly_fundamentals():

    @task
    def run_date() -> str:
        ctx = get_current_context()
        if ctx["params"].get("run_date"):
            return ctx["params"]["run_date"]
        when = ctx.get("logical_date") or ctx["dag_run"].run_after
        return when.in_timezone("Asia/Shanghai").date().isoformat()

    # all_done：前一个任务失败也照样跑（各数据集互不依赖；串起来只是为了一个接一个请求 FMP）
    @task(pool=FMP_POOL, trigger_rule="all_done")
    def ingest(job: str, d: str) -> dict:
        from pipelines.periodic import weekly_job
        return weekly_job(job, date.fromisoformat(d))

    @task(trigger_rule="all_done", retries=0)
    def summarize_and_alert(d: str) -> dict:
        from pipelines.alerts import check
        from pipelines.daily import summarize
        ctx = get_current_context()
        summary = summarize(date.fromisoformat(d), ctx["dag_run"].start_date)
        alerts = check("weekly_fundamentals", ctx["run_id"], summary)
        if any(a["level"] == "error" for a in alerts):
            raise RuntimeError("; ".join(a["message"] for a in alerts if a["level"] == "error"))
        return {"summary": summary, "alerts": alerts}

    d = run_date()
    tasks = [ingest.override(task_id=f"ingest_{j}")(j, d) for j in JOBS]
    for upstream, downstream in zip(tasks, tasks[1:]):        # 一个接一个（资源池本来也只有一个位置）
        upstream >> downstream
    tasks[-1] >> summarize_and_alert(d)


weekly_fundamentals()
