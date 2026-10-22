"""
提醒：检查每次运行的汇总，有问题就记下来

目前写日志 + 追加到 data/alerts/alerts.jsonl（每条一行 JSON）；以后接邮件 / Slack 只改 notify()。
级别：
    error   有拉取批次失败（数据可能不全）→ Airflow 里提醒任务标红
    warning 新的 danger 问题（数据被隔离）、事件修订数异常、和 FMP 核对的问题变多、月度核对发现不一致 → 只记录

另外 on_task_failure() 给 Airflow 的 on_failure_callback 用：任何任务最终失败（重试用完）都记一条 error
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config.paths import DATA_DIR

log = logging.getLogger(__name__)

ALERT_DIR = DATA_DIR / "alerts"
ALERT_LOG = ALERT_DIR / "alerts.jsonl"
SUMMARY_LOG = ALERT_DIR / "summaries.jsonl"           # 每次运行的汇总，用来和上一次比较
EVENT_REVISION_LIMIT = 20                             # 一次运行新增的事件修订超过它就提醒（正常应接近 0）


def _append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def previous_summary(flow: str, path: Path | None = None) -> dict | None:
    """同一个流程上一次的汇总"""
    path = path or SUMMARY_LOG
    if not path.exists():
        return None
    last = None
    for line in path.read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if rec.get("flow") == flow:
            last = rec
    return last


def evaluate(flow: str, summary: dict, previous: dict | None = None) -> list[dict]:
    """汇总 → 提醒列表 [{level, code, message}]"""
    alerts = []
    batches = summary.get("batches", {})
    if "unresolved" in summary:
        # 只有暂时性失败、到现在还没补上的才标红；中途失败但重试补齐了的只提醒（说明网络不稳）
        unresolved = summary["unresolved"]
        if unresolved:
            n = sum(len(v) for v in unresolved.values())
            detail = "；".join(f"{ep} {len(v)} 个（{', '.join(v[:5])}{' …' if len(v) > 5 else ''}）" for ep, v in unresolved.items())
            alerts.append({"level": "error", "code": "batch_failed",
                           "message": f"有 {n} 个代码 / 请求因网络等原因最终没拉到：{detail}；重跑对应任务会自动补拉"})
        elif batches.get("failed") or batches.get("partial"):
            alerts.append({"level": "warning", "code": "batch_recovered",
                           "message": f"有 {batches.get('failed', 0) + batches.get('partial', 0)} 个批次中途拉取失败，"
                                      f"重试后已补齐（网络不稳？）"})
    elif batches.get("failed"):                                  # 旧格式的汇总（没有 unresolved）
        alerts.append({"level": "error", "code": "batch_failed",
                       "message": f"有 {batches['failed']} 个拉取批次失败，数据可能不全；重跑这个任务会自动补拉失败的部分"})
    if batches.get("partial") and "unresolved" not in summary:
        alerts.append({"level": "warning", "code": "batch_partial",
                       "message": f"有 {batches['partial']} 个批次部分股票拉取失败（多为个别股票 402 / 超时）"})
    danger = summary.get("issues", {}).get("danger", 0)
    if danger:
        alerts.append({"level": "warning", "code": "new_danger",
                       "message": f"新增 {danger} 条 danger 问题，对应的数据进了隔离区（meta.quarantine_records），需要核对"})
    revisions = summary.get("event_revisions", 0)
    if revisions > EVENT_REVISION_LIMIT:
        alerts.append({"level": "warning", "code": "event_revisions",
                       "message": f"股票池里新增 {revisions} 条事件内容修订（正常每天几条）：可能数据源字段在变，或者整理时顺序不固定"})
    if previous and "gd001_total" in summary and summary["gd001_total"] > previous.get("gd001_total", 0):
        alerts.append({"level": "warning", "code": "gd001_increase",
                       "message": f"和 FMP 复权价核对的问题从 {previous['gd001_total']} 条增加到 {summary['gd001_total']} 条，"
                                  f"多半是新的拆股 / 分红 / 分拆事件有问题"})
    for item in summary.get("checks", []):                     # 月度核对等额外的检查结果
        alerts.append({"level": "warning", **item})
    return alerts


def notify(flow: str, run_id: str, alerts: list[dict]) -> None:
    """记录提醒：日志 + alerts.jsonl（以后接邮件 / Slack 改这里）"""
    for a in alerts:
        record = {"time": datetime.now(timezone.utc), "flow": flow, "run_id": run_id, **a}
        (log.error if a["level"] == "error" else log.warning)(f"[提醒][{flow}] {a['message']}")
        _append(ALERT_LOG, record)


def check(flow: str, run_id: str, summary: dict) -> list[dict]:
    """评估 + 记录 + 保存这次的汇总（供下次比较）；返回提醒列表"""
    alerts = evaluate(flow, summary, previous_summary(flow))
    notify(flow, run_id, alerts)
    _append(SUMMARY_LOG, {"time": datetime.now(timezone.utc), "flow": flow, "run_id": run_id, **summary})
    return alerts


def on_task_failure(context) -> None:
    """Airflow 的 on_failure_callback：任务重试用完、最终失败时记一条 error"""
    ti = context["task_instance"]
    notify(ti.dag_id, context["run_id"], [{
        "level": "error", "code": "task_failed",
        "message": f"任务 {ti.task_id} 最终失败（已重试 {ti.try_number - 1} 次）：{context.get('exception')}"}])
