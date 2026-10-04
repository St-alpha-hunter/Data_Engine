"""
C：从 raw 按 batch_id 读数据 → 按规则清洗校验 → 写问题记录 / 隔离区 / 中间态 parquet

    ① 前置检查：批次存在且拉取成功（success / partial），raw 写入成功
    ② 同步规则：代码里的规则同步到 meta.quality_rules（改了阈值没升版本会在这里报错）
    ③ 从 raw 读：按 batch_id 读出这一批，行数必须和 load_log 记录的一致
    ④ 校验：清洗类的 validate()，纯计算
    ⑤ 写问题记录 + 隔离区：同一个事务（MetaStore.record_validation）
    ⑥ 写中间态 parquet：clean_df + batch_id + source_logic_version，D 步骤读它写 golden
    ⑦ 错误报告：有问题时用 FeedErrorDeputy 生成 JSON 报告，路径回填 ingestion.report
    ⑧ 对账：raw 行数 = 去重行数 + health + warning + danger

重跑是安全的：问题记录 / 隔离数据有唯一约束不会重复，parquet 覆盖同名文件。

命令行：
    python -m pipelines.clean_runner <batch_id>
"""
import argparse
import logging
from dataclasses import dataclass

from clean.base_clean import CleanBasic, CleanResult
from clean.registry import get_cleaner
from db import ClickHouseStore, MetaStore
from db.object_store import IntermediateStore
from monitoring.feed_error_deputy import FeedErrorDeputy

log = logging.getLogger(__name__)

CLEANABLE_STATUS = ("success", "partial")


class CleanError(Exception):
    """清洗流程的前置条件或对账不满足"""


@dataclass
class CleanRunResult:
    batch_id: str
    endpoint: str
    raw_rows: int
    stats: dict
    issues_inserted: int
    quarantined_inserted: int
    intermediate_location: str
    report: str | None


def run_clean(batch_id: str, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
              intermediate: IntermediateStore | None = None) -> CleanRunResult:
    store = store or MetaStore()
    ch = ch or ClickHouseStore()
    intermediate = intermediate or IntermediateStore()

    # ① 前置检查
    ingestion = store.get_ingestion(batch_id)
    if ingestion is None:
        raise CleanError(f"批次 {batch_id} 不存在")
    if ingestion["status"] not in CLEANABLE_STATUS:
        raise CleanError(f"批次 {batch_id} 拉取状态是 {ingestion['status']}，只清洗 {CLEANABLE_STATUS} 的批次")
    endpoint = ingestion["endpoint"]

    raw_loads = [l for l in store.get_loads(batch_id) if l["layer"] == "raw" and l["status"] == "success"]
    if not raw_loads:
        raise CleanError(f"批次 {batch_id} 没有成功写入 raw 的记录")
    raw_table, raw_rows = raw_loads[-1]["target_table"], raw_loads[-1]["rows_written"]

    cleaner_cls = get_cleaner(endpoint)

    # ② 同步规则
    store.sync_rules(cleaner_cls.rule_definitions())

    # ③ 从 raw 读
    raw_df = ch.query_df(f"SELECT * FROM {raw_table} WHERE batch_id = {{batch_id:String}}", {"batch_id": batch_id})
    if len(raw_df) != raw_rows:
        raise CleanError(f"raw 里批次 {batch_id} 有 {len(raw_df)} 行，load_log 记录写入 {raw_rows} 行，对不上")
    source_logic_versions = raw_df["source_logic_version"].unique().tolist() if not raw_df.empty else []
    if len(source_logic_versions) > 1:
        raise CleanError(f"批次 {batch_id} 混有多个 source_logic_version：{source_logic_versions}")

    # ④ 校验
    cleaner = cleaner_cls(raw_df)
    result = cleaner.validate()
    log.info(f"[{endpoint}] 批次 {batch_id} 校验完成：{result.stats}")

    # ⑤ 问题记录 + 隔离区（同一个事务）
    records = _quarantine_records(result, cleaner_cls)
    with store.load_step(batch_id, "quarantine", "meta.quarantine_records") as step:
        n_issues, n_quarantined = store.record_validation(batch_id, endpoint, result.issues, records)
        step.rows = len(records)

    # ⑥ 中间态 parquet
    clean_df = result.clean_df.copy()
    clean_df["batch_id"] = batch_id
    clean_df["source_logic_version"] = source_logic_versions[0] if source_logic_versions else None
    location = intermediate.location(endpoint, batch_id)
    target = f"intermediate.{raw_table.split('.', 1)[1]}"
    with store.load_step(batch_id, "intermediate", target, location=location) as step:
        intermediate.write(clean_df, endpoint, batch_id)
        step.rows = len(clean_df)

    # ⑦ 错误报告（辅助产物：失败只记日志，不影响清洗结果）
    report = None
    if result.issues:
        try:
            if hasattr(cleaner, "build_stats"):
                cleaner.build_stats()
            report = FeedErrorDeputy(cleaner).generate_report()
            if report is not None:
                store.set_report(batch_id, report)
                report = str(report)
        except Exception as e:
            log.warning(f"[{endpoint}] 批次 {batch_id} 生成错误报告失败：{type(e).__name__}: {e}")

    # ⑧ 对账
    s = result.stats
    accounted = s["duplicates_dropped"] + s["health"] + s["warning"] + s["danger"]
    if accounted != raw_rows:
        raise CleanError(f"对账失败：raw {raw_rows} 行，去重 {s['duplicates_dropped']} + health {s['health']} "
                         f"+ warning {s['warning']} + danger {s['danger']} = {accounted}")

    log.info(f"[{endpoint}] 批次 {batch_id} 清洗完成：中间态 {len(clean_df)} 行 → {location}；"
             f"隔离 {len(records)} 行；问题 {len(result.issues)} 条")
    return CleanRunResult(batch_id, endpoint, raw_rows, s, n_issues, n_quarantined, location, report)


def _quarantine_records(result: CleanResult, cleaner_cls: type[CleanBasic]) -> list[dict]:
    q = result.quarantine_df
    if q.empty:
        return []
    rows = q.astype(object).where(q.notna(), None).to_dict(orient="records")
    return [{"symbol": r["symbol"], "date": r["date"], "record": r} for r in rows]


def main():
    parser = argparse.ArgumentParser(description="清洗一个已经写入 raw 的批次")
    parser.add_argument("batch_id")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    r = run_clean(args.batch_id)
    print(f"批次 {r.batch_id}：raw {r.raw_rows} 行 → {r.stats}")
    print(f"中间态：{r.intermediate_location}")
    if r.report:
        print(f"错误报告：{r.report}")


if __name__ == "__main__":
    main()
