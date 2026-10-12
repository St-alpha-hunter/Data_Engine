"""
EventStore：把整理好的标准化事件写进事件表

    store = EventStore()
    stats = store.load("dividend", result, batch_id)     # result 是 fetch_data.events 的 NormalizeResult

做三件事：
    1. events 逐条调用对应的写库函数 events.record_xxx()（去重、版本、总表 + 明细都在数据库里完成）
    2. 整理阶段被拒绝的行（result.rejected）记为质量问题 XXX001
    3. 写库时被数据库约束拒绝的事件记为质量问题 XXX002；单条失败不影响其他事件
质量规则以代码为准，定义在 EVENT_RULES，写库前自动同步到 meta.quality_rules
"""
import logging
from collections import Counter

import psycopg
from psycopg.types.json import Jsonb

from db.connections import get_pg
from db.meta_store import MetaStore
from fetch_data.events.common import NormalizeResult, clean_text, to_date

log = logging.getLogger(__name__)

# 事件类型 → 写库函数
RECORD_FUNCTIONS = {
    "dividend": "events.record_dividend",
    "split": "events.record_split",
    "symbol_change": "events.record_symbol_change",
    "acquisition": "events.record_acquisition",
    "index_change": "events.record_sp500_index_change",
}

# 事件类型 → (规则编号前缀, 规则所属 endpoint, 原始行里的代码字段, 原始行里的日期字段)
EVENT_META = {
    "dividend":      ("EVD", "dividends",            "symbol",         "date"),
    "split":         ("EVS", "splits",               "symbol",         "date"),
    "symbol_change": ("EVC", "symbol_change",        "oldSymbol",      "date"),
    "acquisition":   ("EVA", "mergers_acquisitions", "targetedSymbol", "transactionDate"),
    "index_change":  ("EVI", "sp500_github",         "ticker",         "start_date"),
}

EVENT_NAMES = {"dividend": "分红", "split": "拆股", "symbol_change": "代码变更",
               "acquisition": "收购", "index_change": "S&P 500 成分股变更"}


def _rules_for(event_type: str) -> list[dict]:
    prefix, endpoint, _, _ = EVENT_META[event_type]
    name = EVENT_NAMES[event_type]
    return [
        {"rule_id": f"{prefix}001", "rule_version": 1, "endpoint": endpoint, "severity": "danger",
         "description": f"{name}原始数据被整理函数拒绝（日期无效、代码格式不对、数值不合法等），未写入事件表；原因见 detail.reason",
         "params": {}, "impl": "fetch_data.events"},
        {"rule_id": f"{prefix}002", "rule_version": 1, "endpoint": endpoint, "severity": "danger",
         "description": f"{name}事件写库时被数据库约束拒绝，未写入事件表；错误见 detail.error",
         "params": {}, "impl": "db.event_store.EventStore"},
    ]


EVENT_RULES = {event_type: _rules_for(event_type) for event_type in EVENT_META}


class EventStore:

    def __init__(self, conn: psycopg.Connection | None = None, meta: MetaStore | None = None):
        self.conn = conn or get_pg()
        self.meta = meta or MetaStore(self.conn)

    def record(self, event_type: str, event: dict, batch_id: str) -> tuple[int, int, str]:
        """写一个标准化事件，返回 (event_id, revision, action)；action = inserted / revised / duplicate"""
        params = {f"p_{k}": (Jsonb(v) if k == "payload" else v) for k, v in event.items()}
        params["p_batch_id"] = batch_id
        args = ", ".join(f"{name} => %({name})s" for name in params)
        return self.conn.execute(f"SELECT * FROM {RECORD_FUNCTIONS[event_type]}({args})", params).fetchone()

    def load(self, event_type: str, result: NormalizeResult, batch_id: str) -> dict:
        """写入一批整理结果，返回统计：inserted / revised / duplicate / db_rejected / rejected / skipped"""
        prefix, _, symbol_field, date_field = EVENT_META[event_type]
        self.meta.sync_rules(EVENT_RULES[event_type])
        versions = {r["rule_id"]: r["rule_version"] for r in EVENT_RULES[event_type]}

        counts = Counter()
        issues = []

        for event in result.events:
            try:
                _, _, action = self.record(event_type, event, batch_id)
                counts[action] += 1
            except psycopg.Error as e:
                counts["db_rejected"] += 1
                log.warning(f"[{event_type}] 写库失败 {event.get('source_event_key')}: {e}")
                issues.append(self._issue(f"{prefix}002", versions, event.get("payload", {}), symbol_field, date_field,
                                          {"source_event_key": event.get("source_event_key"),
                                           "error": f"{type(e).__name__}: {str(e).splitlines()[0]}"}))

        for rejected in result.rejected:
            issues.append(self._issue(f"{prefix}001", versions, rejected["row"], symbol_field, date_field,
                                      {"reason": rejected["reason"], "row": rejected["row"]}))

        n_issues = self.meta.record_issues(batch_id, issues) if issues else 0
        stats = {
            "inserted": counts["inserted"], "revised": counts["revised"], "duplicate": counts["duplicate"],
            "db_rejected": counts["db_rejected"], "rejected": len(result.rejected), "skipped": len(result.skipped),
            "issues_recorded": n_issues,
        }
        log.info(f"[{event_type}] 批次 {batch_id} 写库：{stats}")
        return stats

    @staticmethod
    def _issue(rule_id: str, versions: dict, row: dict, symbol_field: str, date_field: str, detail: dict) -> dict:
        row = row or {}
        # acquisition 的 payload 是合并后的结构，代码 / 日期在第一行原始数据里
        if "rows" in row and isinstance(row["rows"], list) and row["rows"]:
            row = row["rows"][0]
        return {
            "rule_id": rule_id, "rule_version": versions[rule_id], "severity": "danger",
            "symbol": clean_text(row.get(symbol_field)) or "(missing)",
            "date": to_date(row.get(date_field)),
            "detail": detail,
        }
