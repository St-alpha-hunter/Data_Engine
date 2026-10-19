"""
5 分钟线的质量规则（规则即代码）：Spark 按这里的参数检查，同步任务按这里的定义登记到 meta.quality_rules

和日线的规则写在清洗类里不同：检查在 Spark 里做（spark_jobs/bars.py 的 check_bars），这个文件不依赖 pyspark，
Spark 容器和 Airflow 容器都能导入。改了 severity / params 必须升 version（sync_rules 会检查）。

danger → 这根 K 线隔离（quality_status = quarantined），不参与合成；warning → 照常合成，打标记
"""
ENDPOINT = "price_5min"

RULES = [
    {"rule_id": "IB001", "rule_version": 1, "severity": "danger", "params": {},
     "description": "价格为空或不大于 0"},
    {"rule_id": "IB002", "rule_version": 1, "severity": "danger", "params": {"tolerance": 1e-9},
     "description": "最高价低于开盘 / 收盘价，或最低价高于开盘 / 收盘价"},
    {"rule_id": "IB003", "rule_version": 1, "severity": "danger", "params": {},
     "description": "成交量为负"},
    {"rule_id": "IB004", "rule_version": 1, "severity": "warning", "params": {},
     "description": "K 线时间不在当天的交易时段内（按交易日历，提前收盘的日子到 13:00）"},
    {"rule_id": "IB005", "rule_version": 1, "severity": "warning", "params": {"jump": 0.10},
     "description": "相对上一根收盘跳了 jump 以上、下一根又反向跳回 jump 以上（跳了又回来，像坏点）"},
]

BY_ID = {r["rule_id"]: r for r in RULES}
DANGER = [r["rule_id"] for r in RULES if r["severity"] == "danger"]


def rule_definitions() -> list[dict]:
    """给 MetaStore.sync_rules() 用"""
    return [{**r, "endpoint": ENDPOINT, "impl": f"spark_jobs.bars.check_bars:{r['rule_id']}"} for r in RULES]
