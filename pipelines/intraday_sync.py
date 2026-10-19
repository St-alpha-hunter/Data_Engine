"""
5 分钟线的每日流程（Airflow daily_eod 调用），接在 pipelines/intraday.py 的拉取后面：

    publish_day   这一天的 5 分钟线推进 Kafka（不等待，尽快发完）→ 常驻的 Spark 流任务消费：清洗校验 → 合成 → silver
    wait_stream   等 Spark 把 Kafka 里的消息都处理完（比对 Kafka 各分区的末尾和流任务检查点里已提交的位置）
    sync_day      silver 里这一天的结果：
                    · 质量问题、隔离记录 → meta.quality_issues / meta.quarantine_records（复用 record_validation，按批次整体替换）
                    · 通过校验的（5min 去掉隔离的 + 15min / 1h / 1d）→ ClickHouse golden.bars_intraday（先删这一天再写入）

质量问题按（批次, 代码, 交易日, 规则）一条：一天里同一条规则违反了几根、是哪几根写在 detail 里
（meta 的去重键是 批次 + 规则 + 代码 + 日期，按日线设计的；一天几十根 5 分钟线各记一条会被去重掉）；
隔离记录按（批次, 代码, 交易日）一条，record 里是这一天被隔离的全部 K 线。
"""
import json
import logging
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from db import ClickHouseStore, MetaStore
from pipelines import lakehouse
from spark_jobs.bar_rules import BY_ID, ENDPOINT, rule_definitions
from streaming import replay_producer

log = logging.getLogger(__name__)

SILVER = "bars"
TARGET = "golden.bars_intraday"
CHECKPOINT = "_checkpoints/bars_stream"
CH_COLS = ["security_id", "symbol", "interval", "bar_start", "trade_date", "open", "high", "low", "close", "volume",
           "n_bars", "is_complete", "quality_status", "batch_id", "computed_at"]


# =========================
# 推送
# =========================
def publish_day(trade_date: date, bootstrap: str | None = None) -> dict:
    from confluent_kafka import Producer
    bootstrap = bootstrap or replay_producer.default_bootstrap()
    replay_producer.ensure_topic(bootstrap)
    bars = replay_producer.load_bars(trade_date)
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "compression.type": "lz4",
                         "enable.idempotence": True})
    out = replay_producer.replay(bars, producer, speedup=0)
    log.info(f"[intraday] {trade_date} 推进 Kafka：{out}")
    return out


# =========================
# 等流任务处理完
# =========================
def kafka_end_offsets(bootstrap: str, topic: str = replay_producer.TOPIC) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "intraday-sync-probe", "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def committed_offsets(checkpoint: Path, topic: str = replay_producer.TOPIC) -> dict[int, int]:
    """流任务检查点里最后一个已提交微批的结束位置（commits/N 存在 = 第 N 批已经写完 silver）"""
    commits = [int(p.name) for p in (checkpoint / "commits").glob("[0-9]*")]
    if not commits:
        return {}
    last_line = (checkpoint / "offsets" / str(max(commits))).read_text(encoding="utf-8").strip().splitlines()[-1]
    return {int(p): o for p, o in json.loads(last_line).get(topic, {}).items()}


def wait_stream(bootstrap: str | None = None, timeout: float = 1200, poll: float = 10, sleep=time.sleep) -> dict:
    """Kafka 每个分区的末尾都被流任务提交了才返回；超时说明流任务没在跑或跟不上 → 报错（Airflow 会重试）"""
    bootstrap = bootstrap or replay_producer.default_bootstrap()
    checkpoint = Path(lakehouse.table_uri(CHECKPOINT))
    waited = 0.0
    while True:
        end, done = kafka_end_offsets(bootstrap), committed_offsets(checkpoint)
        behind = {p: o - done.get(p, 0) for p, o in end.items() if done.get(p, 0) < o}
        if not behind:
            return {"offsets": end, "waited_s": waited}
        if waited >= timeout:
            raise RuntimeError(f"[intraday] 等了 {waited:.0f} 秒，Spark 流任务还没处理完（各分区落后 {behind}）："
                               f"检查 spark 容器是否在运行（docker compose logs spark）")
        sleep(poll)
        waited += poll


# =========================
# 同步
# =========================
def silver_day(trade_date: date, base: str | None = None) -> pd.DataFrame:
    con = lakehouse.connect(base=base)
    return con.execute("SELECT * FROM bars WHERE trade_date = ?", [trade_date]).df()


def quality_records(day: pd.DataFrame) -> dict[str, tuple[list[dict], list[dict]]]:
    """这一天的 5 分钟线 → {批次: (质量问题, 隔离记录)}；没问题的批次也在里面（空列表，用来替换掉旧记录）"""
    bars = day[(day["interval"] == "5min") & day["batch_id"].notna()]
    out = {b: ([], []) for b in bars["batch_id"].unique()}
    flagged = bars[bars["quality_flags"].map(lambda f: f is not None and len(f) > 0)]
    hits = defaultdict(list)
    for r in flagged.itertuples():
        for rule in r.quality_flags:
            hits[(r.batch_id, r.symbol, r.trade_date, rule)].append(r)
    for (batch, symbol, d, rule), rows in hits.items():
        out[batch][0].append({
            "rule_id": rule, "rule_version": BY_ID[rule]["rule_version"], "severity": BY_ID[rule]["severity"],
            "symbol": symbol, "date": d,
            "detail": {"bars": len(rows), "bar_start": [pd.Timestamp(r.bar_start).isoformat() for r in rows][:20]}})
    quarantined = bars[bars["quality_status"] == "quarantined"]
    for (batch, symbol, d), g in quarantined.groupby(["batch_id", "symbol", "trade_date"]):
        out[batch][1].append({"symbol": symbol, "date": d, "record": {"bars": [
            {"bar_start": pd.Timestamp(r.bar_start).isoformat(), "open": r.open, "high": r.high, "low": r.low,
             "close": r.close, "volume": r.volume, "flags": list(r.quality_flags)} for r in g.itertuples()]}})
    return out


def sync_day(trade_date: date, *, store: MetaStore | None = None, ch: ClickHouseStore | None = None,
             base: str | None = None, target: str = TARGET) -> dict:
    store, ch = store or MetaStore(), ch or ClickHouseStore()
    day = silver_day(trade_date, base)
    if day.empty:
        return {"rows": 0}

    # 质量问题 / 隔离：按批次整体替换（重跑不会重复）
    store.sync_rules(rule_definitions())
    n_issues = n_quarantined = 0
    for batch, (issues, records) in quality_records(day).items():
        a, b = store.record_validation(batch, ENDPOINT, issues, records, replace=True)
        n_issues, n_quarantined = n_issues + a, n_quarantined + b

    # ClickHouse：隔离的不同步；这一天先删再写
    good = day[day["quality_status"].fillna("unchecked") != "quarantined"].copy()
    good["quality_status"] = good["quality_status"].fillna("unchecked")
    good["batch_id"] = good["batch_id"].fillna("")
    good["is_complete"] = good["is_complete"].astype("uint8")
    for c in ["bar_start", "computed_at"]:
        good[c] = pd.to_datetime(good[c], utc=True)
    ch.command(f"DELETE FROM {target} WHERE trade_date = {{d:Date32}}", {"d": trade_date})
    rows = ch.insert_df(target, good[CH_COLS])
    for batch, n in good[good["batch_id"] != ""].groupby("batch_id").size().items():
        store.log_load(batch, "golden", target, int(n))

    out = {"rows": rows, "by_interval": good["interval"].value_counts().to_dict(),
           "status": day[day["interval"] == "5min"]["quality_status"].fillna("unchecked").value_counts().to_dict(),
           "issues": n_issues, "quarantined": n_quarantined}
    log.info(f"[intraday] {trade_date} 同步：{out}")
    return out
