"""
回放：bronze 里的 5 分钟线按时间顺序推进 Kafka，模拟实时行情

为什么用回放：FMP 套餐没有 1 分钟线，盘中轮询 5 分钟线要整夜开机、也很费带宽；
回放出来的消息和真实实时流在 Kafka / Spark 眼里没有区别，而且随时能重来、结果可复现（开发、测试、演示都用它）。

消息：主题 bars.5min，键 = 股票代码（同一只股票进同一个分区 → 分区内按时间有序），值 = JSON 一根 K 线
节奏：每个 5 分钟时刻把所有股票这一根一起发出，再等 300 / speedup 秒（默认 600 倍：一个交易日约 40 秒）
故障演练（--duplicate / --late）：按比例重复发送、或推迟几个时刻再发，用来演示消费端去重和迟到数据处理

用法：
    python -m streaming.replay_producer                              # 全部日期，600 倍速
    python -m streaming.replay_producer --day 2026-10-16 --speedup 0 # 只回放一天，不等待（尽快发完）
    python -m streaming.replay_producer --duplicate 0.01 --late 0.005
"""
import argparse
import json
import logging
import os
import random
import time
from collections import defaultdict
from datetime import date, datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pipelines import lakehouse
from pipelines.intraday import TABLE

log = logging.getLogger(__name__)

TOPIC = "bars.5min"
PARTITIONS = 4
BOOTSTRAP = "localhost:9094"                         # 本机连 Kafka；容器里（Airflow）用环境变量 KAFKA_BOOTSTRAP=kafka:9092


def default_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", BOOTSTRAP)
LATE_TICKS = 3                                       # 迟到的消息推迟几个 5 分钟时刻再发


def ensure_topic(bootstrap: str | None = None, topic: str = TOPIC, partitions: int = PARTITIONS):
    admin = AdminClient({"bootstrap.servers": bootstrap or default_bootstrap()})
    if topic in admin.list_topics(timeout=10).topics:
        return
    for f in admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)]).values():
        f.result()
    log.info(f"[replay] 创建主题 {topic}（{partitions} 个分区）")


def load_bars(day: date | None = None, base: str | None = None) -> list[dict]:
    """bronze → 按 (bar_start, symbol) 排好的 K 线；同一根 K 线拉过多次的，取最近一次拉取的"""
    con = lakehouse.connect(base=base)
    con.register("b", lakehouse.open_table(TABLE, base).to_pyarrow_dataset())
    where = "WHERE trade_date = ?" if day else ""
    rows = con.execute(f"""
        SELECT security_id, symbol, bar_start, trade_date, open, high, low, close, volume, batch_id
        FROM b {where}
        QUALIFY row_number() OVER (PARTITION BY symbol, bar_start ORDER BY ingested_at DESC) = 1
        ORDER BY bar_start, symbol""", [day] if day else []).fetchall()
    cols = ["security_id", "symbol", "bar_start", "trade_date", "open", "high", "low", "close", "volume", "batch_id"]
    return [dict(zip(cols, r)) for r in rows]


def message(bar: dict) -> bytes:
    return json.dumps({**bar, "bar_start": bar["bar_start"].astimezone(timezone.utc).isoformat(),
                       "trade_date": bar["trade_date"].isoformat(),
                       "sent_at": datetime.now(timezone.utc).isoformat()}).encode()


def replay(bars: list[dict], producer, *, speedup: float = 600, duplicate: float = 0.0, late: float = 0.0,
           seed: int = 7, sleep=time.sleep) -> dict:
    """按时刻分组发送；返回发送统计"""
    rng = random.Random(seed)
    ticks = defaultdict(list)
    for b in bars:
        ticks[b["bar_start"]].append(b)
    delayed = defaultdict(list)                      # 时刻序号 → 推迟到那时再发的消息
    stats = {"ticks": len(ticks), "sent": 0, "duplicated": 0, "late": 0}
    order = sorted(ticks)
    for i, t in enumerate(order + [None] * LATE_TICKS):    # 末尾多走几个空时刻，把迟到的发完
        out = list(delayed.pop(i, []))
        for b in ticks.get(t, []):
            if late and rng.random() < late:
                delayed[i + LATE_TICKS].append(b)
                stats["late"] += 1
                continue
            out.append(b)
            if duplicate and rng.random() < duplicate:
                out.append(b)
                stats["duplicated"] += 1
        for b in out:
            producer.produce(TOPIC, key=b["symbol"].encode(), value=message(b))
            producer.poll(0)
        stats["sent"] += len(out)
        producer.flush()
        if t is not None and (i + 1) % 78 == 0:
            log.info(f"[replay] 已发到 {t.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC，累计 {stats['sent']} 条")
        if speedup:
            sleep(300 / speedup)
    return stats


def main():
    parser = argparse.ArgumentParser(description="bronze 5 分钟线 → Kafka 回放")
    parser.add_argument("--day", type=date.fromisoformat, help="只回放这一天（纽约日期）")
    parser.add_argument("--speedup", type=float, default=600, help="倍速；0 = 不等待")
    parser.add_argument("--duplicate", type=float, default=0.0, help="重复发送的比例，如 0.01")
    parser.add_argument("--late", type=float, default=0.0, help=f"推迟 {LATE_TICKS} 个时刻再发的比例")
    parser.add_argument("--bootstrap", default=default_bootstrap())
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_topic(args.bootstrap)
    bars = load_bars(args.day)
    log.info(f"[replay] {len(bars)} 根 K 线，{args.speedup} 倍速")
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20, "compression.type": "lz4",
                         "enable.idempotence": True})        # 幂等生产者：网络重试不会在 Kafka 里产生重复
    print(replay(bars, producer, speedup=args.speedup, duplicate=args.duplicate, late=args.late))


if __name__ == "__main__":
    main()
