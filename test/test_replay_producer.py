"""回放：按时刻顺序发送、重复 / 迟到演练"""
import json
from datetime import date, datetime, timedelta, timezone

from streaming import replay_producer as rp


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key.decode(), json.loads(value)))

    def poll(self, t):
        pass

    def flush(self):
        pass


def bars(n_ticks=10, symbols=("AAA", "BBB")):
    t0 = datetime(2026, 10, 16, 13, 30, tzinfo=timezone.utc)
    return [{"security_id": i + 1, "symbol": s, "bar_start": t0 + timedelta(minutes=5 * k),
             "trade_date": date(2026, 10, 16), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
             "volume": 1.0, "batch_id": "b"} for k in range(n_ticks) for i, s in enumerate(symbols)]


def test_replay_in_time_order_with_pacing():
    p, slept = FakeProducer(), []
    stats = rp.replay(bars(), p, speedup=600, sleep=slept.append)
    assert stats == {"ticks": 10, "sent": 20, "duplicated": 0, "late": 0}
    times = [m["bar_start"] for _, _, m in p.sent]
    assert times == sorted(times) and p.sent[0][1] == "AAA" and p.sent[0][0] == "bars.5min"
    assert slept[0] == 0.5


def test_duplicates_and_late_messages_all_delivered():
    p = FakeProducer()
    stats = rp.replay(bars(200), p, speedup=0, duplicate=0.05, late=0.05)
    assert stats["duplicated"] > 0 and stats["late"] > 0
    assert stats["sent"] == 400 + stats["duplicated"]                     # 迟到的最后也都发了
    keys = {(m["symbol"], m["bar_start"]) for _, _, m in p.sent}
    assert len(keys) == 400                                               # 每根 K 线至少到一次
    times = [m["bar_start"] for _, _, m in p.sent]
    assert times != sorted(times)                                         # 确实有乱序
