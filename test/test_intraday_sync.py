"""5 分钟线每日同步：等流任务、质量问题入库、同步 ClickHouse"""
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest

from pipelines import intraday_sync as sync

D = date(2026, 10, 16)
T0 = pd.Timestamp("2026-10-16 13:30", tz="UTC")


def silver(rows):
    """rows: (interval, symbol, minutes_from_open, status, flags, batch_id)"""
    return pd.DataFrame([{
        "security_id": {"AAA": 1, "BBB": 2}[sym], "symbol": sym, "interval": iv,
        "bar_start": T0 + pd.Timedelta(minutes=m), "trade_date": D,
        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.0, "volume": 100.0,
        "n_bars": 1 if iv == "5min" else 3, "is_complete": True, "quality_status": st,
        "quality_flags": np.array(fl, dtype=object) if fl is not None else None, "batch_id": b,
        "computed_at": pd.Timestamp("2026-10-17 02:10", tz="UTC"), "source": "stream"}
        for iv, sym, m, st, fl, b in rows])


DAY = silver([
    ("5min", "AAA", 0, "health", [], "b1"),
    ("5min", "AAA", 5, "quarantined", ["IB002"], "b1"),
    ("5min", "AAA", 10, "quarantined", ["IB002"], "b1"),
    ("5min", "AAA", 15, "warning", ["IB005"], "b1"),
    ("5min", "BBB", 0, "health", [], "b2"),
    ("15min", "AAA", 0, "warning", None, None),
    ("1d", "BBB", 0, "health", None, None),
])


def test_quality_records_grouped_per_symbol_day_rule():
    out = sync.quality_records(DAY)
    assert set(out) == {"b1", "b2"} and out["b2"] == ([], [])                  # 没问题的批次也在（用来清掉旧记录）
    issues, records = out["b1"]
    by_rule = {i["rule_id"]: i for i in issues}
    assert by_rule["IB002"]["detail"]["bars"] == 2 and by_rule["IB002"]["severity"] == "danger"
    assert by_rule["IB005"]["severity"] == "warning"
    [rec] = records
    assert rec["symbol"] == "AAA" and len(rec["record"]["bars"]) == 2 and rec["record"]["bars"][0]["flags"] == ["IB002"]


def test_sync_day_writes_meta_and_clickhouse_idempotently(store, ch, monkeypatch):
    for b in ["b1", "b2"]:                                                      # 批次要先在 meta.ingestion 里
        store.conn.execute("INSERT INTO meta.ingestion (batch_id, endpoint, params, n_symbols) "
                           "VALUES (%s, 'price_5min', '{}', 1)", (b,))
    monkeypatch.setattr(sync, "silver_day", lambda d, base=None: DAY.copy())
    target = "golden_test.bars_intraday"
    first = sync.sync_day(D, store=store, ch=ch, target=target)
    assert first["rows"] == 5 and first["issues"] == 2 and first["quarantined"] == 1
    again = sync.sync_day(D, store=store, ch=ch, target=target)                  # 重跑：替换，不重复
    assert again["rows"] == 5
    assert ch.query_df(f"SELECT count() AS n FROM {target}")["n"][0] == 5
    got = ch.query_df(f"SELECT `interval` AS i, quality_status AS s, count() AS n FROM {target} GROUP BY 1, 2 ORDER BY 1, 2")
    assert got.values.tolist() == [["15min", "warning", 1], ["1d", "health", 1], ["5min", "health", 2], ["5min", "warning", 1]]
    assert store.conn.execute("SELECT count(*) FROM meta.quality_issues WHERE batch_id = 'b1'").fetchone()[0] == 2
    assert store.conn.execute("SELECT count(*) FROM meta.quarantine_records WHERE batch_id = 'b1'").fetchone()[0] == 1
    assert store.conn.execute("SELECT count(*) FROM meta.quality_rules WHERE endpoint = 'price_5min'").fetchone()[0] == 5


def test_committed_offsets_reads_last_commit(tmp_path):
    for n, offs in [(0, '{"bars.5min":{"0":5,"1":7}}'), (1, '{"bars.5min":{"0":9,"1":7}}'), (2, '{"bars.5min":{"0":99}}')]:
        (tmp_path / "offsets").mkdir(exist_ok=True)
        (tmp_path / "offsets" / str(n)).write_text(f'v1\n{{"batchWatermarkMs":0}}\n{offs}\n', encoding="utf-8")
    (tmp_path / "commits").mkdir()
    for n in (0, 1):                                                            # 第 2 批还没提交
        (tmp_path / "commits" / str(n)).write_text("v1\n{}", encoding="utf-8")
    assert sync.committed_offsets(tmp_path) == {0: 9, 1: 7}


def test_wait_stream_until_caught_up_or_timeout(monkeypatch):
    progress = iter([{0: 3, 1: 7}, {0: 9, 1: 7}])
    monkeypatch.setattr(sync, "kafka_end_offsets", lambda b: {0: 9, 1: 7})
    monkeypatch.setattr(sync, "committed_offsets", lambda cp: next(progress))
    slept = []
    assert sync.wait_stream("x", timeout=60, poll=10, sleep=slept.append)["waited_s"] == 10
    monkeypatch.setattr(sync, "committed_offsets", lambda cp: {0: 3})
    with pytest.raises(RuntimeError, match="还没处理完"):
        sync.wait_stream("x", timeout=20, poll=10, sleep=lambda s: None)
