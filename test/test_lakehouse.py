"""golden → 湖仓（Delta）：增量、幂等、核对、时间旅行"""
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from pipelines import lakehouse

DB = "golden_test"
T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def prices(ch, rows):
    """rows: (security_id, date, close, version, loaded_at)"""
    ch.insert_df(f"{DB}.price_daily", pd.DataFrame([{
        "security_id": s, "date": date.fromisoformat(d), "open": c, "high": c, "low": c, "close": c, "volume": 100.0,
        "quality_status": "health", "source": "fmp", "source_symbol": "AAA", "batch_id": "b", "source_logic_version": 1,
        "version": v, "loaded_at": t} for s, d, c, v, t in rows]))


def adjustment(ch, sid, ex, factor, version, at, active=1):
    ch.insert_df(f"{DB}.adjustment_events", pd.DataFrame([{
        "security_id": sid, "ex_date": date.fromisoformat(ex), "kind": "split", "value": 2.0, "prev_close": None,
        "price_factor": factor, "volume_factor": 2.0, "is_active": active, "event_id": 7, "event_revision": 1,
        "known_at": at, "version": version, "loaded_at": at}]))


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path / "lake")


def test_export_incremental_idempotent_and_time_travel(ch, lake):
    prices(ch, [(1, "2025-12-31", 10.0, 1, T0), (1, "2026-01-02", 11.0, 1, T0), (2, "2026-01-02", 20.0, 1, T0)])
    first = lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake)
    assert first == {"rows": 3, "inserted": 3, "updated": 0}

    # 重跑：没有新数据 → 不提交
    assert lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake) == {"rows": 0}
    assert lakehouse.open_table("price_daily", lake).version() == 0

    # 第二天：一行改了数（新版本），一行新增
    t1 = T0 + timedelta(days=1)
    prices(ch, [(1, "2026-01-02", 11.5, 2, t1), (1, "2026-01-05", 12.0, 1, t1)])
    second = lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake)
    assert second["inserted"] == 1 and second["updated"] == 1
    assert lakehouse.watermark(lakehouse.open_table("price_daily", lake)) == t1

    con = lakehouse.connect(base=lake)
    assert con.sql("SELECT count(*), sum(close) FROM price_daily").fetchone() == (4, 10.0 + 11.5 + 20.0 + 12.0)
    old = lakehouse.connect({"price_daily": 0}, base=lake)
    assert old.sql("SELECT close FROM price_daily WHERE security_id = 1 AND date = '2026-01-02'").fetchone() == (11.0,)

    # 分区按年
    dt = lakehouse.open_table("price_daily", lake)
    assert dt.metadata().partition_columns == ["year"]
    assert lakehouse.verify(ch=ch, db=DB, base=lake) == []


def test_same_watermark_rows_not_duplicated(ch, lake):
    """水位线用 >=：边界上的行会再取一次，但版本相同不会更新、也不会重复插入"""
    prices(ch, [(1, "2026-01-02", 11.0, 1, T0)])
    lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake)
    prices(ch, [(2, "2026-01-02", 20.0, 1, T0)])                  # 同一时间戳写入的另一行
    out = lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake)
    assert out == {"rows": 1, "inserted": 1, "updated": 0}
    assert lakehouse.connect(base=lake).sql("SELECT count(*) FROM price_daily").fetchone() == (2,)


def test_adjustments_retraction_and_verify_mismatch(ch, lake):
    adjustment(ch, 1, "2026-01-02", 0.5, 1, T0)
    lakehouse.export_versioned("adjustment_events", ch=ch, db=DB, base=lake)
    adjustment(ch, 1, "2026-01-02", 0.5, 2, T0 + timedelta(days=1), active=0)    # 撤回
    lakehouse.export_versioned("adjustment_events", ch=ch, db=DB, base=lake)
    con = lakehouse.connect(base=lake)
    assert con.sql("SELECT is_active, version FROM adjustment_events").fetchall() == [(False, 2)]
    assert lakehouse.verify(ch=ch, db=DB, base=lake) == []

    adjustment(ch, 1, "2026-01-02", 0.25, 3, T0 + timedelta(days=2))             # golden 改了、湖还没导
    [item] = lakehouse.verify(ch=ch, db=DB, base=lake)
    assert item["code"] == "lakehouse_mismatch" and "adjustment_events" in item["message"]


def test_securities_overwrite_only_when_changed(ch, lake):
    row = {"security_id": 1, "symbol": "AAA", "name": "A Corp", "status": "active", "security_type": "common_stock",
           "primary_exchange": "NYSE", "sector": None, "industry": None,
           "first_trade_date": date(2000, 1, 3), "last_trade_date": None}
    ch.insert_df(f"{DB}.securities", pd.DataFrame([row]))
    assert lakehouse.export_securities(ch=ch, db=DB, base=lake)["changed"]
    assert not lakehouse.export_securities(ch=ch, db=DB, base=lake)["changed"]
    ch.command(f"TRUNCATE TABLE {DB}.securities")
    ch.insert_df(f"{DB}.securities", pd.DataFrame([{**row, "symbol": "AAB"}]))
    assert lakehouse.export_securities(ch=ch, db=DB, base=lake)["changed"]
    assert lakehouse.open_table("securities", lake).version() == 1


def test_maintain_weekly(ch, lake):
    for i in range(3):
        prices(ch, [(1, f"2026-01-0{i + 2}", 10.0 + i, 1, T0 + timedelta(hours=i))])
        lakehouse.export_versioned("price_daily", ch=ch, db=DB, base=lake)
    assert lakehouse.maintain("price_daily", lake) is None                         # 刚建表不到 7 天
    out = lakehouse.maintain("price_daily", lake, now=datetime.now(timezone.utc) + timedelta(days=8))
    assert out["files_removed"] >= 2
    assert lakehouse.maintain("price_daily", lake, now=datetime.now(timezone.utc) + timedelta(days=1)) is None  # 刚维护过
    assert lakehouse.verify(ch=ch, db=DB, base=lake) == []
    assert lakehouse.connect(base=lake).sql("SELECT count(*) FROM price_daily").fetchone() == (3,)
