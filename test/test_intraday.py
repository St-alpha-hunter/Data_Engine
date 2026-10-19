"""5 分钟线 → bronze：时区换算、断点续跑"""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from pipelines import intraday, lakehouse
from pipelines.backfill import Universe


class FakeFetcher:
    calls = []

    def __init__(self, symbols, endpoint, params):
        self.symbols = symbols
        FakeFetcher.calls.append(list(symbols))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def fetch_fmp_batch(self):
        rows = [{"symbol": s, "date": d, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}
                for s in self.symbols if s != "NODATA"
                for d in ["2026-10-16 09:30:00", "2026-10-16 15:55:00", "2026-10-16 15:55:00"]]   # 重复一根
        ok = [s for s in self.symbols if s != "NODATA"]
        missing = [s for s in self.symbols if s == "NODATA"]
        return (pd.DataFrame(rows), {"success": len(ok), "missing": len(missing), "failed": 0,
                                     "missing_symbols": missing}, ok, [])


@pytest.fixture
def universe(monkeypatch):
    u = Universe(symbols=["AAA", "BBB", "NODATA"], skipped=[], security_of={"AAA": 1, "BBB": 2, "NODATA": 3})
    monkeypatch.setattr(intraday, "build_universe", lambda conn, today: u)
    FakeFetcher.calls = []
    return u


def test_to_bronze_converts_new_york_time():
    df = pd.DataFrame([{"symbol": "AAA", "date": "2026-10-16 09:30:00", "open": 1, "high": 1, "low": 1,
                        "close": 1, "volume": 1},
                       {"symbol": "AAA", "date": "2026-01-16 09:30:00", "open": 1, "high": 1, "low": 1,
                        "close": 1, "volume": 1}])
    t = intraday.to_bronze(df, {"AAA": 1}, "b1").to_pandas()
    assert list(t["bar_start"]) == [pd.Timestamp("2026-01-16 14:30", tz="UTC"),       # 冬令时 UTC-5
                                    pd.Timestamp("2026-10-16 13:30", tz="UTC")]       # 夏令时 UTC-4
    assert list(t["trade_date"]) == [date(2026, 1, 16), date(2026, 10, 16)]


def test_fetch_writes_bronze_and_resumes(store, universe, tmp_path):
    base = str(tmp_path / "lake")
    out = intraday.fetch(date(2026, 10, 1), date(2026, 10, 17), store=store, base=base,
                         chunk_size=2, fetcher_cls=FakeFetcher)
    assert out["todo"] == 3 and out["batches"] == 2
    con = lakehouse.connect(base=base)
    con.register("b", lakehouse.open_table("bars_5min", base).to_pyarrow_dataset())
    assert con.sql("SELECT symbol, security_id, count(*) FROM b GROUP BY ALL ORDER BY 1").fetchall() == \
        [("AAA", 1, 2), ("BBB", 2, 2)]

    # 重跑：都已完成（NODATA 记在 missing_symbols 里）→ 不再请求
    again = intraday.fetch(date(2026, 10, 1), date(2026, 10, 17), store=store, base=base, fetcher_cls=FakeFetcher)
    assert again["todo"] == 0 and len(FakeFetcher.calls) == 2
    # 参数不同（另一段日期）→ 重新拉
    third = intraday.fetch(date(2026, 10, 1), date(2026, 10, 18), store=store, base=base, fetcher_cls=FakeFetcher)
    assert third["todo"] == 3
