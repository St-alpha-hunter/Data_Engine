from datetime import date

import pandas as pd
import pytest
from psycopg.types.json import Jsonb

from db.object_store import IntermediateStore
from pipelines import golden_loader as gl

"""

D：中间态 → golden（测试库 golden_test / raw_test）

"""

DB = "golden_test"
D = date.fromisoformat
DAYS = [d.date() for d in pd.bdate_range("2022-01-03", "2022-01-14")]      # 10 个交易日


@pytest.fixture
def env(store, ch, tmp_path, monkeypatch):
    monkeypatch.setattr(gl, "SKIP_PATH", tmp_path / "skip.csv")
    (tmp_path / "skip.csv").write_text("symbol,reason\nZZQB,测试跳过\n", encoding="utf-8")
    sids = {}
    for sym, first in [("ZZQA", "2022-01-04"), ("ZZQB", "2022-01-03")]:
        sid = store.conn.execute("INSERT INTO ref.securities (name, security_type, first_trade_date, status, review_status) "
                                 "VALUES (%s, 'common_stock', %s, 'active', 'auto') RETURNING security_id", (sym, D(first))).fetchone()[0]
        store.conn.execute("INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source) "
                           "VALUES (%s, 'symbol', %s, %s, 'manual')", (sid, sym, D(first)))
        sids[sym] = sid
    return {"store": store, "ch": ch, "inter": IntermediateStore(str(tmp_path / "inter")), "sids": sids}


def closes(split_day=D("2022-01-10")):
    """不复权收盘价：100，2022-01-10 2 拆 1 之后 50"""
    return [100.0 if d < split_day else 50.0 for d in DAYS]


def make_batch(env, rows: dict[str, list[float]], status="health") -> str:
    store = env["store"]
    with store.ingestion_run(gl.PRICE_ENDPOINT, list(rows)) as run:
        run.finish({"success": len(rows), "missing": 0, "failed": 0}, [])
    df = pd.DataFrame([{"symbol": s, "date": pd.Timestamp(d), "adjOpen": c, "adjHigh": c, "adjLow": c, "adjClose": c,
                        "volume": 1000.0, "quality_status": status, "batch_id": run.batch_id, "source_logic_version": 1}
                       for s, cs in rows.items() for d, c in zip(DAYS, cs)])
    loc = env["inter"].write(df, gl.PRICE_ENDPOINT, run.batch_id)
    with store.load_step(run.batch_id, "intermediate", "intermediate.x", location=loc) as step:
        step.rows = len(df)
    return run.batch_id


def load(env, batch_id):
    return gl.load_prices(batch_id, store=env["store"], ch=env["ch"], intermediate=env["inter"], db=DB)


# =========================
# ✅ 日线写入、版本、截断、跳过
# =========================
def test_load_prices_versions_life_and_skip(env):
    b1 = make_batch(env, {"ZZQA": closes(), "ZZQB": closes(), "NOPE": closes()})
    s = load(env, b1)
    assert (s["unmapped"], s["skipped"], s["outside_life"], s["new"], s["written"]) == (10, 10, 1, 9, 9)  # 01-03 早于首个交易日

    assert load(env, b1)["written"] == 0                                   # 重跑：没有变化，不写

    fixed = closes()
    fixed[5] = 51.0                                                         # 数据源修正了一天
    s = load(env, make_batch(env, {"ZZQA": fixed}))
    assert (s["changed"], s["unchanged"], s["written"]) == (1, 8, 1)
    cur = env["ch"].query_df(f"SELECT date, close, version FROM {DB}.v_price_daily WHERE close = 51")
    assert cur["version"].tolist() == [2]


# =========================
# ✅ 复权因子 + 复权价 + 和 FMP 核对
# =========================
def record_events(env):
    store, sid = env["store"], env["sids"]["ZZQA"]
    with store.ingestion_run("splits", ["ZZQA"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    store.conn.execute("SELECT * FROM events.record_split('ZZQA', '2022-01-10', 2, 1, 'fmp', 's1', %s, %s, "
                       "p_split_type => 'forward', p_security_id => %s)", (run.batch_id, Jsonb({}), sid))
    store.conn.execute("SELECT * FROM events.record_dividend('ZZQA', '2022-01-12', 1.0, 'fmp', 'd1', %s, %s, "
                       "p_security_id => %s)", (run.batch_id, Jsonb({}), sid))


def test_adjustments_and_adjusted_prices(env):
    load(env, make_batch(env, {"ZZQA": closes()}))
    record_events(env)
    s = gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    assert (s["new"], s["written"], s["bad_factor"]) == (2, 2, 0)
    assert gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))["written"] == 0

    adj = env["ch"].query_df(f"SELECT date, adj_close, adj_volume FROM {DB}.v_price_daily_adjusted ORDER BY date")
    by_day = dict(zip(pd.to_datetime(adj["date"]).dt.date, adj["adj_close"].round(6)))
    # 分红因子 = 1 − 1.0 / 50（除息前一天收盘 50）= 0.98；拆股以前还要再除以 2
    assert by_day[D("2022-01-07")] == pytest.approx(100 * 0.5 * 0.98)
    assert by_day[D("2022-01-11")] == pytest.approx(50 * 0.98)
    assert by_day[D("2022-01-12")] == pytest.approx(50.0)
    vol = dict(zip(pd.to_datetime(adj["date"]).dt.date, adj["adj_volume"]))
    assert vol[D("2022-01-07")] == pytest.approx(2000.0) and vol[D("2022-01-12")] == pytest.approx(1000.0)


def test_check_against_fmp_one_issue_per_segment(env):
    b = make_batch(env, {"ZZQA": closes()})
    load(env, b)
    record_events(env)
    gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    ours = env["ch"].query_df(f"SELECT date, adj_close FROM {DB}.v_price_daily_adjusted ORDER BY date")
    fmp = ours["adj_close"].tolist()
    fmp[2], fmp[3] = fmp[2] * 1.1, fmp[3] * 1.1                            # 连续两天偏差 10%：一段
    raw = pd.DataFrame({"symbol": "ZZQA", "date": pd.to_datetime(ours["date"]).dt.date, "adjOpen": fmp, "adjHigh": fmp,
                        "adjLow": fmp, "adjClose": fmp, "volume": 1000.0, "extra": "{}", "batch_id": "fmp_test"})
    env["ch"].insert_df("raw_test.price_volume_daily", raw)
    s = gl.check_against_fmp(store=env["store"], ch=env["ch"], db=DB, raw_table="raw_test.price_volume_daily")
    assert (s["deviating_days"], s["segments"]) == (2, 1)
    rows = env["store"].conn.execute("SELECT rule_id, symbol, date, detail->>'days' FROM meta.quality_issues "
                                     "WHERE rule_id = 'GD001'").fetchall()
    assert rows == [("GD001", "ZZQA", ours["date"].iloc[2], "2")] or rows[0][3] == "2"


def test_duplicate_events_from_old_and_new_symbol_counted_once(env):
    """改过代码的证券：新旧代码各拉过一遍全部历史，同一笔分红出现两次，只能算一次"""
    load(env, make_batch(env, {"ZZQA": closes()}))
    record_events(env)
    store, sid = env["store"], env["sids"]["ZZQA"]
    with store.ingestion_run("dividends", ["OLDQ"]) as run:
        run.finish({"success": 1, "missing": 0, "failed": 0}, [])
    store.conn.execute("SELECT * FROM events.record_dividend('OLDQ', '2022-01-12', 1.0, 'fmp', 'd-old', %s, %s, "
                       "p_security_id => %s)", (run.batch_id, Jsonb({}), sid))
    s = gl.build_adjustments(store=store, ch=env["ch"], db=DB, today=D("2026-10-13"))
    assert (s["duplicates_dropped"], s["written"]) == (1, 2)
