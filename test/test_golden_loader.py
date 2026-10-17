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
    monkeypatch.setattr(gl, "OVERRIDES_PATH", tmp_path / "overrides.csv")      # 不读正式的人工修正
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


def test_check_against_fmp_by_daily_return(env):
    """按每日收益率比：FMP 第 3、4 天都高 10%（一段价格偏差），只有进入和离开这段的两天收益率不同"""
    b = make_batch(env, {"ZZQA": closes()})
    load(env, b)
    record_events(env)
    gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    ours = env["ch"].query_df(f"SELECT date, adj_close FROM {DB}.v_price_daily_adjusted ORDER BY date")
    fmp = ours["adj_close"].tolist()
    fmp[2], fmp[3] = fmp[2] * 1.1, fmp[3] * 1.1
    fmp[-1] = fmp[-1] * 1.5                                                  # 最后一天：拉取时间不同，不比
    raw = pd.DataFrame({"symbol": "ZZQA", "date": pd.to_datetime(ours["date"]).dt.date, "adjOpen": fmp, "adjHigh": fmp,
                        "adjLow": fmp, "adjClose": fmp, "volume": 1000.0, "extra": "{}", "batch_id": "fmp_test"})
    env["ch"].insert_df("raw_test.price_volume_daily", raw)
    s = gl.check_against_fmp(store=env["store"], ch=env["ch"], db=DB, raw_table="raw_test.price_volume_daily")
    assert s["issues"] == 2
    dates = [r[0] for r in env["store"].conn.execute(
        "SELECT date FROM meta.quality_issues WHERE rule_id = 'GD001' ORDER BY date").fetchall()]
    ours_days = pd.to_datetime(ours["date"]).dt.date.tolist()            # ZZQA 首个交易日 01-04，从 DAYS[1] 开始
    assert dates == [ours_days[2], ours_days[4]]


def test_override_drop_retracts_and_restores(env, tmp_path):
    """人工修正删掉一条拆股 → golden 里写 is_active = 0 的新版本；去掉修正后重新生效"""
    load(env, make_batch(env, {"ZZQA": closes()}))
    record_events(env)
    run = lambda: gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    run()
    (tmp_path / "overrides.csv").write_text(
        "action,symbol,kind,ex_date,value,decided_on,note\ndrop,ZZQA,split,2022-01-10,,2026-10-14,测试\n", encoding="utf-8")
    s = run()
    assert (s["overrides"]["dropped"], s["retracted"], s["written"]) == (1, 1, 1)
    f = env["ch"].query_df(f"SELECT ex_date FROM {DB}.v_adjustment_factors")
    assert pd.to_datetime(f["ex_date"]).dt.date.tolist() == [D("2022-01-12")]           # 拆股已撤回，只剩分红
    adj = env["ch"].query_df(f"SELECT adj_close FROM {DB}.v_price_daily_adjusted WHERE date = '2022-01-07'")
    assert adj["adj_close"][0] == pytest.approx(100 * 0.98)                              # 不再除以 2

    (tmp_path / "overrides.csv").write_text("action,symbol,kind,ex_date,value,decided_on,note\n", encoding="utf-8")
    s = run()
    assert (s["changed"], s["retracted"]) == (1, 0)                                      # 重新生效：再写一个新版本
    versions = env["ch"].query_df(f"SELECT version, is_active FROM {DB}.adjustment_events WHERE kind = 'split' ORDER BY version")
    assert list(zip(versions["version"], versions["is_active"])) == [(1, 1), (2, 0), (3, 1)]


def test_override_set_value(env, tmp_path):
    """改值：KMI 那种分红金额被放大 10 倍的，改成正确金额"""
    load(env, make_batch(env, {"ZZQA": closes()}))
    record_events(env)
    (tmp_path / "overrides.csv").write_text(
        "action,symbol,kind,ex_date,value,decided_on,note\nset_value,ZZQA,dividend,2022-01-12,0.5,2026-10-14,测试\n",
        encoding="utf-8")
    s = gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    assert s["overrides"]["set_value"] == 1
    pf = env["ch"].query_df(f"SELECT price_factor FROM {DB}.adjustment_events WHERE kind = 'dividend'")["price_factor"][0]
    assert pf == pytest.approx(1 - 0.5 / 50)


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


def test_override_add_spinoff(env, tmp_path):
    """分拆：和现金分红同一算法，value = 每股分到的价值"""
    load(env, make_batch(env, {"ZZQA": closes()}))
    (tmp_path / "overrides.csv").write_text(
        "action,symbol,kind,ex_date,value,decided_on,note\nadd,ZZQA,spinoff,2022-01-12,10,2026-10-14,测试\n",
        encoding="utf-8")
    s = gl.build_adjustments(store=env["store"], ch=env["ch"], db=DB, today=D("2026-10-13"))
    assert (s["overrides"]["added"], s["written"]) == (1, 1)
    ev = env["ch"].query_df(f"SELECT kind, event_id, price_factor, volume_factor FROM {DB}.adjustment_events")
    assert (ev["kind"][0], ev["event_id"][0]) == ("spinoff", 0)
    assert ev["price_factor"][0] == pytest.approx(1 - 10 / 50) and ev["volume_factor"][0] == 1


def test_pending_batches_only_unloaded(env):
    """每日流程只处理还没写进 golden 的批次"""
    b1 = make_batch(env, {"ZZQA": closes()})
    assert gl.pending_batches(env["store"], db=DB) == [b1]
    load(env, b1)
    assert gl.pending_batches(env["store"], db=DB) == []
    b2 = make_batch(env, {"ZZQA": closes()})
    assert gl.pending_batches(env["store"], db=DB) == [b2]
