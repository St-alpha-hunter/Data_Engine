import json

import numpy as np
import pandas as pd
import pytest

from pipelines import raw_loader
from pipelines.raw_loader import RawSchema, SchemaError, check_structure, ingest

"""

B 步骤测试：
- check_structure：纯计算，不需要数据库
- ingest：拉取用假的 fetcher，写 Postgres 测试库 + ClickHouse 测试库 raw_test

"""

SCHEMA = RawSchema(
    table="raw.price_volume_daily",
    columns={
        "symbol": "LowCardinality(String)", "date": "Date",
        "adjOpen": "Nullable(Float64)", "adjHigh": "Nullable(Float64)", "adjLow": "Nullable(Float64)",
        "adjClose": "Nullable(Float64)", "volume": "Nullable(Float64)",
    },
)


def fmp_df(n=2, **overrides):
    """模拟 FetchData 返回的 df（含 FetchData 自己加的 endpoint 列）"""
    df = pd.DataFrame({
        "symbol": ["AAPL"] * n,
        "date": [f"2022-01-0{3 + i}" for i in range(n)],
        "adjOpen": [100.0] * n, "adjHigh": [102.0] * n, "adjLow": [99.0] * n,
        "adjClose": [101.0] * n, "volume": [1000] * n,
        "endpoint": ["price_volume"] * n,
    })
    for k, v in overrides.items():
        df[k] = v
    return df


# =========================
# ✅ check_structure
# =========================
def test_structure_ok():
    out = check_structure(fmp_df(), SCHEMA)
    assert list(out.columns) == list(SCHEMA.columns) + ["extra"]
    assert out["date"].tolist() == [pd.Timestamp("2022-01-03").date(), pd.Timestamp("2022-01-04").date()]
    assert (out["extra"] == "{}").all()
    assert "endpoint" not in out.columns        # 追溯字段由 ingest 自己填


def test_structure_keeps_quality_problems():
    """价格为 0、价格缺失属于质量问题，不拦，照样进 raw"""
    out = check_structure(fmp_df(adjOpen=[0.0, np.nan]), SCHEMA)
    assert out["adjOpen"].iloc[0] == 0.0
    assert np.isnan(out["adjOpen"].iloc[1])


def test_structure_missing_nullable_column_becomes_null():
    out = check_structure(fmp_df().drop(columns=["volume"]), SCHEMA)
    assert out["volume"].isna().all()


def test_structure_extra_fields_packed():
    out = check_structure(fmp_df(newField=[1, None], note=["a", "b"]), SCHEMA)
    assert json.loads(out["extra"].iloc[0]) == {"newField": 1.0, "note": "a"}
    assert json.loads(out["extra"].iloc[1]) == {"note": "b"}       # 空值不存


@pytest.mark.parametrize("df, message", [
    (fmp_df().drop(columns=["date"]), "缺少必需字段"),
    (fmp_df(symbol=["AAPL", None]), "symbol：1 行为空"),
    (fmp_df(symbol=["AAPL", "  "]), "symbol：1 行为空"),
    (fmp_df(date=["2022-01-03", "not-a-date"]), "date：1 行无法转换为 Date"),
    (fmp_df(date=["2022-01-03", None]), "date：1 行为空"),
    (fmp_df(adjClose=[101.0, "abc"]), "adjClose：1 行无法转换为 Float64"),
])
def test_structure_errors(df, message):
    with pytest.raises(SchemaError, match=message):
        check_structure(df, SCHEMA)


def test_structure_reports_all_problems():
    with pytest.raises(SchemaError) as e:
        check_structure(fmp_df(date=["x", "2022-01-03"], adjClose=[1.0, "y"]), SCHEMA)
    assert len(e.value.problems) == 2


def test_base_type():
    assert RawSchema.base_type("Nullable(Float64)") == "Float64"
    assert RawSchema.base_type("LowCardinality(Nullable(String))") == "String"
    assert SCHEMA.required == ["symbol", "date"]


# =========================
# ✅ ingest：拉取 + 写 raw（集成测试）
# =========================
RAW_TABLE = "raw_test.price_volume_daily"


def fake_fetcher(df, summary=None, failed=None):
    class FakeFetchData:
        def __init__(self, symbols, endpoint_name, extra_params):
            self.symbols = symbols

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def fetch_fmp_batch(self):
            s = summary or {"success": len(self.symbols), "missing": 0, "failed": 0, "missing_symbols": []}
            return df, s, list(self.symbols), failed or []
    return FakeFetchData


def test_ingest_writes_raw_and_meta(store, ch):
    batch_id = ingest("price_volume", ["AAPL"], {"from_date": "2022-01-01", "to_date": "2022-01-31"},
                      store=store, ch=ch, raw_table=RAW_TABLE, fetcher_cls=fake_fetcher(fmp_df(3)))

    raw = ch.query_df(f"SELECT * FROM {RAW_TABLE} WHERE batch_id = {{b:String}} ORDER BY date", {"b": batch_id})
    assert len(raw) == 3
    assert (raw["source"] == "fmp").all() and (raw["endpoint"] == "price_volume").all()
    assert (raw["source_logic_version"] == 1).all()

    ing = store.get_ingestion(batch_id)
    assert ing["status"] == "success" and ing["rows_fetched"] == 3
    [load] = store.get_loads(batch_id)
    assert (load["layer"], load["target_table"], load["rows_written"], load["status"]) == ("raw", RAW_TABLE, 3, "success")


def test_ingest_schema_error_rejects_whole_batch(store, ch, tmp_path, monkeypatch):
    monkeypatch.setattr(raw_loader, "RAW_REJECTS", tmp_path)
    bad = fmp_df(date=["2022-01-03", "garbage"])

    with pytest.raises(SchemaError, match="date") as e:
        ingest("price_volume", ["AAPL"], {"from_date": "2022-01-01", "to_date": "2022-01-31"},
               store=store, ch=ch, raw_table=RAW_TABLE, fetcher_cls=fake_fetcher(bad))

    # 整批不进 raw
    assert ch.query_df(f"SELECT count() AS n FROM {RAW_TABLE}")["n"][0] == 0
    # 原始内容存到本地
    saved = e.value.rejected_path
    assert saved.parent == tmp_path and len(json.loads(saved.read_text(encoding="utf-8"))) == 2

    batch_id = saved.stem
    ing = store.get_ingestion(batch_id)
    assert ing["status"] == "failed"
    assert "结构检查不通过" in ing["failure_detail"][-1]["error"]
    [load] = store.get_loads(batch_id)
    assert load["status"] == "failed" and "date" in load["error"]


def test_ingest_empty_fetch(store, ch):
    empty = pd.DataFrame()
    batch_id = ingest("price_volume", ["AAPL"], {"from_date": "2022-01-01", "to_date": "2022-01-31"},
                      store=store, ch=ch, raw_table=RAW_TABLE,
                      fetcher_cls=fake_fetcher(empty, {"success": 0, "missing": 1, "failed": 0, "missing_symbols": ["AAPL"]}))
    [load] = store.get_loads(batch_id)
    assert (load["rows_written"], load["status"]) == (0, "success")
    assert store.get_ingestion(batch_id)["missing_symbols"] == ["AAPL"]


def test_ingest_requires_raw_table(store, ch):
    with pytest.raises(ValueError, match="raw_table"):
        ingest("market_cap", ["AAPL"], store=store, ch=ch, fetcher_cls=fake_fetcher(fmp_df()))
