"""
A + B：从 FMP 拉取，做结构检查，写入 raw 层

结构检查以 raw 表的实际表结构为准（从 ClickHouse 读），不需要为每个接口单独写检查规则：
    - 非 Nullable 的数据列（如 symbol / date）必须存在且不能为空
    - 每列都必须能转换成表里定义的类型（日期能解析、数值能转换）
    - 表里没定义的字段打包成 JSON 放进 extra，不会丢
结构检查只看"形状"，不看"质量"：价格为 0 这类问题照样写入 raw，留给清洗阶段判断。
结构检查不通过时整批不写 raw：原始返回内容存到 data/raw_rejects/{batch_id}.json，批次记为 failed。

用法：
    batch_id = ingest("price_volume", symbols, {"from_date": ..., "to_date": ...})
"""
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config.endpoints import FMP_ENDPOINTS
from config.paths import RAW_REJECTS
from db import ClickHouseStore, MetaStore
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

# raw 表里由我们自己填的追溯字段，不参与结构检查
LINEAGE_COLUMNS = {"extra", "batch_id", "source", "endpoint", "source_logic_version", "ingested_at"}


class SchemaError(Exception):
    """FMP 返回的数据结构和 raw 表对不上（通常是 FMP 改了接口）"""

    def __init__(self, problems: list[str], rejected_path: Path | None = None):
        self.problems = problems
        self.rejected_path = rejected_path
        msg = "结构检查不通过：" + "；".join(problems)
        if rejected_path is not None:
            msg += f"。原始数据已保存到 {rejected_path}"
        super().__init__(msg)


@dataclass(frozen=True)
class RawSchema:
    table: str
    columns: dict[str, str]          # 数据列 -> ClickHouse 类型（不含追溯字段）

    @property
    def required(self) -> list[str]:
        """非 Nullable 的数据列：必须存在、不能为空"""
        return [c for c, t in self.columns.items() if "Nullable" not in t]

    @staticmethod
    def base_type(ch_type: str) -> str:
        """去掉 Nullable(...) / LowCardinality(...) 外壳：Nullable(Float64) -> Float64"""
        while (m := re.fullmatch(r"(?:Nullable|LowCardinality)\((.*)\)", ch_type)):
            ch_type = m.group(1)
        return ch_type

    @classmethod
    def from_clickhouse(cls, ch: ClickHouseStore, table: str) -> "RawSchema":
        database, name = table.split(".", 1)
        df = ch.query_df(
            "SELECT name, type FROM system.columns WHERE database = {db:String} AND table = {tbl:String} ORDER BY position",
            {"db": database, "tbl": name},
        )
        if df.empty:
            raise ValueError(f"ClickHouse 里没有表 {table}，请先执行 sql/clickhouse/ 下的建表语句")
        columns = {n: t for n, t in zip(df["name"], df["type"]) if n not in LINEAGE_COLUMNS}
        return cls(table=table, columns=columns)


def check_structure(df: pd.DataFrame, schema: RawSchema) -> pd.DataFrame:
    """
    结构检查 + 类型转换。通过则返回可直接写入 raw 的 DataFrame（数据列 + extra），
    不通过抛 SchemaError，列出全部问题。
    """
    problems = []

    missing = [c for c in schema.required if c not in df.columns]
    if missing:
        raise SchemaError([f"缺少必需字段 {missing}"])

    out = pd.DataFrame(index=df.index)
    for col, ch_type in schema.columns.items():
        base = RawSchema.base_type(ch_type)
        if col not in df.columns:
            out[col] = None                      # 可空字段 FMP 没给：记为 NULL
            continue

        src = df[col]
        if base.startswith("Date"):
            converted = pd.to_datetime(src, errors="coerce", format="ISO8601")
            values = converted.dt.date if base == "Date" else converted
        elif base.startswith(("Float", "Int", "UInt", "Decimal")):
            converted = values = pd.to_numeric(src, errors="coerce")
        else:
            converted = src
            values = src.where(src.isna(), src.astype(str))

        bad = converted.isna() & src.notna()
        if bad.any():
            problems.append(f"{col}：{int(bad.sum())} 行无法转换为 {base}，例如 {src[bad].iloc[0]!r}")
        if col in schema.required:
            # 只看原值是否为空；有值但转换失败的已经在上面报过了
            empty = src.isna() | (src.astype(str).str.strip() == "")
            if empty.any():
                problems.append(f"{col}：{int(empty.sum())} 行为空")
        out[col] = values

    if problems:
        raise SchemaError(problems)

    extra_cols = [c for c in df.columns if c not in schema.columns and c not in LINEAGE_COLUMNS]
    if extra_cols:
        log.warning(f"[{schema.table}] FMP 返回了表里没定义的字段 {extra_cols}，已存入 extra")
        out["extra"] = [
            json.dumps({k: v for k, v in row.items() if pd.notna(v)}, ensure_ascii=False, default=str)
            for row in df[extra_cols].to_dict(orient="records")
        ]
    else:
        out["extra"] = "{}"
    return out.reset_index(drop=True)


def save_rejected(df: pd.DataFrame, batch_id: str) -> Path:
    RAW_REJECTS.mkdir(parents=True, exist_ok=True)
    path = RAW_REJECTS / f"{batch_id}.json"
    df.to_json(path, orient="records", date_format="iso", force_ascii=False, indent=1)
    return path


def ingest(endpoint: str, symbols: list[str], params: dict | None = None, *,
           store: MetaStore | None = None, ch: ClickHouseStore | None = None,
           raw_table: str | None = None, fetcher_cls=FetchData) -> str:
    """
    A 拉取 + B 写 raw，返回 batch_id。
    meta.ingestion 记录拉取结果；meta.load_log 记录 raw 写入。
    结构检查不通过时抛 SchemaError，批次记为 failed。
    """
    cfg = FMP_ENDPOINTS[endpoint]
    raw_table = raw_table or cfg.get("raw_table")
    if raw_table is None:
        raise ValueError(f"endpoint {endpoint} 没有配置 raw_table（config/endpoints.py）")

    store = store or MetaStore()
    ch = ch or ClickHouseStore()
    schema = RawSchema.from_clickhouse(ch, raw_table)      # 表不存在就在开批次之前报错

    with store.ingestion_run(endpoint, symbols, params) as run:
        # A：拉取
        with fetcher_cls(symbols, endpoint, params) as fetcher:
            df, summary, succeed, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))

        # B：结构检查 → 写 raw
        with store.load_step(run.batch_id, "raw", raw_table) as step:
            if df.empty:
                log.warning(f"[{endpoint}] 批次 {run.batch_id} 没有拉到数据，raw 写入 0 行")
            else:
                try:
                    typed = check_structure(df, schema)
                except SchemaError as e:
                    e.rejected_path = save_rejected(df, run.batch_id)
                    raise SchemaError(e.problems, e.rejected_path) from None
                typed["batch_id"] = run.batch_id
                typed["source"] = "fmp"
                typed["endpoint"] = endpoint
                typed["source_logic_version"] = cfg["source_logic_version"]
                step.rows = ch.insert_df(raw_table, typed)

    log.info(f"[{endpoint}] 批次 {run.batch_id} 写入 raw {step.rows} 行")
    return run.batch_id
