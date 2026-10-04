# raw.price_volume_daily

> ClickHouse `raw` 库 · 建表 SQL：[001_raw_price_volume_daily.sql](../../sql/clickhouse/001_raw_price_volume_daily.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

FMP 日线行情的**原始数据层**：FMP 返回什么就存什么，外加追溯字段。

- 是清洗（C 步骤）的**唯一输入**：清洗按 `batch_id` 从本表读数据，不直接用拉取时内存里的 DataFrame
- 改了清洗规则后，可以对本表里已有的批次重新清洗，**不需要再调 FMP**
- 以后有人问"FMP 当时到底给了什么"，本表就是原始证据

写入前只做**结构检查**，不做质量检查：价格为 0、价格缺失这类数据照样写入，交给清洗阶段判定。

## Grain

**一行 = 一个批次里一只股票一天的数据**（batch × symbol × date）。

**只追加、不去重**：同一个 `(symbol, date)` 被拉了 3 次，就存 3 行，用 `batch_id` 区分。去重、版本、`quality_status` 都在 golden 层处理。

没有主键（ClickHouse MergeTree 不强制唯一）。排序键：`(batch_id, symbol, date)`。

## Source and lineage

- **上游**：FMP `historical-price-eod/dividend-adjusted`（[`config/endpoints.py`](../../config/endpoints.py) 的 `price_volume`，已按分红复权）
- **写入方**：[`pipelines/raw_loader.py`](../../pipelines/raw_loader.py) 的 `ingest("price_volume", symbols, params)`
  1. A：`FetchData` 拉取，结果记入 `meta.ingestion`
  2. B：`check_structure()` 按本表结构做检查和类型转换，通过后写入本表，记入 `meta.load_log`（layer = `raw`）
- **结构检查规则**（直接从本表的表结构推导，不需要单独维护）：
  - 非 Nullable 的数据列（`symbol`、`date`）必须存在且不能为空
  - 每列都必须能转换成表里定义的类型
  - 表里没定义的字段打包成 JSON 存进 `extra`
  - 任何一条不满足：**整批不写入**，原始返回内容存到 `data/raw_rejects/{batch_id}.json`，`meta.ingestion.status = 'failed'`
- **下游**：[`pipelines/clean_runner.py`](../../pipelines/clean_runner.py) 的 `run_clean(batch_id)` 按 batch_id 读取，用 `PriceVolume.validate()` 清洗 → `meta.quality_issues` / `meta.quarantine_records` / 中间态 parquet → golden（待建）

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `symbol` | LowCardinality(String) | 否 | — | 股票代码 |
| `date` | Date | 否 | — | 交易日 |
| `adjOpen` | Nullable(Float64) | 是 | — | 复权开盘价；FMP 没给就是 NULL（不会被填成 0） |
| `adjHigh` | Nullable(Float64) | 是 | — | 复权最高价 |
| `adjLow` | Nullable(Float64) | 是 | — | 复权最低价 |
| `adjClose` | Nullable(Float64) | 是 | — | 复权收盘价 |
| `volume` | Nullable(Float64) | 是 | — | 成交量 |
| `extra` | String | 否 | `'{}'` | FMP 多返回的、表里没定义的字段，JSON 字符串 |
| `batch_id` | LowCardinality(String) | 否 | — | 对应 `meta.ingestion.batch_id` |
| `source` | LowCardinality(String) | 否 | `'fmp'` | 数据源 |
| `endpoint` | LowCardinality(String) | 否 | `'price_volume'` | FMP 接口名 |
| `source_logic_version` | UInt16 | 否 | 1 | FMP 计算口径版本，写入时取自 `FMP_ENDPOINTS["price_volume"]["source_logic_version"]` |
| `ingested_at` | DateTime64(3, 'UTC') | 否 | `now64(3)` | 入库时间（UTC） |

字段名与 FMP 返回的保持一致（驼峰写法）。

### 约束

ClickHouse 往非 Nullable 列写 NULL 时**不会报错，而是静默转成默认值**（`''` / `1970-01-01`）。下面三个约束是最后一道防线：

| 约束名 | 规则 |
|---|---|
| `chk_symbol_not_empty` | `symbol != ''` |
| `chk_date_not_default` | `date > 1970-01-01` |
| `chk_batch_id_not_empty` | `batch_id != ''` |

### 表引擎

| 设置 | 值 | 原因 |
|---|---|---|
| 引擎 | `MergeTree` | 只追加、不去重 |
| 分区 | `toYYYYMM(ingested_at)` | 按入库月份分区，以后清理旧批次可以直接删分区 |
| 排序键 | `(batch_id, symbol, date)` | 主要用法是"按 batch_id 读一批"，放最前面查询最快 |

## 常用查询

```sql
-- 清洗时读一批（C 步骤）
SELECT * FROM raw.price_volume_daily WHERE batch_id = 'price_volume_20261004_083410_c417';

-- 某只股票某天被拉过几次、每次的值
SELECT batch_id, ingested_at, adjOpen, adjHigh, adjLow, adjClose, volume
FROM raw.price_volume_daily
WHERE symbol = 'AAPL' AND date = '2022-01-03'
ORDER BY ingested_at;

-- FMP 有没有返回过新字段
SELECT batch_id, extra, count() AS n
FROM raw.price_volume_daily
WHERE extra != '{}'
GROUP BY batch_id, extra;

-- 对账：raw 行数 vs meta.load_log 记录的行数（load_log 在 Postgres，需分别查）
SELECT batch_id, count() AS raw_rows FROM raw.price_volume_daily GROUP BY batch_id ORDER BY batch_id DESC;
```
