# golden.price_daily（含视图 v_price_daily / v_price_daily_adjusted）

> ClickHouse `golden` · 建表 SQL：[013](../../sql/clickhouse/013_golden_price_daily.sql)、视图 [015](../../sql/clickhouse/015_golden_v_price_daily.sql)、[017](../../sql/clickhouse/017_golden_v_price_daily_adjusted.sql) · 写入：[`pipelines/golden_loader.py`](../../pipelines/golden_loader.py) · 返回 [数据字典](../data-dictionary.md)

## Purpose

日线的**主数据**：经过清洗校验的**不复权**价格和成交量，按**证券编号**存，带版本。回测读视图 `golden.v_price_daily_adjusted`（复权价）。

为什么存不复权价、不存复权价：

| | 存复权价 | 存不复权价（现在的做法） |
|---|---|---|
| 历史会不会被改写 | 每次分红，这只股票**以前所有日子**的复权价都会变 | 当时的成交价一旦确定就不变，只有数据被修正时才有新版本 |
| 按时点查询 | 难 | 可以：用当时能看到的事件现算复权价 |
| 复权价从哪来 | 直接用 FMP 的 | 不复权价 × 复权因子（[golden.adjustment_events](golden.adjustment_events.md) 由事件库算出），FMP 复权价只用来**核对** |

为什么按证券编号存、不按代码存：代码会改名、会被别的公司重复使用。按代码存时，FMP 会把**新公司的数据接在老公司后面**（实测 NSM、LLL、EP 等），按编号存再按存续期截断就不会串。

## Grain

**一行 = 一只证券一天的一个版本**（security_id × date × version）。当前值看视图 `golden.v_price_daily`（每只证券每天一行）。

## Source and lineage

`raw.price_volume_unadjusted_daily`（FMP `historical-price-eod/non-split-adjusted`）→ 清洗（PV001–PV005，和复权日线共用）→ 中间态 parquet → **D 步骤**写入本表。

D 步骤（`load_prices(batch_id)`）按顺序：

1. 读这个批次的中间态（health / warning），加上人工放行的隔离行（`quality_status = released`），写完回填 `golden_written_at`
2. **代码 → 证券编号**：和补数据用同一套对应（`pipelines/backfill.build_universe`：每只证券最后使用的代码；代码现在属于别的证券或主表以外的公司的不要）
3. **跳过** `config/golden_skip.csv` 里的证券（存续期待核对，目前 16 只）
4. **按存续期截断**：首个交易日之前、最后交易日之后的行不写
5. **和当前版本比对**：没有的写 version 1；任一数值相对变化 > 0.0001% 或 `quality_status` 变了写新版本；没变跳过。所以**可以重复执行**
6. 记 `meta.load_log`（layer = `golden`）

写完后接着 `build_adjustments()` 算复权因子、`check_against_fmp()` 核对（见 [golden.adjustment_events](golden.adjustment_events.md)）。

**2026-10-14 首次写入**：17 个批次，约 266 万行，**711 只证券**（补数据名单 787 只：60 只已退市证券 FMP 完全没有数据，16 只在跳过名单里）。

## Schemas

| 字段 | 类型 | 说明 |
|---|---|---|
| `security_id` | UInt64 | `ref.securities.security_id` |
| `date` | Date32 | 交易日 |
| `open` / `high` / `low` / `close` | Nullable(Float64) | **不复权**价格 |
| `volume` | Nullable(Float64) | 当时真实成交量（拆股前没有按比例放大） |
| `quality_status` | LowCardinality(String) | `health` / `warning`（触发了 warning 规则）/ `released`（隔离后人工放行） |
| `source` | LowCardinality(String) | `fmp` |
| `source_symbol` | LowCardinality(String) | 向 FMP 请求时用的代码（追溯用；不是这只证券当天的代码） |
| `batch_id` | LowCardinality(String) | 来自哪个拉取批次（`meta.ingestion`） |
| `source_logic_version` | UInt16 | FMP 计算口径版本 |
| `version` | UInt32 | 同一证券同一天第几个版本，从 1 开始 |
| `loaded_at` | DateTime64(3, 'UTC') | 写入 golden 的时间；按时点查询用它 |

排序键 `(security_id, date, version)`，按年分区。约束：`security_id > 0`、`version >= 1`、`quality_status` 只能是三个值之一。

## 视图

| 视图 | 内容 |
|---|---|
| `golden.v_price_daily` | 每只证券每天的**当前版本**（不复权）。内层最大版本号用别名 `v`——直接叫 `version` 的话，`argMax(…, version)` 会被 ClickHouse 解析成这个别名 |
| `golden.v_price_daily_adjusted` | **回测用复权价**：`adj_close = close × price_factor`，`adj_volume = volume × volume_factor`。因子取这一天之后最近一个除权日的累积因子（ASOF JOIN），之后再没有事件的日子因子为 1。向最新一天对齐，和 FMP 复权价同一口径 |

## 常用查询

```sql
-- 某只证券的复权日线（回测用）
SELECT date, adj_open, adj_high, adj_low, adj_close, adj_volume, quality_status
FROM golden.v_price_daily_adjusted WHERE security_id = 4454 ORDER BY date;

-- 按代码查：先用证券主表把代码（在那一天）对应到 security_id
SELECT * FROM golden.v_price_daily_adjusted
WHERE security_id = (SELECT security_id FROM postgresql('postgres:5432', 'data_engine', 'security_identifiers', '…', '…', 'ref')
                     WHERE id_type = 'symbol' AND id_value = 'GOOGL' AND valid_to IS NULL);

-- 某一天被修正过的数据（有多个版本）
SELECT security_id, date, count() AS versions FROM golden.price_daily GROUP BY security_id, date HAVING versions > 1;
```

## 已知问题

- **FMP 不复权价自己也有错误报价**：如 AVB 2013-01-07、VTR 2015-08-18 不复权价无故上涨 13%，FMP 复权价却没动。由和 FMP 的核对（GD001）抓出，记为 warning，还没修
- `config/golden_skip.csv` 里 16 只证券的存续期待核对，核对完一只删一行，再跑 `golden_loader all` 就会写入
