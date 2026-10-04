# meta.load_log

> PostgreSQL `data_engine.meta` · 建表 SQL：[006_meta_load_log.sql](../../sql/postgres/006_meta_load_log.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

记录**每个批次写入了哪些表、写了多少行、成没成功**。

`meta.ingestion` 只回答"从 FMP 拉了什么"，本表回答"拉下来的数据落到了哪里"。两者合起来可以：
- 查一个批次进 raw 了没有、进 golden 了没有
- **对账**：raw 行数 = `ingestion.rows_fetched`；golden + quarantine 行数 = raw 行数。对不上说明中间丢了数据
- 找出最近写入失败的表和原因

分工：`ingestion.status` 只表示"拉取"这一步；每一步写入是否成功，看本表的 `status`。

## Grain

**一行 = 一次写表**（一个批次写入一张表的一次尝试）。

同一个批次、同一张表可以有多行：写入失败后重试，会留下一条 `failed` 和一条 `success`，作为历史保留。

主键：`load_id`（自增）。

## Source and lineage

- **写入方**：[`db/meta_store.py`](../../db/meta_store.py) 的 `MetaStore`
  - `load_step(batch_id, layer, target_table)`：with 写法，块内把写入行数赋给 `step.rows`；正常退出记 `success`，出异常记 `failed`（带异常信息）并继续抛出
  - `log_load(...)`：直接写一行
- **调用位置**（规划中，流程脚本尚未接入）：每次写 raw / golden / quarantine 时调用
- **上游**：`batch_id` → `meta.ingestion`
- **写入顺序**：raw 写入前只做**结构检查**（必需字段存在、类型可转换、`symbol` / `date` 非空）。结构检查不通过时整批不写 raw，`ingestion.status = 'failed'`，本表不产生 raw 记录

| layer | target_table 示例 | 含义 |
|---|---|---|
| `raw` | `raw.price_volume_daily` | FMP 原始数据落库 |
| `golden` | `golden.price_volume_daily` | 清洗后数据落库（含 outbox 写入的放行数据） |
| `quarantine` | `meta.quarantine_records` | danger 数据进隔离区 |

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `load_id` | bigint | 否 | 自增 | 主键 |
| `batch_id` | text | 否 | — | 哪个批次 |
| `layer` | text | 否 | — | `raw` / `golden` / `quarantine` |
| `target_table` | text | 否 | — | 带库名前缀的表名，如 `raw.price_volume_daily` |
| `rows_written` | bigint | 否 | 0 | 本次实际写入行数；失败时为 0 或已写入的部分 |
| `status` | text | 否 | — | `success` / `failed` |
| `error` | text | 是 | — | 失败原因；`success` 时必须为空 |
| `loaded_at` | timestamptz | 否 | `now()` | 写入时间（UTC） |

### 约束

| 约束名 | 规则 |
|---|---|
| `pk_load_log` | `load_id` 唯一 |
| `fk_load_log_batch` | `batch_id` 必须存在于 `meta.ingestion` |
| `chk_load_log_layer` | `layer` 只能是三个值之一 |
| `chk_load_log_status` | `status` 只能是 `success` / `failed` |
| `chk_load_log_rows_nonneg` | `rows_written ≥ 0` |
| `chk_load_log_error_iff_failed` | `failed` 必须有 `error`；`success` 不能有 `error` |
| `chk_load_log_table_qualified` | `target_table` 必须是 `库名.表名` 格式（raw 和 golden 的表名相同，不带前缀分不清） |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_load_log_batch` (`batch_id, loaded_at`) | 查一个批次的所有写入 |
| `idx_load_log_target_table` (`target_table, loaded_at DESC`) | 查某张表最近的写入 |
| `idx_load_log_failed` (`loaded_at DESC`) WHERE `status = 'failed'` | 快速找出写入失败的记录 |

## 常用查询

```sql
-- 对账：每个批次拉取行数、raw 行数、golden + quarantine 行数是否一致
SELECT i.batch_id,
       i.rows_fetched,
       sum(l.rows_written) FILTER (WHERE l.layer = 'raw')                       AS raw_rows,
       sum(l.rows_written) FILTER (WHERE l.layer IN ('golden', 'quarantine'))   AS golden_plus_quarantine,
       i.rows_fetched = sum(l.rows_written) FILTER (WHERE l.layer = 'raw')
         AND sum(l.rows_written) FILTER (WHERE l.layer = 'raw')
           = sum(l.rows_written) FILTER (WHERE l.layer IN ('golden', 'quarantine')) AS reconciled
FROM meta.ingestion i
JOIN meta.load_log l USING (batch_id)
WHERE l.status = 'success'
GROUP BY i.batch_id, i.rows_fetched
ORDER BY i.batch_id DESC;

-- 最近的写入失败
SELECT loaded_at, batch_id, layer, target_table, rows_written, error
FROM meta.load_log
WHERE status = 'failed'
ORDER BY loaded_at DESC
LIMIT 20;
```
