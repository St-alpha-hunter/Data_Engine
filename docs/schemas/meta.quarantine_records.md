# meta.quarantine_records

> PostgreSQL `data_engine.meta` · 建表 SQL：[003_meta_quarantine.sql](../../sql/postgres/003_meta_quarantine.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

隔离区：存放触发了 `danger` 规则、**不允许直接进 golden** 的数据。每条被隔离的数据保留完整的原始数据行，等待人工或规则处理：

- **放行**（`released`）：确认数据没问题，由 outbox 任务写入 golden，`quality_status` 标为 `warning`
- **丢弃**（`discarded`）：确认数据有误，永远不进 golden

本表只记**当前状态**；谁处理的、为什么、怎么处理的，记在 `meta.data_corrections`。

## Grain

**一行 = 一条被隔离的数据（batch × endpoint × symbol × date）**。

同一条数据同时触发多条 danger 规则，**只隔离一次**；触发了哪些规则，用 `(batch_id, symbol, date)` 去 `meta.quality_issues` 查。

主键：`quarantine_id`（自增）。唯一键：`(batch_id, endpoint, symbol, date)`。

## Source and lineage

- **产生**：清洗类的 `validate()` 把触发 danger 规则的行放进 `CleanResult.quarantine_df`，这些行不会进入 `clean_df`（也就不会进 golden）
- **写入方**：[`pipelines/clean_runner.py`](../../pipelines/clean_runner.py) 的 `run_clean()` 调用 `MetaStore.record_validation()`，**和问题记录在同一个事务里写入**，`status = 'pending'`。分开写的话，中途失败会出现"问题记下了、数据却没进隔离区"，这些 danger 行既不在隔离区也不在 golden，凭空消失
- **状态变更**：`MetaStore.release()` / `MetaStore.discard()` ✅，在**同一个 Postgres 事务**里：
  1. 插入一条 `meta.data_corrections`
  2. 更新本表的 `status` 和 `resolved_at`
- **写入 golden**（outbox 模式；`MetaStore.pending_outbox()` / `mark_written()` 已就绪，定时任务待写）：定时任务扫描 `status = 'released' AND golden_written_at IS NULL` 的行，写入 ClickHouse golden（`quality_status = 'warning'`），成功后回填 `golden_written_at`；失败不回填，下次重试。golden 是"追加新版本、按版本取最新"的设计，重复写入不会产生重复数据
- **上游**：`batch_id` → `meta.ingestion`
- **下游**：
  - `meta.data_corrections.quarantine_id` 引用本表
  - ClickHouse `golden.*`（放行后写入，待建）

状态流转：

```
pending ──release()──▶ released ──outbox 任务──▶ released + golden_written_at
   │
   └────discard()────▶ discarded（终态，不会进 golden）
```

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `quarantine_id` | bigint | 否 | 自增 | 主键 |
| `batch_id` | text | 否 | — | 哪一次拉取的数据 |
| `endpoint` | text | 否 | — | 数据类别，如 `price_volume` |
| `symbol` | text | 否 | — | 股票代码 |
| `date` | date | 否 | — | 交易日 |
| `record` | jsonb | 否 | — | 被阻断的整行原始数据，如 `{"adjOpen": 0, "adjHigh": 175.3, ...}` |
| `status` | text | 否 | `pending` | `pending` / `released` / `discarded` |
| `quarantined_at` | timestamptz | 否 | `now()` | 进入隔离区的时间（UTC） |
| `resolved_at` | timestamptz | 是 | — | 处理完成时间；`pending` 时为空 |
| `golden_written_at` | timestamptz | 是 | — | 写入 golden 的时间；`released` 且为空 = 等待写入 |

### 约束

| 约束名 | 规则 |
|---|---|
| `pk_quarantine_records` | `quarantine_id` 唯一 |
| `uq_quarantine_records_record` | `(batch_id, endpoint, symbol, date)` 唯一，同一条数据只隔离一次 |
| `fk_quarantine_records_batch` | `batch_id` 必须存在于 `meta.ingestion` |
| `chk_quarantine_status` | `status` 只能是三个值之一 |
| `chk_quarantine_resolved_iff_not_pending` | `pending` 时 `resolved_at` 必须为空；`released` / `discarded` 时必须有值 |
| `chk_quarantine_discarded_not_written` | 只有 `released` 的数据才能有 `golden_written_at`，**丢弃的数据不能写进 golden** |
| `chk_quarantine_resolved_after_quarantined` | `resolved_at` 不早于 `quarantined_at` |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_quarantine_outbox` (`resolved_at`) WHERE `status = 'released' AND golden_written_at IS NULL` | outbox 任务扫描待写入的数据 |
| `idx_quarantine_pending` (`quarantined_at`) WHERE `status = 'pending'` | 列出待处理队列 |
| `idx_quarantine_symbol_date` (`symbol, date`) | 按股票、日期查 |

## 常用查询

```sql
-- 待处理队列，附带触发的规则
SELECT q.quarantine_id, q.symbol, q.date, q.record,
       array_agg(i.rule_id ORDER BY i.rule_id) AS rules
FROM meta.quarantine_records q
JOIN meta.quality_issues i
  ON i.batch_id = q.batch_id AND i.symbol = q.symbol AND i.date = q.date
WHERE q.status = 'pending'
GROUP BY q.quarantine_id
ORDER BY q.quarantined_at;

-- outbox：已放行但还没写入 golden
SELECT quarantine_id, endpoint, symbol, date, record
FROM meta.quarantine_records
WHERE status = 'released' AND golden_written_at IS NULL;
```
