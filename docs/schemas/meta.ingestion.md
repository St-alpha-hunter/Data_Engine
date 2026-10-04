# meta.ingestion

> PostgreSQL `data_engine.meta` · 建表 SQL：[001_meta_ingestion.sql](../../sql/postgres/001_meta_ingestion.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

记录每一次 FMP 拉取：拉了哪个接口、多少只股票、成功 / 无数据 / 失败各多少、用的什么参数和代码版本、错误报告在哪。

它是整个数据治理的**起点**：raw / golden 里的每一行数据、每一条质量问题和隔离记录，都能通过 `batch_id` 追溯到这里。

典型用途：
- 查某个接口最近几次拉取是否成功
- 找出卡在 `running`（程序中途崩溃）或 `partial`（部分失败）的批次，安排重跑
- 审计某一批数据是用哪版代码、什么参数拉下来的

## Grain

**一行 = 一次拉取批次**（一次 `FetchData.fetch_fmp_batch()` 调用）。

主键：`batch_id`。

## Source and lineage

- **写入方**：[`db/meta_store.py`](../../db/meta_store.py) 的 `MetaStore.ingestion_run()`（with 写法），由 [`pipelines/raw_loader.py`](../../pipelines/raw_loader.py) 的 `ingest()` 调用，包住 A 拉取 + B 写 raw：
  1. 进入 with：插入一行 `batch_id`、`endpoint`、`params`、`n_symbols`、`git_version`，`status` 自动为 `running`
  2. 拉取结束：`run.finish()` 用 `FetchData.fetch_fmp_batch()` 返回的 `summary`、`failed_list` 准备好计数、列表和状态
  3. 退出 with：写入结果和 `finished_at`；中途报错（包括结构检查不通过）记为 `failed`，异常信息追加到 `failure_detail`
  4. `report`：C 步骤 `run_clean()` 发现问题时，用 `FeedErrorDeputy.generate_report()` 生成 JSON 错误报告，通过 `MetaStore.set_report()` 回填
- **上游**：FMP API（[`config/endpoints.py`](../../config/endpoints.py) 的 `FMP_ENDPOINTS`）
- **下游**（通过 `batch_id` 引用本表）：
  - `meta.quality_issues.batch_id`
  - `meta.quarantine_records.batch_id`
  - `meta.load_log.batch_id`（这个批次写入了哪些表、多少行）
  - `meta.data_corrections.new_batch_id`
  - ClickHouse `raw.*` / `golden.*` 的 `batch_id`（待建）

`status` 判定规则：

| status | 条件 |
|---|---|
| `running` | 拉取进行中；如果长时间停在这里，说明程序中途崩溃 |
| `success` | `n_failed = 0` |
| `partial` | `0 < n_failed < n_symbols` |
| `failed` | 全部失败，或程序中途报错 |

返回空数据（`n_missing`）**不算失败**，例如股票在请求的时间段里还没上市。

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `batch_id` | text | 否 | — | 主键。格式 `{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}`，如 `price_volume_20261003_123000_a1b2` |
| `endpoint` | text | 否 | — | FMP 接口名，`FMP_ENDPOINTS` 的 key |
| `params` | jsonb | 否 | `{}` | 请求参数，如 `{"from_date": "2022-01-01", "to_date": "2025-01-01"}` |
| `n_symbols` | integer | 否 | — | 请求的股票数 |
| `n_success` | integer | 否 | 0 | 成功拉到数据的股票数 |
| `n_missing` | integer | 否 | 0 | 返回空数据的股票数 |
| `n_failed` | integer | 否 | 0 | 失败的股票数 |
| `failed_symbols` | text[] | 否 | `{}` | 失败的股票列表 |
| `missing_symbols` | text[] | 否 | `{}` | 返回空数据的股票列表 |
| `failure_detail` | jsonb | 否 | `[]` | `failed_list` 原样存放，每项包含 `symbol / endpoint / error_type / status_code / attempts / error` |
| `rows_fetched` | bigint | 否 | 0 | 一共拉到的行数 |
| `status` | text | 否 | `running` | `running` / `success` / `partial` / `failed` |
| `started_at` | timestamptz | 否 | `now()` | 开始时间（UTC） |
| `finished_at` | timestamptz | 是 | — | 结束时间（UTC）；`running` 时为空 |
| `git_version` | text | 是 | — | 运行时代码的 git commit |
| `report` | text | 是 | — | `FeedErrorDeputy` 生成的错误报告 JSON 文件路径 |

### 约束

| 约束名 | 规则 |
|---|---|
| `pk_ingestion` | `batch_id` 唯一 |
| `chk_ingestion_status` | `status` 只能是四个值之一 |
| `chk_ingestion_n_symbols_nonneg` / `_n_success_` / `_n_missing_` / `_n_failed_` / `_rows_fetched_nonneg` | 各计数 ≥ 0 |
| `chk_ingestion_counts_le_symbols` | `n_success + n_missing + n_failed ≤ n_symbols` |
| `chk_ingestion_finished_after_started` | `finished_at` 不早于 `started_at` |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_ingestion_endpoint_started` (`endpoint, started_at DESC`) | 查某接口最近的批次 |
| `idx_ingestion_status` (`status`) WHERE `status <> 'success'` | 快速找出卡住 / 不完整的批次 |

## 常用查询

```sql
-- 某接口最近 10 次拉取
SELECT batch_id, status, n_symbols, n_success, n_missing, n_failed, started_at
FROM meta.ingestion
WHERE endpoint = 'price_volume'
ORDER BY started_at DESC
LIMIT 10;

-- 需要处理的批次：卡住超过 1 小时的 running，以及 partial / failed
SELECT batch_id, endpoint, status, started_at, failed_symbols
FROM meta.ingestion
WHERE (status = 'running' AND started_at < now() - interval '1 hour')
   OR status IN ('partial', 'failed');
```
