# 数据字典

本文档是 Data_Engine 所有数据表的**全局目录**和**统一字段术语**。每张表的详细说明（用途、粒度、来源、字段）见 [`schemas/`](schemas/) 下的表级文档。

> 维护约定：新建或修改表时，同步更新本目录和对应的表级文档；建表 SQL 按编号放在 `sql/postgres/`（ClickHouse 的放在 `sql/clickhouse/`）。

---

## 1. 存储总览

| 存储 | 连接 | 库 / schema | 放什么 | 写入方式 |
|---|---|---|---|---|
| PostgreSQL 16（Docker） | `localhost:5433`，库 `data_engine` | `meta` | 数据治理工具表：批次、规则、问题、隔离、处理记录 | 行数少，允许按状态更新（`data_corrections` 除外） |
| ClickHouse 24.8（Docker） | `localhost:8123` | `raw` | FMP 原始数据，原样落库 | 只追加 |
| ClickHouse 24.8（Docker） | `localhost:8123` | `golden` | 清洗校验后的数据，可追溯版本 | 只追加，用视图取当前版本 |
| 本地文件 | `data/` | — | 清洗中间态 parquet | 按批次落文件 |

> 注意：本机还有一个原生 PostgreSQL 15（Windows 服务，端口 **5432**），与本项目无关。连接本项目请用 **5433**。

数据流：

```
FMP API ──拉取──▶ raw（ClickHouse）──清洗校验──▶ 中间态 parquet ──▶ golden（ClickHouse）
   │                                    │
   └─▶ meta.ingestion                   ├─▶ meta.quality_issues      （warning / danger 都记）
                                        └─▶ meta.quarantine_records  （danger 被阻断，不进 golden）
                                                   │
                                     人工/规则处理 ─▶ meta.data_corrections
                                                   │
                                     release 后由 outbox 任务写入 golden（quality_status = warning）
```

---

## 2. 表目录

### PostgreSQL · `data_engine.meta`

| 表 | 用途 | 粒度（一行代表） | 建表 SQL | 表级文档 |
|---|---|---|---|---|
| `meta.ingestion` | 记录每次 FMP 拉取 | 一次拉取批次 | [001](../sql/postgres/001_meta_ingestion.sql) | [ingestion](schemas/meta.ingestion.md) |
| `meta.quality_rules` | 校验规则清单（带版本） | 一条规则的一个版本 | [002](../sql/postgres/002_meta_quality.sql) | [quality_rules](schemas/meta.quality_rules.md) |
| `meta.quality_issues` | 校验发现的问题 | 一条记录（ticker × date）触发的一条规则 | [002](../sql/postgres/002_meta_quality.sql) | [quality_issues](schemas/meta.quality_issues.md) |
| `meta.quarantine_records` | 被阻断、不允许直接进 golden 的数据 | 一条被隔离的记录（batch × endpoint × ticker × date） | [003](../sql/postgres/003_meta_quarantine.sql) | [quarantine_records](schemas/meta.quarantine_records.md) |
| `meta.data_corrections` | 对问题 / 隔离数据的处理记录 | 一次处理动作（只追加） | [004](../sql/postgres/004_meta_data_corrections.sql) | [data_corrections](schemas/meta.data_corrections.md) |

约束统一改名见 [005](../sql/postgres/005_rename_constraints.sql)。

### ClickHouse · `raw` / `golden`

| 表 | 状态 |
|---|---|
| `raw.price_volume_daily` | 待建：FMP 日线原始数据 |
| `golden.price_volume_daily` | 待建：日线清洗后数据，带版本字段 |
| `golden.v_price_volume_daily` | 待建：取当前版本的视图，默认读这个 |

### 表之间的关系

```
meta.ingestion (batch_id)
   ├──< meta.quality_issues.batch_id ──> meta.quality_rules (rule_id, rule_version)
   ├──< meta.quarantine_records.batch_id
   └──< meta.data_corrections.new_batch_id          （refetch 时的新批次）

meta.data_corrections ──> meta.quarantine_records.quarantine_id   （release / discard）
                      ──> meta.quality_issues.issue_id            （refetch / manual_fix）
                      ──> meta.quality_rules (rule_id, rule_version)（规则自动处理时）

meta.quarantine_records ⟷ meta.quality_issues    通过 (batch_id, ticker, date) 关联，无外键
```

---

## 3. 统一字段术语

同名字段在所有表里含义一致。新建表时优先复用下面的名字和类型。

### 3.1 标识与追溯

| 字段 | 类型 | 含义 | 规则 |
|---|---|---|---|
| `batch_id` | text | 一次 FMP 拉取的批次号 | 格式 `{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}`，如 `price_volume_20261003_123000_a1b2`；主表是 `meta.ingestion` |
| `endpoint` | text | FMP 接口名 | 取值必须是 `config/endpoints.py` 里 `FMP_ENDPOINTS` 的 key，如 `price_volume`、`market_cap` |
| `issue_id` | bigint | 一条质量问题的编号 | 主表是 `meta.quality_issues`，自增 |
| `quarantine_id` | bigint | 一条隔离记录的编号 | 主表是 `meta.quarantine_records`，自增 |
| `correction_id` | bigint | 一次处理动作的编号 | 主表是 `meta.data_corrections`，自增 |
| `rule_id` + `rule_version` | text + integer | 校验规则及其版本 | 两者总是成对出现；`rule_id` 格式为「接口缩写 + 3 位序号」，如 `PV001`（PV = price_volume） |
| `git_version` | text | 运行时代码的 git commit | 用于追溯是哪版代码产生的数据 |

### 3.2 业务键

| 字段 | 类型 | 含义 | 规则 |
|---|---|---|---|
| `ticker` | text | 股票代码 | meta 表统一用 `ticker`；FMP 返回的数据和 raw / golden 表里沿用 FMP 的字段名 `symbol`，两者含义相同 |
| `date` | date | 交易日（数据所属日期） | 美东交易日，不带时区；不是入库时间 |

### 3.3 状态与等级

| 字段 | 取值 | 含义 |
|---|---|---|
| `severity`（问题的严重程度） | `warning` | 可疑但可能是真实行情（如财报日大涨跌），**数据照常进 golden 并打标** |
|  | `danger` | 明确错误（如价格 ≤ 0、最高价 < 最低价），**数据进隔离区，不进 golden** |
| `quality_status`（golden 数据的质量标记） | `health` | 未触发任何规则 |
|  | `warning` | 触发了 warning 规则，或是被人工放行的 danger 数据 |
|  | `danger` | 不会出现在 golden 里（danger 数据都在隔离区） |
| `ingestion.status` | `running` / `success` / `partial` / `failed` | 进行中 / 全部成功 / 部分失败 / 全部失败或中途报错。返回空数据（missing）不算失败 |
| `quarantine_records.status` | `pending` / `released` / `discarded` | 待处理 / 已放行 / 已丢弃 |
| `data_corrections.action` | `release` / `discard` / `refetch` / `manual_fix` | 放行隔离数据 / 丢弃隔离数据 / 重新拉取 / 人工修正数值 |

**默认读取口径**：golden 默认读 `health` + `warning`。只要"最干净"的数据时，再手动加 `WHERE quality_status = 'health'`。不要默认只读 `health`，否则会把财报日、危机日等真实极端行情排除掉，导致回测偏差。

### 3.4 版本字段（golden 表专用，待建）

| 字段 | 含义 | 什么时候变 |
|---|---|---|
| `data_version` | 我们这边的数据版次 | 通常为 1；表结构或清洗逻辑大规模重构时升到 2 |
| `source_logic_version` | 数据源的计算口径版本 | FMP 改了计算逻辑（如复权方法）时加 1，用于审计和回滚 |
| `batch_id` | 这一版数据来自哪次拉取 | 每次拉取 / 补跑都不同 |
| `quality_status` | 质量标记 | 见 3.3 |

当前版本**不存字段**（没有 `is_current` 列），而是由视图计算：对每个 `(symbol, date)`，取 `data_version` 最大、再取 `ingested_at` 最新的一行。补跑数据一律**追加新版本**，不删除旧版本。

### 3.5 时间字段

所有时间字段都是 `timestamptz`，按 **UTC** 存储。

| 字段 | 含义 |
|---|---|
| `started_at` / `finished_at` | 拉取开始 / 结束时间 |
| `detected_at` | 发现问题的时间 |
| `quarantined_at` | 进入隔离区的时间 |
| `resolved_at` | 隔离记录被处理（放行或丢弃）的时间 |
| `golden_written_at` | 被放行的数据写入 golden 的时间（outbox 标记） |
| `corrected_at` | 处理动作发生的时间 |
| `created_at` | 记录创建时间（规则表） |
| `ingested_at` | 数据入库时间（ClickHouse 表，待建） |

### 3.6 JSON 字段

| 字段 | 所在表 | 内容 |
|---|---|---|
| `params` | `ingestion` | 请求参数，如 `{"from_date": "2022-01-01", "to_date": "2025-01-01"}` |
| `params` | `quality_rules` | 规则阈值，如 `{"max_range": 0.15}` |
| `failure_detail` | `ingestion` | `fetch_fmp_batch()` 返回的 `failed_list` 原样存放 |
| `detail` | `quality_issues` | 出问题的具体数值，如 `{"adjLow": 0}` |
| `record` | `quarantine_records` | 被阻断的整行原始数据 |
| `corrected_values` | `data_corrections` | 人工修正前后的值：`{"before": {...}, "after": {...}}` |

### 3.7 约束命名

| 前缀 | 类型 | 例子 |
|---|---|---|
| `pk_` | 主键 | `pk_ingestion` |
| `uq_` | 唯一约束 / 唯一索引 | `uq_quarantine_records_record` |
| `fk_` | 外键 | `fk_quality_issues_batch` |
| `chk_` | 检查约束 | `chk_quarantine_discarded_not_written` |
| `idx_` | 普通索引 | `idx_ingestion_endpoint_started` |
| `trg_` | 触发器 | `trg_data_corrections_append_only` |
