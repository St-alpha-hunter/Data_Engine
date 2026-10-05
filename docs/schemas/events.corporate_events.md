# events.corporate_events

> PostgreSQL `data_engine.events` · 建表 SQL：[011_events_corporate_events.sql](../../sql/postgres/011_events_corporate_events.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

公司事件**总表**：拆股、分红、代码变更、成分股变更、收购，所有事件都在这里有一行（每个版本一行），形成统一的事件时间线。

- **原始内容原样保存**在 `payload`，能追溯、能重放
- 各类事件特有的字段（拆股比例、分红金额……）放在各自的明细表里（待建，按 `event_id` 一对一关联）
- 证券主表、成分股区间等状态表，由这里的事件推导出来

## Grain

**一行 = 一个事件的一个版本**。

事件被修改（分红金额改了、收购取消了）时**追加新版本**（`revision + 1`），旧版本保留。按 `(source, source_event_key)` 区分是不是同一个事件。

主键：`event_id`（自增）。唯一键：
- `(source, source_event_key, revision)`：同一事件的版本号唯一
- `(source, source_event_key, payload_hash)`：**内容完全相同的不重复写入**（Kafka 会重复投递）

## Source and lineage

- **写入方式**：一律调用 `events.record_event(...)`，不要直接 `INSERT`。它在数据库里完成：

| 情况 | 结果 | 返回的 action |
|---|---|---|
| 内容和这个事件已有的某个版本完全相同（重复投递，包括旧消息晚到） | 不写入，返回已有版本 | `duplicate` |
| 事件已存在，但内容不同 | 写入新版本，`revision + 1` | `revised` |
| 全新的事件 | 写入，`revision = 1` | `inserted` |

  同一事件加了事务级咨询锁，多个消费者并发写同一事件时会排队，版本号不会冲突或跳号（有并发测试）。

- **"内容"指什么**：`payload_hash` = sha256(事件类型, 代码, 生效日, 状态, 公告时间, 原始内容)。jsonb 会规范化键的顺序，所以键顺序不同但内容相同算重复
- **已知局限**：如果事件内容改成 B 之后又改回 A，第二次的 A 会被当成重复投递（和第一个版本内容相同）。这种情况极少，需要时人工处理
- **来源**（`source`）：`fmp` / `yfinance` / `sp500_github` / `sec` ……；每个来源的拉取都记一个 `meta.ingestion` 批次（`batch_id` 必填）
- **`security_id`**：写入时可为空（证券主表可能还没有这只证券），对应上以后**回填一次**
- **只追加保护**（触发器 `trg_corporate_events_protect`）：
  - 不允许删除
  - 只允许把 `security_id` 从空回填成有值；已有值不能再改
  - 其他字段一律不能改，修改请追加新版本
- **上游**：`batch_id` → `meta.ingestion`；`security_id` → `ref.securities`
- **下游**：`ref.security_identifiers.event_id` 引用本表（某段代码由哪个事件引起）；以后的 5 张明细表按 `event_id` 关联

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `event_id` | bigint | 否 | 自增 | 主键（一个版本一个 id） |
| `event_type` | text | 否 | — | `dividend` / `split` / `symbol_change` / `index_change` / `acquisition` |
| `symbol` | text | 否 | — | 数据源给出的原始代码 |
| `security_id` | bigint | 是 | — | 对应到 `ref.securities` 后回填 |
| `event_date` | date | 否 | — | 生效日：除息日 / 拆股生效日 / 代码变更日 / 成分股变更生效日 / 收购完成日 |
| `announced_at` | timestamptz | 是 | — | 公告时间；数据源不提供则为空 |
| `known_at` | timestamptz | 否 | `now()` | **我们得知这个版本的时间**，按时点查询靠它 |
| `status` | text | 否 | — | `announced` 已公告 / `confirmed` 已确认或已生效 / `cancelled` 已取消 |
| `revision` | integer | 否 | — | 版本号，从 1 开始 |
| `source` | text | 否 | — | 数据来源 |
| `source_event_key` | text | 否 | — | 数据源里的唯一标识；没有则按规则拼，如 `AAPL\|2020-08-31\|split` |
| `payload_hash` | text | 否 | — | 内容哈希（64 位十六进制），`record_event` 计算 |
| `batch_id` | text | 否 | — | 来自哪次拉取 |
| `payload` | jsonb | 否 | — | 原始内容 |

### 约束

| 名称 | 规则 |
|---|---|
| `pk_corporate_events` | `event_id` 唯一 |
| `fk_corporate_events_security` | `security_id` 必须存在于 `ref.securities` |
| `fk_corporate_events_batch` | `batch_id` 必须存在于 `meta.ingestion` |
| `chk_corporate_events_type` | 只能是 5 类事件 |
| `chk_corporate_events_status` | 只能是 3 种状态 |
| `chk_corporate_events_revision_positive` | 版本号 ≥ 1 |
| `chk_corporate_events_symbol_format` | 大写字母、数字、`.`、`-` |
| `chk_corporate_events_key_not_blank` | `source_event_key` 不能为空白 |
| `chk_corporate_events_hash_format` | 64 位十六进制 |
| `uq_corporate_events_revision` | `(source, source_event_key, revision)` 唯一 |
| `uq_corporate_events_content` | `(source, source_event_key, payload_hash)` 唯一 |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_corporate_events_symbol_date` | 按代码、日期查 |
| `idx_corporate_events_type_date` | 按事件类型、日期查 |
| `idx_corporate_events_security` | 按证券查 |
| `idx_corporate_events_unresolved` WHERE `security_id IS NULL` | 待匹配到证券的事件 |
| `idx_corporate_events_known_at` | 按时点查询 |

## 查询接口

| 接口 | 用途 |
|---|---|
| `events.record_event(...)` | 写入事件，返回 `(event_id, revision, action)` |
| `events.v_corporate_events_current` | 每个事件的当前版本 |
| `events.corporate_events_asof(时间)` | 站在某个时间点，每个事件的最新版本（`known_at <=` 该时间） |

```sql
-- 写入（Kafka 消费者、拉取脚本都这样写）
SELECT * FROM events.record_event(
    'symbol_change', 'FB', '2022-06-09', 'confirmed',
    'fmp', 'FB|META|2022-06-09', '<batch_id>',
    '{"oldSymbol": "FB", "newSymbol": "META"}'::jsonb);

-- 某只证券的所有事件（当前版本）
SELECT event_type, event_date, status, payload
FROM events.v_corporate_events_current WHERE security_id = 3 ORDER BY event_date;

-- 回测：站在 2022-05-15，我们知道哪些事件
SELECT * FROM events.corporate_events_asof('2022-05-15') WHERE event_type = 'split';

-- 还没对应到证券的事件
SELECT event_id, symbol, event_type, event_date FROM events.corporate_events WHERE security_id IS NULL;

-- 回填 security_id
UPDATE events.corporate_events SET security_id = ref.resolve_security('symbol', symbol, event_date)
WHERE security_id IS NULL AND ref.resolve_security('symbol', symbol, event_date) IS NOT NULL;
```
