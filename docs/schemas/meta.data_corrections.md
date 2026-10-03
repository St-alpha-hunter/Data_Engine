# meta.data_corrections

> PostgreSQL `data_engine.meta` · 建表 SQL：[004_meta_data_corrections.sql](../../sql/postgres/004_meta_data_corrections.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

数据问题的**处理记录**：每一次放行、丢弃、重新拉取、人工修正，都记一行。记录最终判定（为什么这样处理）、处理人或规则版本、重跑批次、修复时间。

本表**只追加、不修改**，由触发器在数据库层面禁止 UPDATE 和 DELETE。录错了就再追加一条新记录来更正，处理历史完整保留，不会被篡改。

## Grain

**一行 = 一次处理动作**。

同一个问题或同一条隔离数据可以有多行，例如先放行、后来发现不对又重新拉取。按 `corrected_at` 排序就是完整的处理历史。

主键：`correction_id`（自增）。

## Source and lineage

- **写入方**（规划中，代码尚未接入）：
  - `release(quarantine_id, reason, decided_by)` / `discard(...)`：与 `meta.quarantine_records` 的状态更新放在**同一个 Postgres 事务**里，要么都成功，要么都不生效
  - refetch / manual_fix 的处理函数
  - 规则自动处理时，`decided_by = 'auto'`，并填 `rule_id` / `rule_version`
- **上游**：
  - `quarantine_id` → `meta.quarantine_records`（release / discard）
  - `issue_id` → `meta.quality_issues`（refetch / manual_fix）
  - `(rule_id, rule_version)` → `meta.quality_rules`
  - `new_batch_id` → `meta.ingestion`（refetch 产生的新批次）

处理动作与对象：

| action | 处理对象 | 必填 |
|---|---|---|
| `release` | 被隔离的数据，放行进 golden | `quarantine_id` |
| `discard` | 被隔离的数据，丢弃 | `quarantine_id` |
| `refetch` | 问题或隔离数据，重新拉取 | `new_batch_id` |
| `manual_fix` | 问题或隔离数据，人工修正数值 | `corrected_values` |

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `correction_id` | bigint | 否 | 自增 | 主键 |
| `quarantine_id` | bigint | 是 | — | 处理的是哪条被隔离的数据 |
| `issue_id` | bigint | 是 | — | 处理的是哪个问题 |
| `action` | text | 否 | — | `release` / `discard` / `refetch` / `manual_fix` |
| `reason` | text | 否 | — | 最终判定：为什么这样处理，如"核对雅虎财经，当天因财报暴跌 18%，属实" |
| `decided_by` | text | 否 | — | 处理人：人名，或 `auto`（规则自动处理） |
| `rule_id` | text | 是 | — | 规则自动处理时：哪条规则 |
| `rule_version` | integer | 是 | — | 规则自动处理时：哪个版本 |
| `new_batch_id` | text | 是 | — | refetch 时重新拉取的批次号 |
| `corrected_values` | jsonb | 是 | — | manual_fix 时修改前后的值：`{"before": {...}, "after": {...}}` |
| `corrected_at` | timestamptz | 否 | `now()` | 处理时间（UTC） |

### 约束

| 约束名 | 规则 |
|---|---|
| `pk_data_corrections` | `correction_id` 唯一 |
| `fk_data_corrections_quarantine` | `quarantine_id` 必须存在于 `meta.quarantine_records` |
| `fk_data_corrections_issue` | `issue_id` 必须存在于 `meta.quality_issues` |
| `fk_data_corrections_rule` | `(rule_id, rule_version)` 必须存在于 `meta.quality_rules` |
| `fk_data_corrections_new_batch` | `new_batch_id` 必须存在于 `meta.ingestion` |
| `chk_data_corrections_has_target` | `quarantine_id` 和 `issue_id` 至少填一个 |
| `chk_data_corrections_action` | `action` 只能是四个值之一 |
| `chk_data_corrections_quarantine_action` | `release` / `discard` 必须填 `quarantine_id` |
| `chk_data_corrections_refetch_batch` | `refetch` 必须填 `new_batch_id` |
| `chk_data_corrections_manual_values` | `manual_fix` 必须填 `corrected_values` |
| `chk_data_corrections_rule_pair` | `rule_id` 和 `rule_version` 要么都填，要么都不填 |
| `chk_data_corrections_reason_not_blank` | `reason` 不能为空白 |
| `chk_data_corrections_decided_by_not_blank` | `decided_by` 不能为空白 |

### 触发器

| 触发器 | 作用 |
|---|---|
| `trg_data_corrections_append_only` | 禁止 UPDATE / DELETE，报错"只允许追加，如需更正请追加一条新记录" |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_data_corrections_quarantine` (`quarantine_id, corrected_at DESC`) | 查某条隔离数据的处理历史 |
| `idx_data_corrections_issue` (`issue_id, corrected_at DESC`) | 查某个问题的处理历史 |
| `idx_data_corrections_new_batch` (`new_batch_id`) WHERE 非空 | 从重跑批次反查是为了处理什么问题 |

## 常用查询

```sql
-- 放行一条隔离数据（同一个事务里改两张表）
BEGIN;
INSERT INTO meta.data_corrections (quarantine_id, action, reason, decided_by)
VALUES (42, 'release', '核对雅虎财经，当天数据属实', 'K.Hawk');
UPDATE meta.quarantine_records
SET status = 'released', resolved_at = now()
WHERE quarantine_id = 42 AND status = 'pending';
COMMIT;

-- 某条隔离数据的完整处理历史
SELECT corrected_at, action, decided_by, reason, new_batch_id
FROM meta.data_corrections
WHERE quarantine_id = 42
ORDER BY corrected_at;
```
