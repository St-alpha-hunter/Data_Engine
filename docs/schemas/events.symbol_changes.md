# events.symbol_changes

> PostgreSQL `data_engine.events` · 建表 SQL：[014_events_symbol_changes.sql](../../sql/postgres/014_events_symbol_changes.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

代码变更事件的**明细**：哪天、从哪个代码改成哪个代码（如 2022-06-09 FB → META）。

与事件总表 [`events.corporate_events`](events.corporate_events.md) **一对一**（按 `event_id`）。

用途：**推导证券主表的代码历史**（[`ref.security_identifiers`](ref.security_identifiers.md)）：在变更日关闭旧代码的一段，同一天开始新代码的一段，把改名前后串成同一个 `security_id`。

## Grain

**一行 = 一个代码变更事件的一个版本**（和总表的一行对应）。

主键：`event_id`（同时是指向总表的外键）。

## Source and lineage

- **写入方式**：一律调用 `events.record_symbol_change(...)`，同一个事务写总表 + 明细。**总表的 `symbol` 自动填旧代码**（事件发生时证券还在用旧代码）
- **数据来源**：FMP `symbol-change`（5504 条，有完整历史，如 FB → META 2022-06-09）

| 本表字段 | FMP 字段 |
|---|---|
| `change_date` | `date` |
| `old_symbol` | `oldSymbol` |
| `new_symbol` | `newSymbol` |
| `company_name` | `companyName` |

- **无效日期**：FMP 数据里有日期为 `1969-12-31` 的记录。**表约束不拦截**，导入时识别出来记为质量问题（原始数据仍在 raw / 总表 payload 里）
- **下游**：`events.v_symbol_changes_current`；证券主表的代码历史（`ref.security_identifiers.event_id` 指回引起变化的事件）

## Schemas

| 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|
| `event_id` | bigint | 否 | 主键；外键 → `events.corporate_events` |
| `change_date` | date | 否 | 变更生效日，等于总表 `event_date`；**从这天起使用新代码** |
| `old_symbol` | text | 否 | 旧代码；总表的 `symbol` 也填它 |
| `new_symbol` | text | 否 | 新代码 |
| `company_name` | text | 是 | 公司名称，人工核对用 |

### 约束与触发器

| 名称 | 规则 |
|---|---|
| `pk_symbol_changes` | `event_id` 唯一 |
| `fk_symbol_changes_event` | `event_id` 必须存在于总表 |
| `chk_symbol_changes_old_format` / `_new_format` | 大写字母、数字、`.`、`-`，与证券主表一致 |
| `chk_symbol_changes_different` | 新旧代码不能相同 |
| `trg_symbol_changes_check` | 通用触发器 `events.check_detail_event('symbol_change', 'change_date')` |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_symbol_changes_old` (`old_symbol, change_date`) | 某个旧代码后来改成了什么 |
| `idx_symbol_changes_new` (`new_symbol, change_date`) | 某个新代码以前叫什么 |

## 常用查询

```sql
-- 写入
SELECT * FROM events.record_symbol_change('2022-06-09', 'FB', 'META',
    'fmp', 'FB|META|2022-06-09', '<batch_id>', '{"oldSymbol": "FB", "newSymbol": "META"}'::jsonb, 'Meta Platforms, Inc.');

-- META 以前叫什么（可以连续追溯多次改名）
WITH RECURSIVE chain AS (
    SELECT old_symbol, new_symbol, change_date FROM events.v_symbol_changes_current WHERE new_symbol = 'META'
    UNION ALL
    SELECT c.old_symbol, c.new_symbol, c.change_date
    FROM events.v_symbol_changes_current c JOIN chain ON c.new_symbol = chain.old_symbol AND c.change_date < chain.change_date
)
SELECT * FROM chain ORDER BY change_date;
```
