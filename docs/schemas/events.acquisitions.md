# events.acquisitions

> PostgreSQL `data_engine.events` · 建表 SQL：[015_events_acquisitions.sql](../../sql/postgres/015_events_acquisitions.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

收购事件的**明细**：谁收购了谁、文件日期、完成日期。

**"中立"的表结构**：一笔交易只存一次（一个事实只存一份），读的时候用两个视图从不同视角看：

| 视图 | 视角 | `security_id` 是 | `counterparty_*` 是 |
|---|---|---|---|
| `events.v_acquisitions_as_target` | 被收购方："我被谁收购" | 被收购方 | 收购方 |
| `events.v_acquisitions_as_acquirer` | 收购方："我收购了谁" | 收购方 | 被收购方 |
| `events.v_acquisitions_current` | 中立 | 两边都有（`target_security_id` / `acquirer_security_id`） | — |

例：A 收购 B、C、D、E → 总表 4 个事件；收购方视角查 A 得到 4 行；被收购方视角查 B 得到 1 行。

与事件总表 [`events.corporate_events`](events.corporate_events.md) **一对一**（按 `event_id`），事件记在**被收购方**名下（总表 `symbol` / `security_id` = 被收购方，退市的是它）。

## Grain

**一行 = 一笔收购交易的一个版本**。

一笔交易通常有两个版本：
1. **公告阶段**：`status = announced`，`event_date = filing_date`，`completion_date` 为空
2. **完成后**：追加新版本，`status = confirmed`，`event_date = completion_date`

主键：`event_id`。同一笔交易的各版本用 `deal_key`（也是总表的 `source_event_key`）串起来。

## Source and lineage

- **数据来源**：FMP `mergers-acquisitions-latest`（可翻页，约追溯到 2015 年前后）
- **FMP 字段 `transactionDate` 的含义**：FMP 抓到一份"和这笔交易有关的 SEC 文件"的日期（500 条里 499 条与 SEC 接收日 `acceptedDate` 同一天）。**它不是公告日，也不是完成日**；同一笔交易提交多份文件时会出现多次
- **导入前先合并**（FMP 的原始数据有大量重复）：

| FMP 的重复 | 例子 | 处理 |
|---|---|---|
| 收购方有多个代码，每个代码一行 | HTB / HTBI；MITT 和三只优先股 MITT-PA/PB/PC；SPAC 的单位 CCXIU、认股权证 CCXIW（500 条里 90 笔交易） | 按 `deal_key` 合并成一笔，所有代码保存在 payload 里；收购方代码选普通股 |
| 同一笔交易提交了多份文件 | Acelyrin ← Alumis：2025-03-27、2025-04-21 | 合并成一笔，`filing_date` 取**最早**的 `transactionDate` |

- **只记被收购方是上市公司（有代码）的交易**：被收购方是私有公司的（约 8%）跳过，原始数据保留在 raw
- **完成日**：用被收购方在日线里的最后一个交易日推断，不需要读 SEC 文件
- **写入方式**：一律调用 `events.record_acquisition(...)`，同一个事务写总表 + 明细；状态不传时按是否有完成日推导（有 → `confirmed`，无 → `announced`）
- **`acquirer_security_id`**：收购方是上市公司、对应到证券主表后**回填一次**（通用触发器的"可回填列"）；收购方不是上市公司时为空
- **以后的增强（计划中）**：交易对价（现金 / 换股 / 每股价格）、正式公告日、交易终止等信息只在 SEC 文件原文里，计划用 AI 从文件中提取，提取结果标记为 `source = 'llm'` 并需人工核对

## Schemas

| 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|
| `event_id` | bigint | 否 | 主键；外键 → `events.corporate_events` |
| `deal_key` | text | 否 | 交易编号 = `被收购方 CIK \| 收购方 CIK` |
| `effective_date` | date | 否 | = 总表 `event_date` = `coalesce(completion_date, filing_date)` |
| `filing_date` | date | 否 | 这笔交易最早的 SEC 文件日期 |
| `completion_date` | date | 是 | 完成日（被收购方最后交易日）；未知为空 |
| `target_symbol` | text | 否 | 被收购方代码 |
| `target_name` | text | 是 | 被收购方名称 |
| `target_cik` | text | 是 | 被收购方 CIK（10 位） |
| `acquirer_symbol` | text | 是 | 收购方代码（普通股） |
| `acquirer_name` | text | 是 | 收购方名称 |
| `acquirer_cik` | text | 是 | 收购方 CIK（10 位） |
| `acquirer_security_id` | bigint | 是 | 收购方在证券主表里的编号；只能从空回填一次 |
| `filing_url` | text | 是 | 最早那份 SEC 文件的链接，人工核对用 |
| `accepted_at` | timestamptz | 是 | 最早那份文件的 SEC 接收时间 |

### 约束与触发器

| 名称 | 规则 |
|---|---|
| `pk_acquisitions` | `event_id` 唯一 |
| `fk_acquisitions_event` | `event_id` 必须存在于总表 |
| `fk_acquisitions_acquirer_security` | `acquirer_security_id` 必须存在于 `ref.securities` |
| `chk_acquisitions_effective_date` | `effective_date = coalesce(completion_date, filing_date)` |
| `chk_acquisitions_deal_key_not_blank` | `deal_key` 不能为空白 |
| `chk_acquisitions_target_symbol_format` / `_acquirer_symbol_format` | 大写字母、数字、`.`、`-` |
| `chk_acquisitions_target_cik_format` / `_acquirer_cik_format` | 10 位数字 |
| `trg_acquisitions_check` | 通用触发器 `events.check_detail_event('acquisition', 'effective_date', 'acquirer_security_id')`：事件类型、生效日与总表一致；只追加，只允许 `acquirer_security_id` 从空回填一次 |

## 常用查询

```sql
-- 写入（公告阶段）
SELECT * FROM events.record_acquisition(
    p_target_symbol => 'ATVI', p_filing_date => '2022-01-18', p_deal_key => '0000718877|0000789019',
    p_source => 'fmp', p_batch_id => '<batch_id>', p_payload => '{...}'::jsonb,
    p_target_name => 'Activision Blizzard', p_target_cik => '0000718877',
    p_acquirer_symbol => 'MSFT', p_acquirer_name => 'Microsoft', p_acquirer_cik => '0000789019');

-- 完成后：同一个 deal_key 再写一次，带上完成日 → 新版本
SELECT * FROM events.record_acquisition(
    p_target_symbol => 'ATVI', p_filing_date => '2022-01-18', p_deal_key => '0000718877|0000789019',
    p_source => 'fmp', p_batch_id => '<batch_id>', p_payload => '{...}'::jsonb,
    p_completion_date => '2023-10-13', ...);

-- 收购方视角：微软收购过哪些公司
SELECT counterparty_symbol, counterparty_name, status, effective_date
FROM events.v_acquisitions_as_acquirer WHERE symbol = 'MSFT' ORDER BY effective_date;

-- 被收购方视角：ATVI 被谁收购、什么时候完成
SELECT counterparty_symbol, status, filing_date, completion_date
FROM events.v_acquisitions_as_target WHERE symbol = 'ATVI';
```
