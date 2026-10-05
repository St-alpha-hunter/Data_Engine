# events.dividends

> PostgreSQL `data_engine.events` · 建表 SQL：[012_events_dividends.sql](../../sql/postgres/012_events_dividends.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

分红事件的**明细**：除息日、登记日、派息日、公告日、每股分红、派息频率、类型。

与事件总表 [`events.corporate_events`](events.corporate_events.md) **一对一**（按 `event_id`）。事件的版本、来源、原始内容、`security_id` 都在总表，本表只存分红特有的字段。

用途：
- 计算总收益 / 自算复权价（核对 FMP 复权价）
- 股息相关因子（股息率、派息稳定性、特别股息）

## Grain

**一行 = 一个分红事件的一个版本**（和总表的一行对应）。

分红被修改（金额、日期变了）时，总表追加新版本，本表也随之新增一行明细；旧明细保留。

主键：`event_id`（同时是指向总表的外键）。

## Source and lineage

- **写入方式**：一律调用 `events.record_dividend(...)`。它在**同一个事务**里：
  1. 调 `events.record_event()` 写总表（去重、分配版本号）
  2. 结果是 `inserted` / `revised` 时写本表明细；`duplicate` 时不写
  
  任何一步失败（如金额 ≤ 0），总表也一起回滚，不会出现"有事件没明细"。
- **金额请用 `Decimal` 传入**（Python 的 `float` 会变成 `double precision`，与函数的 `numeric` 参数不匹配，且有精度误差）
- **数据来源（FMP）**：

| 用途 | 接口 | 说明 |
|---|---|---|
| 补历史 | `dividends?symbol=XXX` | 每只股票的全部分红历史（AAPL 从 1987 年起）。部分退市股票有（TWX、ATVI），部分没有（CELG、LEH） |
| 每天增量 | `dividends-calendar?from=&to=` | **单次最多返回 4000 条**，每次只查 1 天；只能查最近约一年和未来，更早返回 402 |

- **字段映射**：

| 本表字段 | FMP 字段 | 说明 |
|---|---|---|
| `ex_date` | `date` | 除息日；也是总表的 `event_date` |
| `record_date` | `recordDate` | 少量缺失 |
| `payment_date` | `paymentDate` | 约 9% 缺失 |
| `declaration_date` | `declarationDate` | **约 40% 缺失** |
| `amount` | `dividend` | |
| `adj_amount` | `adjDividend` | 按拆股调整 |
| `frequency` | `frequency` | |
| `dividend_type` | 由 `frequency` 推导 | `Special` / `Irregular` → `special`，其余 → `regular` |
| `currency` | 无 | FMP 不提供，默认 `USD` |
| — | `yield` | **不存**：按某天股价算出的派生值，不是事件本身的属性（原始值仍在总表 `payload` 里） |

- **股票股利**不记在本表，记在拆股表（FMP `splitType = stock-dividend`）
- **数据不全怎么办**：计划在不复权价接入后，用"复权因子跳动 ↔ 分红事件"互相核对，找出缺失的分红（作为 C 步骤的一组新规则，计划中）
- **下游**：`events.v_dividends_current`；以后的自算复权、总收益计算

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `event_id` | bigint | 否 | — | 主键；外键 → `events.corporate_events` |
| `ex_date` | date | 否 | — | 除息日，等于总表 `event_date` |
| `record_date` | date | 是 | — | 登记日 |
| `payment_date` | date | 是 | — | 派息日 |
| `declaration_date` | date | 是 | — | 公告日 |
| `amount` | numeric | 否 | — | 每股分红（原始金额），> 0 |
| `adj_amount` | numeric | 是 | — | 按拆股调整后的每股分红，> 0 |
| `frequency` | text | 是 | — | `Weekly` / `Bi-Weekly` / `Monthly` / `Quarterly` / `Semi-Annual` / `Annual` / `Irregular` / `Special` |
| `dividend_type` | text | 否 | — | `regular` 定期 / `special` 特别股息 |
| `currency` | text | 否 | `USD` | 三位大写币种代码 |

### 约束与触发器

| 名称 | 规则 |
|---|---|
| `pk_dividends` | `event_id` 唯一 |
| `fk_dividends_event` | `event_id` 必须存在于总表 |
| `chk_dividends_amount_positive` | `amount > 0` |
| `chk_dividends_adj_amount_positive` | `adj_amount` 为空或 > 0 |
| `chk_dividends_frequency` | 8 种取值之一 |
| `chk_dividends_type` | `regular` / `special` |
| `chk_dividends_currency` | 三位大写字母 |
| `trg_dividends_check` | 通用触发器 `events.check_detail_event('dividend', 'ex_date')`（[013](../../sql/postgres/013_events_splits.sql) 起替换了原来的专用函数）：① 对应的事件必须是 `dividend` 类型；② `ex_date` 必须等于总表的 `event_date`；③ 只追加，不能修改、删除 |

日期之间的先后关系（公告日 ≤ 除息日 ≤ 派息日）**不做硬性约束**：数据源本身有缺失和异常，硬性约束会让整批数据写不进来。这类检查留给 C 步骤的质量规则。

## 常用查询

```sql
-- 写入
SELECT * FROM events.record_dividend(
    p_symbol => 'AAPL', p_ex_date => '2026-08-10', p_amount => 0.27,
    p_source => 'fmp', p_source_event_key => 'AAPL|2026-08-10|dividend', p_batch_id => '<batch_id>',
    p_payload => '{"symbol": "AAPL", "date": "2026-08-10", "dividend": 0.27}'::jsonb,
    p_record_date => '2026-08-10', p_payment_date => '2026-08-13', p_declaration_date => '2026-07-30',
    p_frequency => 'Quarterly', p_adj_amount => 0.27);

-- 某只证券的分红历史（当前版本）
SELECT ex_date, amount, frequency, dividend_type FROM events.v_dividends_current
WHERE security_id = 3 ORDER BY ex_date;

-- 特别股息
SELECT symbol, ex_date, amount FROM events.v_dividends_current WHERE dividend_type = 'special';
```
