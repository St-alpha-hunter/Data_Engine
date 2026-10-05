# events.sp500_index_changes

> PostgreSQL `data_engine.events` · 建表 SQL：[016_events_sp500_index_changes.sql](../../sql/postgres/016_events_sp500_index_changes.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

**S&P 500** 成分股变更的明细：哪只股票哪天调入、哪天调出。

与事件总表 [`events.corporate_events`](events.corporate_events.md) **一对一**（按 `event_id`）。

**按指数分表**：表名带 `sp500`，以后纳斯达克 100 用单独的 `events.nasdaq100_index_changes`（两个股票池的数据源、字段可能不同）。总表里两者的 `event_type` 都是 `index_change`，靠 `source_event_key` 的前缀（`SP500|` / `NDX|`）区分；本表保留 `index_code` 列并固定为 `SP500`，方便以后和纳斯达克的表合并成一个视图。

用途：
- 推导成分股区间 `ref.sp500_membership`（待建）：每对 add / remove 组成一段区间，回测时用当天的成分股，解决幸存者偏差
- 确定股票池：2010 年以后进入过 S&P 500 的约 848 个代码

## Grain

**一行 = 一次调入或调出事件的一个版本**。社区数据集里每个代码只有"进入日期"和"退出日期"，所以**每段区间拆成两个事件**：`add` 一个、`remove` 一个（仍在成分里的只有 `add`）。

主键：`event_id`。`source_event_key` 默认 = `SP500|代码|动作|日期`，如 `SP500|FB|add|2013-12-23`。

## Source and lineage

- **数据来源**：GitHub `fja05680/sp500` 的 `sp500_ticker_start_end.csv`（`ticker, start_date, end_date`，1996 年起）；同仓库的 `S&P 500 Historical Components & Changes (Updated).csv`（每天的成分名单）可用来核对。FMP 的成分股接口在当前套餐下全部返回 402
- **⚠️ 代码不一定是当时的代码**：该数据集有时把公司**后来的代码**套用到更早的时期（如 2006 年的谷歌记成 `GOOGL`，当时用的是 `GOOG`；1999–2017 年的雅虎记成 `AABA`）。所以：
  - 写入时 `security_id` **先留空**，不直接按代码匹配
  - 证券主表建好后，结合代码变更事件和公司名称核对，再回填
- **写入方式**：一律调用 `events.record_sp500_index_change(...)`，同一个事务写总表 + 明细
- **`counterpart_symbol`、`reason`**：社区数据集没有，先留空；以后可以从维基百科的成分变动表补（那里有"谁替换了谁、为什么"）
- **下游**：`events.v_sp500_index_changes_current`；`ref.sp500_membership`（待建）

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `event_id` | bigint | 否 | — | 主键；外键 → `events.corporate_events` |
| `effective_date` | date | 否 | — | 生效日，等于总表 `event_date` |
| `index_code` | text | 否 | `SP500` | 固定为 `SP500` |
| `action` | text | 否 | — | `add` 调入 / `remove` 调出 |
| `company_name` | text | 是 | — | 公司名称，人工核对用 |
| `counterpart_symbol` | text | 是 | — | 同一天对应调出 / 调入的那只股票 |
| `reason` | text | 是 | — | 调整原因 |

总表的 `symbol` 填被调入或调出的那只股票的代码。

### 约束与触发器

| 名称 | 规则 |
|---|---|
| `pk_sp500_index_changes` | `event_id` 唯一 |
| `fk_sp500_index_changes_event` | `event_id` 必须存在于总表 |
| `chk_sp500_index_changes_index_code` | 只能是 `SP500` |
| `chk_sp500_index_changes_action` | `add` / `remove` |
| `chk_sp500_index_changes_counterpart_format` | 大写字母、数字、`.`、`-` |
| `trg_sp500_index_changes_check` | 通用触发器 `events.check_detail_event('index_change', 'effective_date')`：只追加，事件类型、生效日与总表一致 |

## 常用查询

```sql
-- 写入
SELECT * FROM events.record_sp500_index_change('META', '2022-06-09', 'add',
    'sp500_github', '<batch_id>', '{"ticker": "META", "start_date": "2022-06-09"}'::jsonb);

-- 某一天的调入 / 调出
SELECT symbol, action, company_name FROM events.v_sp500_index_changes_current
WHERE effective_date = '2022-06-09' ORDER BY action, symbol;

-- 每年调入、调出的数量
SELECT date_part('year', effective_date) AS year,
       count(*) FILTER (WHERE action = 'add') AS adds,
       count(*) FILTER (WHERE action = 'remove') AS removes
FROM events.v_sp500_index_changes_current GROUP BY 1 ORDER BY 1;
```
