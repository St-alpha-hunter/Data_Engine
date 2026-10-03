# meta.quality_issues

> PostgreSQL `data_engine.meta` · 建表 SQL：[002_meta_quality.sql](../../sql/postgres/002_meta_quality.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

记录清洗校验时发现的每一个数据质量问题：哪一批数据、哪只股票的哪一天、触发了哪条规则的哪个版本、严重程度、具体数值。

`warning` 和 `danger` 都记在这里：
- `warning` 的数据照常进 golden，本表留下记录供以后查
- `danger` 的数据同时进入 `meta.quarantine_records` 被隔离

典型用途：
- 查某只股票某一天有过哪些问题
- 统计某条规则最近触发了多少次，判断阈值是否合理
- 处理问题时，作为 `meta.data_corrections` 的关联对象

## Grain

**一行 = 一条记录（ticker × date）在一个批次里触发的一条规则**。

同一条记录触发两条规则，就是两行。股票层面的问题（不对应具体某一天）`date` 为空。

主键：`issue_id`（自增）。唯一键：`(batch_id, rule_id, ticker, date)`，同一批数据重复校验不会重复记录。

## Source and lineage

- **写入方**（规划中，代码尚未接入）：`fetch_data/basic_clean.py` 的 `CleanBasic._add_error()`，以及各子类的 `_check_*` 方法。目前 `_add_error` 只把错误行存在内存里的 `self.errors`，之后会改为同时写入本表
- **上游**：
  - `batch_id` → `meta.ingestion`
  - `(rule_id, rule_version)` → `meta.quality_rules`
- **下游**：
  - `meta.data_corrections.issue_id` 引用本表（refetch / manual_fix）
  - 与 `meta.quarantine_records` 通过 `(batch_id, ticker, date)` 关联（无外键）

`severity` 是从 `quality_rules` 冗余过来的，方便直接筛选 `danger`，不用每次 JOIN 规则表。

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `issue_id` | bigint | 否 | 自增 | 主键 |
| `batch_id` | text | 否 | — | 哪一次拉取发现的 |
| `rule_id` | text | 否 | — | 规则编号，如 `PV001` |
| `rule_version` | integer | 否 | — | 规则版本 |
| `severity` | text | 否 | — | `warning` / `danger` |
| `ticker` | text | 否 | — | 股票代码 |
| `date` | date | 是 | — | 出问题的交易日；股票层面的问题为空 |
| `detail` | jsonb | 否 | `{}` | 出问题的具体数值，如 `{"adjLow": 0, "adjHigh": 175.3}` |
| `detected_at` | timestamptz | 否 | `now()` | 发现时间（UTC） |

### 约束

| 约束名 | 规则 |
|---|---|
| `pk_quality_issues` | `issue_id` 唯一 |
| `fk_quality_issues_batch` | `batch_id` 必须存在于 `meta.ingestion` |
| `fk_quality_issues_rule` | `(rule_id, rule_version)` 必须存在于 `meta.quality_rules` |
| `chk_quality_issues_severity` | `severity` 只能是 `warning` / `danger` |

### 索引

| 索引 | 用途 |
|---|---|
| `uq_quality_issues_dedup` (`batch_id, rule_id, ticker, date`) NULLS NOT DISTINCT | 去重；`date` 为空也视为相同 |
| `idx_quality_issues_ticker_date` (`ticker, date`) | 按股票、日期查 |
| `idx_quality_issues_rule` (`rule_id, detected_at DESC`) | 按规则看最近的触发情况 |

## 常用查询

```sql
-- 某只股票某天的所有问题
SELECT i.rule_id, i.rule_version, i.severity, r.description, i.detail, i.batch_id
FROM meta.quality_issues i
JOIN meta.quality_rules r USING (rule_id, rule_version)
WHERE i.ticker = 'AAPL' AND i.date = '2022-03-01';

-- 各规则最近 30 天触发次数
SELECT rule_id, severity, count(*) AS n
FROM meta.quality_issues
WHERE detected_at > now() - interval '30 days'
GROUP BY rule_id, severity
ORDER BY n DESC;
```
