# events.splits

> PostgreSQL `data_engine.events` · 建表 SQL：[013_events_splits.sql](../../sql/postgres/013_events_splits.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

拆股事件的**明细**，包括拆股、合股（反向拆股）、股票股利，以及 FMP 记录的**价格调整**（合并、分拆、特别分配）：生效日、比例、类型。

与事件总表 [`events.corporate_events`](events.corporate_events.md) **一对一**（按 `event_id`）。

用途：
- 不复权价 → 自算复权价（核对 FMP 复权价）
- 识别价格断层：不复权价在拆股生效日的跳动，应等于拆股比例

## Grain

**一行 = 一个拆股事件的一个版本**（和总表的一行对应）。

主键：`event_id`（同时是指向总表的外键）。

## Source and lineage

- **写入方式**：一律调用 `events.record_split(...)`，在同一个事务里写总表（去重、分配版本号）+ 本表明细；任何一步失败，总表一起回滚。分子、分母请用 `Decimal` 传入
- **`split_type` 的推导**：不传时按比例推导，`分子 < 分母 → reverse`，否则 `forward`；股票股利需要显式传 `stock_dividend`
- **数据来源（FMP）**：

| 用途 | 接口 | 说明 |
|---|---|---|
| 补历史 | `splits?symbol=XXX` | 每只股票的全部拆股历史（AAPL 5 次，NVDA 6 次含 2024 年 10:1，TSLA 2 次）。部分退市股票有（TWX、ATVI），部分没有（CELG、LEH） |
| 每天增量 | `splits-calendar?from=&to=` | 只能查最近约 3 个月和未来，更早返回 402 |

- **字段映射**：

| 本表字段 | FMP 字段 | 说明 |
|---|---|---|
| `effective_date` | `date` | 生效日；也是总表的 `event_date` |
| `numerator` | `numerator` | 按原值保存，可以不是整数 |
| `denominator` | `denominator` | 同上 |
| `split_type` | `splitType` | `stock-dividend` 且比例 > 1 → `stock_dividend`；`stock-dividend` 且比例 < 1 → `adjustment`；`stock-split` 或空（约 13% 为空）→ 按比例推导 `forward` / `reverse` |

- **`adjustment`（价格调整）**：FMP 把合并、分拆、特别分配对股价的影响，记成"比例小于 1 的 stock-dividend"。它们不是真正的股数变化，但复权时需要，所以单独用这个类型保存，比例按原值保存。首次导入共 13 条：

| 股票 | 日期 | 比例 | 实际事件 |
|---|---|---|---|
| BK / BNY | 2007-07-02 | 4717:5000 | 纽约银行与梅隆金融合并 |
| VTR | 2015-08-18 | 8757:10000 | 分拆 CCP |
| JCI | 2016-09-06 | 191:200 | 江森自控与泰科合并 |
| AIV | 2020-12-01 | 807:1000 | 分拆 AIR |
| SLG | 2021、2022 | 971:1000、97:100 | 特别股息（部分以股票支付） |
| 其他 | | | SJM 2002、FERG 2012/2013/2018、AVB 2013、AIV 2019 |

  这类比例在复权时具体怎么使用（方向、是否与 FMP 复权价一致），等不复权价接入、做复权核对时再验证

- **注意 GOOGL 2014-04-03 记为 `999:500`**（不是整齐的 2:1）：那次是谷歌发行 C 类股（GOOG），比例里带了调整系数。所以比例按原值保存，不假定为整数
- **数据不全怎么办**：计划在不复权价接入后，用"复权因子跳动 / 不复权价断层 ↔ 拆股事件"互相核对（作为 C 步骤的一组新规则，计划中）
- **下游**：`events.v_splits_current`；以后的自算复权

## Schemas

| 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|
| `event_id` | bigint | 否 | 主键；外键 → `events.corporate_events` |
| `effective_date` | date | 否 | 生效日，等于总表 `event_date` |
| `numerator` | numeric | 否 | 拆股后股数（分子） |
| `denominator` | numeric | 否 | 拆股前股数（分母） |
| `split_ratio` | numeric | 否 | **生成列** = `numerator / denominator`，自动计算、不能手工修改。4 拆 1 为 4，1 合 20 为 0.05 |
| `split_type` | text | 否 | `forward` 拆股 / `reverse` 合股 / `stock_dividend` 股票股利 / `adjustment` 价格调整（[017](../../sql/postgres/017_splits_adjustment_type.sql) 新增） |

### 约束与触发器

| 名称 | 规则 |
|---|---|
| `pk_splits` | `event_id` 唯一 |
| `fk_splits_event` | `event_id` 必须存在于总表 |
| `chk_splits_positive` | 分子、分母都 > 0 |
| `chk_splits_not_one_to_one` | 分子 ≠ 分母（1:1 不算拆股） |
| `chk_splits_type` | 只能是 4 种类型 |
| `chk_splits_type_matches_ratio` | 按类型检查：`forward` / `stock_dividend` 分子 > 分母（股数增加）；`reverse` 分子 < 分母；`adjustment` 不限方向 |
| `trg_splits_check` | 通用触发器 `events.check_detail_event('split', 'effective_date')`：事件类型必须是 `split`；生效日必须等于总表 `event_date`；只追加（没有可回填列，完全不能修改） |

## 常用查询

```sql
-- 写入（不传 split_type，按比例推导）
SELECT * FROM events.record_split('AAPL', '2020-08-31', 4, 1,
    'fmp', 'AAPL|2020-08-31|split', '<batch_id>', '{"numerator": 4, "denominator": 1}'::jsonb);

-- 某只证券的拆股历史
SELECT effective_date, numerator, denominator, split_ratio, split_type
FROM events.v_splits_current WHERE security_id = 3 ORDER BY effective_date;

-- 某日之后的累计拆股倍数（用于把历史股数换算到今天）
SELECT exp(sum(ln(split_ratio))) AS cumulative_ratio
FROM events.v_splits_current WHERE security_id = 3 AND effective_date > '2015-01-01';
```
