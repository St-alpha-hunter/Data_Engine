# golden.adjustment_events（含视图 v_adjustment_factors）

> ClickHouse `golden` · 建表 SQL：[014](../../sql/clickhouse/014_golden_adjustment_events.sql)、视图 [016](../../sql/clickhouse/016_golden_v_adjustment_factors.sql) · 写入：[`pipelines/golden_loader.py`](../../pipelines/golden_loader.py) 的 `build_adjustments()` · 返回 [数据字典](../data-dictionary.md)

## Purpose

**复权事件和复权因子**：把 [golden.price_daily](golden.price_daily.md) 的不复权价换算成复权价要用的系数。由事件库（Postgres `events.splits` / `events.dividends`）+ 除权前一天的不复权收盘价算出，加上人工修正。

## Grain

**一行 = 一个事件的一个版本**（security_id × ex_date × kind × event_id × version）。

## 复权方式

和 CRSP 等数据商的常见做法一致：**除权日以前**的价格乘 `price_factor`，成交量乘 `volume_factor`。

| 事件 | `value` | `price_factor` | `volume_factor` |
|---|---|---|---|
| 拆股（含合股、股票股利） | 比例 = 新股数 / 旧股数（4 拆 1 = 4） | 1 / 比例 | 比例 |
| 现金分红（常规 + 特别） | 每股金额（不复权） | 1 − 金额 / 除息前一天不复权收盘价 | 1 |
| 分拆（只来自人工修正） | 每股分到的价值 | 1 − 价值 / 除权前一天不复权收盘价（和现金分红同一算法：发的是子公司股票而不是现金） | 1 |

同一天有多个事件（如拆股 + 分红）先相乘合并。累积因子 = 这一天及之后所有事件因子的乘积。

## Source and lineage

`build_adjustments()` 按顺序：

1. 读事件库里已回填 `security_id` 的拆股（不含 FMP 的 `adjustment` 类型）和现金分红；只要除权日不晚于今天的
2. **去重**：改过代码的证券（MMC → MRSH、BK → BNY 等），新旧代码各拉过一遍全部历史，同一笔分红会出现两次（实测 1,039 条）。同一证券、同一天、同一类事件只留一条，优先除权日当天这只证券正在用的代码
3. **人工修正**：`config/adjustment_overrides.csv`（见下）
4. 只算落在这只证券 golden 日线区间里的事件（区间外的不影响任何已存价格）
5. 算因子；无效的跳过并记日志：因子 ≤ 0，缺前收，或分红 / 分拆金额 ≥ 前收（目前 2 个）
6. **和当前版本比对**：新的写 version 1；事件修订（revision 变了）、因子变了、或已撤回的又出现了，写新版本；**golden 里有效、但这次没有了的，写一个 `is_active = 0` 的新版本撤回**，不删数据

## 人工修正：`config/adjustment_overrides.csv`

| 列 | 说明 |
|---|---|
| `action` | `drop` 删除事件 / `set_value` 改 value / `add` 补事件 |
| `symbol` | 证券主表里这只证券最后使用的代码 |
| `kind` | `split` / `dividend` / `spinoff` |
| `ex_date` | 除权日 |
| `value` | `set_value` / `add` 时填（含义同上表） |
| `decided_on` | 做出修正的日期（`add` 的事件用它作为得知时间） |
| `note` | 原因和依据 |

找不到对应事件的修正会记日志（可能事件库已经改对了，这条修正可以删掉）。**2026-10-14 现有 23 条**：

- 删除 FMP 的错误拆股：PNW、COR、KMI 的"10 拆 1"根本不存在（不复权价那天没跳，复权后当天收益率 +907%）；GOOGL 2014 年同一次拆股出现两条
- KMI 2015 年之前 15 笔分红改成正确金额（FMP 原始金额是实际的 10 倍，和那条错误的拆股配套）
- 补分拆：CCEP 2010、DELL 2018、CNX 2017（CNX 同时删掉 FMP 记成"1.2 拆 1"的近似写法）。每股价值由 FMP 当天复权收益率反推：`前收 − 当天收盘 / (1 + FMP 收益率)`，**只用于 FMP 那天看起来合理的情况**

## 和 FMP 复权价核对（GD001 v2）

`check_against_fmp()`：我们算的复权价的**每日收益率**，和 FMP 复权价的每日收益率比，相差 > 2% 记一条 `meta.quality_issues`（warning）。每次全量重做（先删上一轮的 GD001）。两边重叠的最后一天不比（两次拉取时间不同，当天收盘数据可能还在更新）。

为什么按收益率比、不按价格比：漏一个事件，之前所有日子的价格都差一个固定比例（v1 按价格比时 22.9 万天"偏差"），但每天的涨跌幅都是对的，只有事件那一天的收益率是错的——回测真正受影响的只有那一天。

**2026-10-14 结果**：比较 266 万天，87 条问题、40 只证券，其中收益率差 > 10% 的 15 条，基本都是：

| 类型 | 例子 | 状态 |
|---|---|---|
| 分拆 / 重组，FMP 那天也不合理，不能用来反推 | DHR 2016（FMP 显示 +61%）、DXC 2015、HWM 2016、AIV 2020 | 待查公司公告 |
| FMP 不复权价本身有问题 | ESRX 2010（2 拆 1 却跌 75%）、WY 2010、CTVA 2026 | 待查 |
| 不复权价无故上涨、复权价没动（错误报价） | AVB 2013、VTR 2015、TRIP 2011、TMUS 2013 | 待查 |
| 特别分红 / 合股细节 | GEN 2020、TPL 2020、EXPE 2011 | 待查 |

另有 GL、RRD、IGT 在分红日有一串 2%–7% 的偏差，可能是分红金额或日期的口径问题，待查。

## Schemas

| 字段 | 类型 | 说明 |
|---|---|---|
| `security_id` | UInt64 | |
| `ex_date` | Date32 | 除权 / 除息日 |
| `kind` | LowCardinality(String) | `split` / `dividend` / `spinoff` |
| `value` | Float64 | 见上表 |
| `prev_close` | Nullable(Float64) | 除权前一个交易日的不复权收盘价 |
| `price_factor` | Float64 | 除权日以前的价格要乘的系数（> 0） |
| `volume_factor` | Float64 | 除权日以前的成交量要乘的系数（> 0） |
| `is_active` | UInt8 | 1 = 有效；0 = 已撤回（这个版本起不再生效） |
| `event_id` | UInt64 | `events.corporate_events.event_id`；人工补的为 0 |
| `event_revision` | UInt32 | 事件的版本号；人工补的为 0 |
| `known_at` | DateTime64(3, 'UTC') | 事件被我们得知的时间（按时点查询用）；人工补的为 `decided_on` |
| `version` | UInt32 | 第几个版本 |
| `loaded_at` | DateTime64(3, 'UTC') | 写入时间 |

视图 `golden.v_adjustment_factors`：每只证券每个除权日一行——`day_price_factor` / `day_volume_factor`（当天合并后的因子）、`cum_price_factor` / `cum_volume_factor`（累积因子）。只用当前版本且 `is_active = 1` 的事件。

## 常用查询

```sql
-- 某只证券的复权事件（当前有效的）
SELECT * FROM golden.v_adjustment_factors WHERE security_id = 4454 ORDER BY ex_date;

-- 被撤回的事件（看修正历史）
SELECT security_id, ex_date, kind, value, version, is_active, loaded_at
FROM golden.adjustment_events WHERE (security_id, ex_date, kind, event_id) IN
    (SELECT security_id, ex_date, kind, event_id FROM golden.adjustment_events WHERE is_active = 0)
ORDER BY security_id, ex_date, version;

-- 和 FMP 核对的问题
-- （Postgres）SELECT symbol, date, detail FROM meta.quality_issues WHERE rule_id = 'GD001' ORDER BY date;
```
