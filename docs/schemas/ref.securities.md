# ref.securities

> PostgreSQL `data_engine.ref` · 建表 SQL：[010_ref_securities.sql](../../sql/postgres/010_ref_securities.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

证券主表：**一只证券一行**，给每只证券分配一个**永久不变、永不复用**的内部编号 `security_id`。事件、行情、财报最终都通过 `security_id` 关联到证券，而不是通过股票代码。

为什么不用外部代码当主键：

| 外部代码 | 问题 | 例子 |
|---|---|---|
| symbol | 会改名，也会被别的公司重新使用 | FB → META；WB 以前是美联银行，现在是微博；FB 现在又被一只 ETF 使用 |
| CIK | 代表公司不代表股票，且会随公司重组变化 | GOOG 和 GOOGL 共用一个 CIK；2015 年谷歌重组为 Alphabet，GOOGL 的 CIK 从 1288776 变成 1652044 |
| CUSIP / ISIN | 公司重组、改名时也会变 | 同上，GOOGL 的 CUSIP 从 38259P… 变成 02079K… |

外部代码连同各自的有效期记在 [`ref.security_identifiers`](ref.security_identifiers.md)。

## Grain

**一行 = 一只证券**（按证券一级，不按公司：GOOG 和 GOOGL 是两行）。

主键：`security_id`（自增，`GENERATED ALWAYS`，不能手工指定）。

## Source and lineage

- **建档名单**：S&P 500 历史成分（GitHub `fja05680/sp500` 的 `sp500_ticker_start_end.csv`，2010 年以后约 848 个代码）。注意它的代码有时是公司**后来的代码**（如把 2006 年的谷歌记成 GOOGL），不能直接当作当时的代码使用
- **字段来源**：

| 字段 | 来源 |
|---|---|
| `name`、`security_type`、`country`、`primary_exchange`、`first_trade_date` | FMP `profile`（仍在交易的）、SEC EDGAR（已退市的） |
| `last_trade_date`、`status` | FMP 日线最后一天 / 退市事件 |
| `delisting_reason` | 收购事件等 |
| `review_status` | 匹配过程：对不上、不确定的标 `needs_review`，人工核对后改 `verified` |

- **写入方**：证券主表的生成脚本（待写），之后由事件流（代码变更、退市、收购）维护
- **下游**：`ref.security_identifiers.security_id` 外键引用本表；以后事件总表、成分股区间、行情都关联 `security_id`

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `security_id` | bigint | 否 | 自增 | 内部永久编号 |
| `name` | text | 否 | — | 当前名称；已退市的为最后使用的名称 |
| `security_type` | text | 否 | — | `common_stock` / `adr` / `etf` / `preferred` / `other` |
| `share_class` | text | 是 | — | A / B / C |
| `country` | text | 是 | — | 注册国家，ISO 两位代码（如 US、CN） |
| `primary_exchange` | text | 是 | — | 当前主要上市交易所，MIC（XNYS / XNAS） |
| `first_trade_date` | date | 是 | — | 第一个交易日 |
| `last_trade_date` | date | 是 | — | 最后一个交易日；仍在交易为空 |
| `status` | text | 否 | `active` | `active` / `delisted` |
| `delisting_reason` | text | 是 | — | `acquired` / `bankruptcy` / `other`；只有已退市的才能填 |
| `review_status` | text | 否 | `needs_review` | `verified` 人工确认 / `auto` 自动匹配且可信 / `needs_review` 待人工核对 |
| `notes` | text | 是 | — | 人工核对备注 |
| `created_at` | timestamptz | 否 | `now()` | |
| `updated_at` | timestamptz | 否 | `now()` | 更新时由触发器 `trg_securities_updated_at` 自动刷新 |

`name`、`primary_exchange` 只存当前值；它们的历史变化（改名、转板）以后放到 `security_attributes`。

### 约束与索引

| 名称 | 规则 |
|---|---|
| `pk_securities` | `security_id` 唯一 |
| `chk_securities_name_not_blank` | 名称不能为空白 |
| `chk_securities_type` | `security_type` 只能是 5 个值之一 |
| `chk_securities_status` | `active` / `delisted` |
| `chk_securities_delisted_iff_last_date` | 已退市必须有 `last_trade_date`；交易中不能有 |
| `chk_securities_trade_dates` | 第一个交易日不晚于最后一个交易日 |
| `chk_securities_delisting_reason` | 退市原因只能填在已退市的证券上，且只能是 3 个值之一 |
| `chk_securities_review_status` | 只能是 3 个值之一 |
| `chk_securities_exchange_mic` | 交易所必须是 MIC 格式 |
| `chk_securities_country` | 国家必须是两位大写字母 |
| `idx_securities_review` WHERE `review_status = 'needs_review'` | 人工核对清单 |

## 常用查询

```sql
-- 当前视图（大宽表）：每只证券一行，代码为当前值
SELECT * FROM ref.v_securities_current WHERE status = 'active';

-- 人工核对清单
SELECT security_id, name, notes FROM ref.securities WHERE review_status = 'needs_review';

-- 人工确认后标记
UPDATE ref.securities SET review_status = 'verified', notes = '核对 SEC 文件，确认为美联银行' WHERE security_id = 1;
```
