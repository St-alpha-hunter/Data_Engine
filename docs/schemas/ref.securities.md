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

- **建档名单**：S&P 500 历次调入 / 调出事件（`events.sp500_index_changes`，来自 GitHub `fja05680/sp500`），只保留 **2010 年以后还在指数里**的证券。注意这份数据的代码有时是公司**后来的代码**（比如把 2006 年的谷歌记成 GOOGL），不能直接当作当时的代码使用
- **字段来源**：

| 字段 | 来源 |
|---|---|
| `name`、`security_type`、`country`、`primary_exchange`、`first_trade_date` | FMP `profile`（按代码查，只有当前状态）；没有可用 profile 时，名称取收购记录或代码变更里的公司名，首个交易日用首次进入指数的日期（近似） |
| `status`、`last_trade_date` | 还在指数里或 profile 显示仍在交易 → `active`；否则取 FMP 日线最后一天，查不到时用调出指数前一天（近似） |
| `delisting_reason` | 收购事件（被收购方用过这只证券的代码，且文件日在最后交易日之前） |
| `review_status` / `notes` | 生成过程中的核对结果，见下文 |

- **写入方**：[`pipelines/security_master.py`](../../pipelines/security_master.py)（首次生成）。2026-10-12 事件已回填 `security_id`，**从此只能增量维护**，不能再清空重建
- **下游**：`ref.security_identifiers.security_id` 外键引用本表；`events.corporate_events.security_id`、`events.acquisitions.acquirer_security_id` 已回填；以后成分股区间、行情也关联 `security_id`

### 生成流程

```bash
python -m pipelines.security_master build                        # 主表为空时生成
python -m pipelines.security_master build --rebuild --use-cache  # 清空重建（事件已回填后会拒绝）；--use-cache 用缓存的 profile
```

1. **算成分区间**：每个代码的调入 / 调出配对，得到 [调入, 调出)
2. **串成证券候选**：
   - 有代码变更 X→Y，且 X 的调出、Y 的调入都在改名日前后 3 天内 → 同一只证券
   - 人工串联：`symbol_change_fixes.csv` 里标 `link` 的
   - 同一代码的多段区间先合并
3. **拆开被重复使用的代码**：同一代码有多段区间，且 profile 上市日晚于第一段开始、不晚于后面某段开始，说明是两家公司，在那里拆开（HLT、DELL、DOW、CEG、CCI、CNC）。前面那只是老公司，不用 profile
4. **按人工对照拆分**：同一个代码下有多个 `security` 标签的，拆成多只证券，成分区间按时间段切开（如 FOXA：21 世纪福克斯 / 新福克斯）
5. **核对、补字段**：
   - profile 上市日晚于首次进入指数：如果已经调出指数，认为代码被别人拿去用了，不用 profile（APC、GENZ、SBNY）；如果还在指数里，认为 FMP 写的是重组日期，仍用 profile，但 CIK / CUSIP / ISIN 从上市日起生效（ED、HON、NEE）
   - CIK 优先级：人工对照 → profile → SEC 代码表（仍在交易的要和 SEC 交叉核对）→ 收购记录
   - 代码历史：以成分区间里的代码为基础，用 FMP 代码变更补上漏掉的改名（只补到调出指数为止）
6. **写库**：在一个事务里完成。代码冲突（排除约束）时只跳过那一条，这只证券标 `needs_review`。同时生成报告 `data/security_master_review.csv`
7. **`review_status`**：有人工对照的是 `verified`；所有检查都通过的是 `auto`；其余是 `needs_review`，原因写在 `notes` 里

### 人工配置

| 文件 | 内容 |
|---|---|
| [`config/security_overrides.csv`](../../config/security_overrides.csv) | 人工对照：某个代码（`anchor_symbol`）的代码历史以人工为准。`security` 列不同，表示同一个代码下是不同的证券；`name`、`cik` 用于没有 profile 的老公司。目前包括 GOOG/GOOGL、AABA、FOXA/FOX、NWSA/NWS、IR、HWM、LHX、GL、CPRI、UA |
| [`config/symbol_change_fixes.csv`](../../config/symbol_change_fixes.csv) | `link`：人工把两个代码串成同一只证券（HRS→LHX 等，调出 / 调入日和改名日差太多）；`ignore`：忽略 FMP 的错误代码变更（如 POM→EXC 其实是收购） |

**2026-10-12 生成结果**：共 820 只（active 604、delisted 216）；`auto` 655、`verified` 15、`needs_review` 150；代码冲突 0。`needs_review` 里主要是已退市、FMP 没有数据的证券（缺 CIK、最后交易日是近似值），另有 8 只仍在交易、上市日偏晚的（AEE、BNY、ED、FE、HON、NEE、SNA、XEL）。生成后人工改了 4 只拆出来的老公司名称（CEG、DOW、DELL、Q），原因记在 `notes` 里。

### 回填事件之后怎么维护

主表已经被事件引用，**不能删证券、不能清空重建**，只能在原来的证券上改。日常维护用 [`pipelines/security_maintenance.py`](../../pipelines/security_maintenance.py)，默认只试算、出报告（`data/security_maintenance_report.csv`），加 `--apply` 才写库；每个动作一个事务，可以重复执行。

```bash
# 每天的顺序
python -m pipelines.event_loader sp500 / symbol_change / acquisition ...   # 导入新事件
python -m pipelines.security_maintenance --apply                            # 改名、新调入、退市
python -m pipelines.event_security_backfill --apply                         # 补分红、拆股等事件的 security_id
```

| 动作 | 触发条件 | 做什么 |
|---|---|---|
| 改名 | 还没对应上的代码变更 X→Y，改名前一天用 X 的是主表里的证券，且 X 现在还没结束 | X 在改名日结束，从改名日开始用 Y（`source = fmp_symbol_change`，记 `event_id`）；回填这条事件；`notes` 里记一笔。Y 那天已被别的证券占着，或者在 `ignore` 名单里 → 不改（前者进报告） |
| 新调入 | 2010 年以后、还没对应上的调入事件，调入日主表里没人用这个代码 | 新建证券：拉 profile 和 SEC CIK，用和首次生成同一套 `build_record` 组装（检查规则、`review_status` 一样）；回填这条事件 |
| 退市 | 仍是 active、最近一次成分事件是调出的证券，最近 60 天日线的最后一天早于 10 天前 | `status = delisted`，最后交易日取日线最后一天；被收购的（收购记录里是被收购方，文件日在最后交易日前两年以内）填 `acquired`，否则退市原因留空、标 `needs_review`；所有还没结束的代码在最后交易日的下一天结束 |
| 不处理、进报告 | 改名的新代码已被别的证券占着；一条日线都拉不到 | 人工核对 |

**人工修正**：直接 UPDATE，在 `notes` 里写明原因和日期；核对完的改成 `verified`。

**还没做的**：定期用 SEC 代码表和 FMP profile 对一遍仍在交易的证券（CIK、名称、交易所的变化）；改名后公司名称不会自动更新。

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
