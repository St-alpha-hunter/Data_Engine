# 数据字典

本文档是 Data_Engine 所有数据表的**全局目录**和**统一字段术语**。每张表的详细说明（用途、粒度、来源、字段）见 [`schemas/`](schemas/) 下的表级文档。

> 维护约定：新建或修改表时，同步更新本目录和对应的表级文档；建表 SQL 按编号放在 `sql/postgres/`（ClickHouse 的放在 `sql/clickhouse/`）。
> 质量规则以**代码**为准（清洗类的 `rules`），运行时同步到 `meta.quality_rules`，不要直接改规则表。

---

## 1. 存储总览

| 存储 | 连接 | 库 / schema | 放什么 | 写入方式 |
|---|---|---|---|---|
| PostgreSQL 16（Docker） | `localhost:5433`，库 `data_engine` | `meta` | 数据治理工具表：批次、规则、问题、隔离、处理记录 | 行数少，允许按状态更新（`data_corrections` 除外） |
| PostgreSQL 16（Docker） | 同上 | `ref` | 状态表：交易日历、证券主表（以后还有成分股区间） | 有变化才更新；ClickHouse 用 `postgresql()` 直接跨库读 |
| PostgreSQL 16（Docker） | 同上 | `events` | 公司事件流：事件总表（以后还有 5 张明细表） | 只追加；修改 = 追加新版本；写入用 `events.record_event()` |
| ClickHouse 24.8（Docker） | `localhost:8123` | `raw` | FMP 原始数据，原样落库 | 只追加 |
| ClickHouse 24.8（Docker） | `localhost:8123` | `golden` | 清洗校验后的数据，可追溯版本 | 只追加，用视图取当前版本 |
| 中间态存储 | 本地 `data/` 下每个接口自己的目录（`config/paths.py` 的 `ENDPOINT_DATA_DIRS`，如 price_volume → `data/volume_data_1y/`） | — | 清洗后的中间态 parquet，D 步骤读它写 golden | 每个批次一个文件 `{batch_id}.parquet`，重跑覆盖。以后换对象存储只需在 `.env` 设 `INTERMEDIATE_URL=s3://...` |

> 注意：本机还有一个原生 PostgreSQL 15（Windows 服务，端口 **5432**），与本项目无关。连接本项目请用 **5433**。

数据流：

```
FMP API ──拉取──▶ 结构检查 ──▶ raw（ClickHouse）──清洗校验──▶ 中间态 parquet ──▶ golden（ClickHouse）
   │                                    │
   └─▶ meta.ingestion                   ├─▶ meta.quality_issues      （warning / danger 都记）
                                        └─▶ meta.quarantine_records  （danger 被阻断，不进 golden）
                                                   │
                                     人工/规则处理 ─▶ meta.data_corrections
                                                   │
                                     release 后由 outbox 任务写入 golden（quality_status = warning）

每次写 raw / golden / quarantine ─▶ meta.load_log（写了哪张表、多少行、是否成功）
```

raw 写入前只做**结构检查**（必需字段存在、类型可转换、`symbol` / `date` 非空），不做质量检查。
结构检查不通过（通常是 FMP 改了接口）时整批不写 raw，`ingestion.status = 'failed'`，原始返回内容存到 `data/raw_rejects/{batch_id}.json` 排查。

流程分四步，对应的代码：

| 步骤 | 做什么 | 代码 | 状态 |
|---|---|---|---|
| A 拉取 | 调 FMP | `fetch_data/fetch.py` 的 `FetchData` | ✅ |
| B 进 raw | 结构检查 → 写 raw | `pipelines/raw_loader.py` 的 `ingest()` | ✅ |
| C 清洗校验 | 从 raw 按 batch_id 读 → 按规则校验 → 写 issues + quarantine（同一事务）→ 写中间态 parquet → 错误报告 → 对账 | `pipelines/clean_runner.py` 的 `run_clean(batch_id)`；清洗类按接口名自动注册（`clean/registry.py`） | ✅ |
| D 进 golden | 中间态 parquet → 代码对应证券编号 → 按存续期截断 → 和当前版本比对、有变化才写新版本；再由事件算复权因子、和 FMP 复权价核对 | `pipelines/golden_loader.py`（目前只有日线：不复权价 + 复权因子） | ✅ 日线 |

`meta.ingestion` 只包 A + B；C、D 是否成功看 `meta.load_log`。

---

## 2. 表目录

### PostgreSQL · `data_engine.meta`

| 表 | 用途 | 粒度（一行代表） | 建表 SQL | 表级文档 |
|---|---|---|---|---|
| `meta.ingestion` | 记录每次 FMP 拉取 | 一次拉取批次 | [001](../sql/postgres/001_meta_ingestion.sql) | [ingestion](schemas/meta.ingestion.md) |
| `meta.quality_rules` | 校验规则清单（带版本） | 一条规则的一个版本 | [002](../sql/postgres/002_meta_quality.sql) | [quality_rules](schemas/meta.quality_rules.md) |
| `meta.quality_issues` | 校验发现的问题 | 一条记录（symbol × date）触发的一条规则 | [002](../sql/postgres/002_meta_quality.sql) | [quality_issues](schemas/meta.quality_issues.md) |
| `meta.quarantine_records` | 被阻断、不允许直接进 golden 的数据 | 一条被隔离的记录（batch × endpoint × symbol × date） | [003](../sql/postgres/003_meta_quarantine.sql) | [quarantine_records](schemas/meta.quarantine_records.md) |
| `meta.data_corrections` | 对问题 / 隔离数据的处理记录 | 一次处理动作（只追加） | [004](../sql/postgres/004_meta_data_corrections.sql) | [data_corrections](schemas/meta.data_corrections.md) |
| `meta.load_log` | 每个批次写入了哪些表、多少行 | 一次写表 | [006](../sql/postgres/006_meta_load_log.sql) | [load_log](schemas/meta.load_log.md) |

`meta` schema 本身见 [000](../sql/postgres/000_schema_meta.sql)；约束统一改名见 [005](../sql/postgres/005_rename_constraints.sql)；ticker → symbol 改名见 [007](../sql/postgres/007_rename_ticker_to_symbol.sql)。issues 去重键与 load_log 中间态见 [008](../sql/postgres/008_issues_version_and_load_intermediate.sql)。交易日历见 [009](../sql/postgres/009_ref_trading_calendar.sql)；证券主表见 [010](../sql/postgres/010_ref_securities.sql)；事件总表见 [011](../sql/postgres/011_events_corporate_events.sql)；分红明细见 [012](../sql/postgres/012_events_dividends.sql)；拆股明细与通用明细触发器见 [013](../sql/postgres/013_events_splits.sql)；代码变更明细见 [014](../sql/postgres/014_events_symbol_changes.sql)；收购明细见 [015](../sql/postgres/015_events_acquisitions.sql)；S&P 500 成分股变更见 [016](../sql/postgres/016_events_sp500_index_changes.sql)；拆股表增加 adjustment 类型见 [017](../sql/postgres/017_splits_adjustment_type.sql)；CIK 允许多只证券共用见 [018](../sql/postgres/018_identifiers_cik_shared.sql)。新环境按 000 → 018 顺序执行即可从零重建。

### PostgreSQL · `data_engine.ref`

| 表 | 用途 | 粒度（一行代表） | 建表 SQL | 表级文档 |
|---|---|---|---|---|
| `ref.trading_calendar` | 交易所日历：是否交易、提前收盘、开收盘时间、前后交易日 | 一个交易所的一个自然日 | [009](../sql/postgres/009_ref_trading_calendar.sql) | [trading_calendar](schemas/ref.trading_calendar.md) |
| `ref.securities` | 证券主表：永久编号 `security_id` + 当前属性 | 一只证券（按证券一级，GOOG / GOOGL 各一行） | [010](../sql/postgres/010_ref_securities.sql) | [securities](schemas/ref.securities.md) |
| `ref.security_identifiers` | 代码历史：symbol / CIK / CUSIP / ISIN 的有效期 | 一只证券一种代码的一段有效期 | [010](../sql/postgres/010_ref_securities.sql) | [security_identifiers](schemas/ref.security_identifiers.md) |

查询接口：`ref.resolve_security(代码类型, 代码, 日期)` → `security_id`；`ref.securities_asof(日期)`；视图 `ref.v_securities_current`（大宽表）。
证券主表由 `pipelines/security_master.py` 生成（人工配置：`config/security_overrides.csv`、`config/symbol_change_fixes.csv`），事件的 `security_id` 由 `pipelines/event_security_backfill.py` 回填。2026-10-12 已回填，此后主表只能增量维护，不能清空重建。

### PostgreSQL · `data_engine.events`

| 表 | 用途 | 粒度（一行代表） | 建表 SQL | 表级文档 |
|---|---|---|---|---|
| `events.corporate_events` | 公司事件总表：拆股、分红、代码变更、成分股变更、收购 | 一个事件的一个版本 | [011](../sql/postgres/011_events_corporate_events.sql) | [corporate_events](schemas/events.corporate_events.md) |
| `events.dividends` | 分红明细：除息日、登记日、派息日、公告日、金额、频率、类型 | 一个分红事件的一个版本（与总表一对一） | [012](../sql/postgres/012_events_dividends.sql) | [dividends](schemas/events.dividends.md) |
| `events.splits` | 拆股明细（含合股、股票股利、价格调整）：生效日、分子、分母、比例、类型 | 一个拆股事件的一个版本（与总表一对一） | [013](../sql/postgres/013_events_splits.sql) | [splits](schemas/events.splits.md) |
| `events.symbol_changes` | 代码变更明细：生效日、旧代码、新代码 | 一个代码变更事件的一个版本（与总表一对一） | [014](../sql/postgres/014_events_symbol_changes.sql) | [symbol_changes](schemas/events.symbol_changes.md) |
| `events.sp500_index_changes` | S&P 500 成分股变更明细：调入 / 调出、生效日（以后纳斯达克 100 单独建 `events.nasdaq100_index_changes`） | 一次调入或调出事件的一个版本（与总表一对一） | [016](../sql/postgres/016_events_sp500_index_changes.sql) | [sp500_index_changes](schemas/events.sp500_index_changes.md) |
| `events.acquisitions` | 收购明细（中立结构）：双方、文件日、完成日；两个视图分别从被收购方 / 收购方视角读 | 一笔收购交易的一个版本（与总表一对一，记在被收购方名下） | [015](../sql/postgres/015_events_acquisitions.sql) | [acquisitions](schemas/events.acquisitions.md) |

查询接口：`events.record_event(...)` 写入（自动去重、分配版本号）；`events.v_corporate_events_current`；`events.corporate_events_asof(时间)`。
明细表各有自己的写入函数（如 `events.record_dividend(...)`），在同一个事务里写总表 + 明细；明细表同样只追加，并由通用触发器 `events.check_detail_event(事件类型, 生效日列名 [, 可回填列名])` 检查事件类型和生效日与总表一致；可选允许某一列从空回填一次（如收购表的 `acquirer_security_id`）。

### ClickHouse · `raw` / `golden`

| 表 | 用途 | 粒度（一行代表） | 建表 SQL | 表级文档 |
|---|---|---|---|---|
| `raw.price_volume_daily` | FMP 日线原始数据，只追加、不去重 | 一个批次里一只股票一天（batch × symbol × date） | [ch 001](../sql/clickhouse/001_raw_price_volume_daily.sql) | [raw.price_volume_daily](schemas/raw.price_volume_daily.md) |
| `raw.income_statement` | 利润表；年报 + 季报（`period` = FY / Q1–Q4），2010 年起；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票一个报告期（batch × symbol × date × period） | [ch 002](../sql/clickhouse/002_raw_income_statement.sql) | — |
| `raw.balance_sheet` | 资产负债表；年报 + 季报；FMP 原样 + 追溯字段，只追加 | 同上 | [ch 003](../sql/clickhouse/003_raw_balance_sheet.sql) | — |
| `raw.cash_flow` | 现金流量表；年报 + 季报；FMP 原样 + 追溯字段，只追加 | 同上 | [ch 004](../sql/clickhouse/004_raw_cash_flow.sql) | — |
| `raw.financial_ratio` | 财务比率；只有年报（季报需要高级套餐）；FMP 原样 + 追溯字段，只追加 | 同上 | [ch 005](../sql/clickhouse/005_raw_financial_ratio.sql) | — |
| `raw.enterprise_values` | 企业价值（年度）；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票一个财年末（batch × symbol × date） | [ch 006](../sql/clickhouse/006_raw_enterprise_values.sql) | — |
| `raw.market_cap` | 历史市值（每天）；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票一天（batch × symbol × date） | [ch 007](../sql/clickhouse/007_raw_market_cap.sql) | — |
| `raw.financial_estimate` | 分析师一致预期（年度，最近 10 个财年）；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票一个被预测财年 | [ch 008](../sql/clickhouse/008_raw_financial_estimate.sql) | — |
| `raw.analyst_grades` | 分析师评级人数（每月）；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票一个月 | [ch 009](../sql/clickhouse/009_raw_analyst_grades.sql) | — |
| `raw.profile` | 公司概况快照（没有日期，看 `ingested_at`）；FMP 原样 + 追溯字段，只追加 | 一个批次里一只股票 | [ch 010](../sql/clickhouse/010_raw_profile.sql) | — |
| `raw.executive_compensation` | 高管薪酬；FMP 原样 + 追溯字段，只追加 | 一个批次里一份委托书的一位高管（batch × symbol × filingDate × nameAndPosition） | [ch 011](../sql/clickhouse/011_raw_executive_compensation.sql) | — |
| `raw.price_volume_unadjusted_daily` | FMP **不复权**日线（字段名也叫 adjOpen / adjClose，实际是当时真实成交价），2010 年起；golden 日线的来源 | 一个批次里一只股票一天（batch × symbol × date） | [ch 012](../sql/clickhouse/012_raw_price_volume_unadjusted_daily.sql) | — |
| `golden.price_daily` | **日线主数据：不复权价**，按证券编号存，带版本；有变化才写新版本 | 一只证券一天的一个版本（security_id × date × version） | [ch 013](../sql/clickhouse/013_golden_price_daily.sql) | [golden.price_daily](schemas/golden.price_daily.md) |
| `golden.adjustment_events` | 复权事件和复权因子（拆股 / 现金分红 / 分拆），带版本和撤回标记 `is_active` | 一个事件的一个版本 | [ch 014](../sql/clickhouse/014_golden_adjustment_events.sql) | [golden.adjustment_events](schemas/golden.adjustment_events.md) |
| `golden.v_price_daily` | 视图：每只证券每天的当前版本（不复权） | 一只证券一天 | [ch 015](../sql/clickhouse/015_golden_v_price_daily.sql) | [golden.price_daily](schemas/golden.price_daily.md) |
| `golden.v_adjustment_factors` | 视图：每个除权日的当前因子 + 累积因子（只用有效事件） | 一只证券一个除权日 | [ch 016](../sql/clickhouse/016_golden_v_adjustment_factors.sql) | [golden.adjustment_events](schemas/golden.adjustment_events.md) |
| `golden.v_price_daily_adjusted` | 视图：**回测用复权价** = 不复权价 × 累积因子（向最新一天对齐，和 FMP 复权价同一口径） | 一只证券一天 | [ch 017](../sql/clickhouse/017_golden_v_price_daily_adjusted.sql) | [golden.price_daily](schemas/golden.price_daily.md) |

### 人工配置文件（`config/`）

人工核对的结论都放在这几个文件里，和代码一起提交 git，每一行写明原因：

| 文件 | 用途 | 谁读它 |
|---|---|---|
| `security_overrides.csv` | 证券的代码历史以人工为准（GOOG/GOOGL、FOXA 拆成两只等） | `pipelines/security_master.py` |
| `symbol_change_fixes.csv` | `link` 人工串联两个代码；`ignore` 忽略 FMP 的错误代码变更 | `pipelines/security_master.py` |
| `golden_skip.csv` | 写 golden 时跳过的证券（存续期待核对：主表最后交易日是近似值，或代码被别的公司重复使用） | `pipelines/golden_loader.py` |
| `adjustment_overrides.csv` | 复权事件的人工修正：`drop` 删除 / `set_value` 改值 / `add` 补事件（含分拆） | `pipelines/golden_loader.py` |

### 常用命令

日常由 Airflow 自动调度（每日 / 每周 / 每月三个流程），部署和用法见 [airflow.md](airflow.md)。下面是不经过 Airflow 手动执行的命令：

```bash
python -m pipelines.daily --date 2026-10-13                  # 每日流程的某一天（事件 → 主表 → 日线 → golden）
python -m pipelines.backfill --to-date 2026-10-13 --clean     # 全量补数据（断点续跑；--dry-run 看计划）
python -m pipelines.clean_runner --reclean --all             # 规则升版本后，所有批次重新清洗
python -m pipelines.golden_loader all                        # 不复权日线 → golden，算复权因子，和 FMP 核对
python -m pipelines.golden_loader adjustments                # 只重算复权因子（改了 adjustment_overrides.csv 之后）
python -m pipelines.security_maintenance --apply             # 证券主表增量维护
python -m pipelines.event_security_backfill --apply          # 新事件回填 security_id
```

### 表之间的关系

```
meta.ingestion (batch_id)
   ├──< meta.quality_issues.batch_id ──> meta.quality_rules (rule_id, rule_version)
   ├──< meta.quarantine_records.batch_id
   ├──< meta.load_log.batch_id                      （每次写表）
   └──< meta.data_corrections.new_batch_id          （refetch 时的新批次）

meta.data_corrections ──> meta.quarantine_records.quarantine_id   （release / discard）
                      ──> meta.quality_issues.issue_id            （refetch / manual_fix）
                      ──> meta.quality_rules (rule_id, rule_version)（规则自动处理时）

meta.quarantine_records ⟷ meta.quality_issues    通过 (batch_id, symbol, date) 关联，无外键
```

---

## 3. 统一字段术语

同名字段在所有表里含义一致。新建表时优先复用下面的名字和类型。

### 3.1 标识与追溯

| 字段 | 类型 | 含义 | 规则 |
|---|---|---|---|
| `batch_id` | text | 一次 FMP 拉取的批次号 | 格式 `{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}`，如 `price_volume_20261003_123000_a1b2`；主表是 `meta.ingestion` |
| `endpoint` | text | FMP 接口名 | 取值必须是 `config/endpoints.py` 里 `FMP_ENDPOINTS` 的 key，如 `price_volume`、`market_cap`；清洗类的 `endpoint_name` 也必须用这个 key |
| `exchange` | text | 交易所 | MIC 代码：`XNYS`（纽交所）、`XNAS`（纳斯达克）。FMP 用的是 `NYSE` / `NASDAQ`，入库时统一转成 MIC |
| `source` | text | 数据源 | 目前只有 `fmp`（raw 表） |
| `extra` | text（JSON） | FMP 多返回的、表里没定义的字段 | raw 表专用；没有多余字段时为 `'{}'` |
| `issue_id` | bigint | 一条质量问题的编号 | 主表是 `meta.quality_issues`，自增 |
| `quarantine_id` | bigint | 一条隔离记录的编号 | 主表是 `meta.quarantine_records`，自增 |
| `correction_id` | bigint | 一次处理动作的编号 | 主表是 `meta.data_corrections`，自增 |
| `load_id` | bigint | 一次写表的编号 | 主表是 `meta.load_log`，自增 |
| `target_table` | text | 写入的目标表 | 必须带库名前缀，如 `raw.price_volume_daily`；raw 和 golden 表名相同，不带前缀分不清。中间态写逻辑名，如 `intermediate.price_volume_daily` |
| `location` | text | 文件的实际地址 | `load_log` 中 intermediate 层专用：本地路径或 `s3://...` |
| `rule_id` + `rule_version` | text + integer | 校验规则及其版本 | 两者总是成对出现；`rule_id` 格式为「接口缩写 + 3 位序号」，如 `PV001`（PV = price_volume）。定义在清洗类的 `rules` 里；改了判定逻辑、阈值或严重程度都要升版本号 |
| `git_version` | text | 运行时代码的 git commit | 用于追溯是哪版代码产生的数据 |

### 3.2 业务键

| 字段 | 类型 | 含义 | 规则 |
|---|---|---|---|
| `security_id` | bigint | 证券的内部永久编号 | 主表是 `ref.securities`；自己分配、永不变、不复用。**跨表关联证券一律用它，不用 symbol** |
| `symbol` | text | 股票代码 | 全项目统一用 `symbol`（与 FMP 返回字段一致），meta / raw / golden 表都一样。不要再用 `ticker`；股票列表、计数类字段也用 symbol，如 `n_symbols`、`failed_symbols`。**类别股统一用点**（`BRK.B`），FMP 的横杠写法（`BRK-B`）在 `config/symbol_map.py` 登记映射 |
| `valid_from` / `valid_to` | date | 有效期 | 统一约定 **`[valid_from, valid_to)`：含开始日、不含结束日**；`valid_to` 为空表示仍有效 |
| `date` | date | 交易日（数据所属日期） | 美东交易日，不带时区；不是入库时间 |

### 3.3 状态与等级

| 字段 | 取值 | 含义 |
|---|---|---|
| `severity`（问题的严重程度） | `warning` | 可疑但可能是真实行情（如财报日大涨跌），**数据照常进 golden 并打标** |
|  | `danger` | 明确错误（如价格 ≤ 0、最高价 < 最低价），**数据进隔离区，不进 golden** |
| `quality_status`（golden 数据的质量标记） | `health` | 未触发任何规则 |
|  | `warning` | 触发了 warning 规则，或是被人工放行的 danger 数据 |
|  | `danger` | 不会出现在 golden 里（danger 数据都在隔离区） |
| `ingestion.status` | `running` / `success` / `partial` / `failed` | 进行中 / 全部成功 / 部分失败 / 全部失败或中途报错。返回空数据（missing）不算失败 |
| `quarantine_records.status` | `pending` / `released` / `discarded` | 待处理 / 已放行 / 已丢弃 |
| `load_log.layer` | `raw` / `intermediate` / `quarantine` / `golden` | 写入的是哪一层；`intermediate` 是中间态 parquet，必须带 `location` |
| `load_log.status` | `success` / `failed` | 这次写表是否成功；`failed` 必须带 `error` |
| `corporate_events.event_type` | `dividend` / `split` / `symbol_change` / `index_change` / `acquisition` | 分红 / 拆股 / 代码变更 / 成分股变更 / 收购。成分股变更按指数分明细表（`sp500_index_changes` / 以后的 `nasdaq100_index_changes`），`source_event_key` 以指数代码开头 |
| `corporate_events.status` | `announced` / `confirmed` / `cancelled` | 已公告 / 已确认或已生效 / 已取消。内容的修改用 `revision` 表示，不单设"已修订"状态 |
| `data_corrections.action` | `release` / `discard` / `refetch` / `manual_fix` | 放行隔离数据 / 丢弃隔离数据 / 重新拉取 / 人工修正数值 |

**默认读取口径**：golden 默认读 `health` + `warning`。只要"最干净"的数据时，再手动加 `WHERE quality_status = 'health'`。不要默认只读 `health`，否则会把财报日、危机日等真实极端行情排除掉，导致回测偏差。

### 3.4 版本字段

| 字段 | 在哪一层 | 含义 | 什么时候变 |
|---|---|---|---|
| `batch_id` | raw + golden | 这一版数据来自哪次拉取 | 每次拉取 / 补跑都不同 |
| `source_logic_version` | raw + golden | 数据源的计算口径版本 | FMP 改了计算逻辑（如复权方法）时，把 `FMP_ENDPOINTS` 里对应接口的值加 1；拉取时就能确定，所以 raw 里也有 |
| `data_version` | 只在 golden | 我们这边的数据版次 | 通常为 1；表结构或清洗逻辑大规模重构时升到 2。raw 不经过我们的处理，没有这个字段 |
| `quality_status` | 只在 golden | 质量标记，见 3.3 | 清洗校验时才确定；写 raw 时还不知道，所以 raw 里没有 |

当前版本**不存字段**（没有 `is_current` 列），而是由视图计算：对每个 `(symbol, date)`，取 `data_version` 最大、再取 `ingested_at` 最新的一行。补跑数据一律**追加新版本**，不删除旧版本。

### 3.5 时间字段

所有时间字段都按 **UTC** 存储（Postgres 用 `timestamptz`，ClickHouse 用 `DateTime64(3, 'UTC')`）。

| 字段 | 含义 |
|---|---|
| `started_at` / `finished_at` | 拉取开始 / 结束时间 |
| `detected_at` | 发现问题的时间 |
| `quarantined_at` | 进入隔离区的时间 |
| `resolved_at` | 隔离记录被处理（放行或丢弃）的时间 |
| `golden_written_at` | 被放行的数据写入 golden 的时间（outbox 标记） |
| `corrected_at` | 处理动作发生的时间 |
| `loaded_at` | 写表的时间 |
| `created_at` | 记录创建时间（规则表） |
| `ingested_at` | 数据写入 ClickHouse 的时间（raw / golden 表） |
| `announced_at` | 事件的公告时间（数据源提供时才有） |
| `known_at` | 我们得知某个事件版本的时间；按时点查询事件用它 |

### 3.6 JSON 字段

| 字段 | 所在表 | 内容 |
|---|---|---|
| `params` | `ingestion` | 请求参数，如 `{"from_date": "2022-01-01", "to_date": "2025-01-01"}` |
| `params` | `quality_rules` | 规则阈值，如 `{"max_range": 0.15}` |
| `failure_detail` | `ingestion` | `fetch_fmp_batch()` 返回的 `failed_list` 原样存放 |
| `detail` | `quality_issues` | 出问题的具体数值，如 `{"adjLow": 0}` |
| `record` | `quarantine_records` | 被阻断的整行原始数据 |
| `corrected_values` | `data_corrections` | 人工修正前后的值：`{"before": {...}, "after": {...}}` |

### 3.7 约束命名

| 前缀 | 类型 | 例子 |
|---|---|---|
| `pk_` | 主键 | `pk_ingestion` |
| `uq_` | 唯一约束 / 唯一索引 | `uq_quarantine_records_record` |
| `fk_` | 外键 | `fk_quality_issues_batch` |
| `chk_` | 检查约束 | `chk_quarantine_discarded_not_written` |
| `idx_` | 普通索引 | `idx_ingestion_endpoint_started` |
| `trg_` | 触发器 | `trg_data_corrections_append_only` |
