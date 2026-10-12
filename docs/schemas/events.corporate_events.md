# events.corporate_events

> PostgreSQL `data_engine.events` · 建表 SQL：[011_events_corporate_events.sql](../../sql/postgres/011_events_corporate_events.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

公司事件**总表**：拆股、分红、代码变更、成分股变更、收购，所有事件都在这里有一行（每个版本一行），形成统一的事件时间线。

- **原始内容原样保存**在 `payload`，能追溯、能重放
- 各类事件特有的字段（拆股比例、分红金额……）放在各自的明细表里（待建，按 `event_id` 一对一关联）
- 证券主表、成分股区间等状态表，由这里的事件推导出来

## Grain

**一行 = 一个事件的一个版本**。

事件被修改（分红金额改了、收购取消了）时**追加新版本**（`revision + 1`），旧版本保留。按 `(source, source_event_key)` 区分是不是同一个事件。

主键：`event_id`（自增）。唯一键：
- `(source, source_event_key, revision)`：同一事件的版本号唯一
- `(source, source_event_key, payload_hash)`：**内容完全相同的不重复写入**（Kafka 会重复投递）

## Source and lineage

- **写入方式**：一律调用 `events.record_event(...)`，不要直接 `INSERT`。它在数据库里完成：

| 情况 | 结果 | 返回的 action |
|---|---|---|
| 内容和这个事件已有的某个版本完全相同（重复投递，包括旧消息晚到） | 不写入，返回已有版本 | `duplicate` |
| 事件已存在，但内容不同 | 写入新版本，`revision + 1` | `revised` |
| 全新的事件 | 写入，`revision = 1` | `inserted` |

  同一事件加了事务级咨询锁，多个消费者并发写同一事件时会排队，版本号不会冲突或跳号（有并发测试）。

- **"内容"指什么**：`payload_hash` = sha256(事件类型, 代码, 生效日, 状态, 公告时间, 原始内容)。jsonb 会规范化键的顺序，所以键顺序不同但内容相同算重复
- **已知局限**：如果事件内容改成 B 之后又改回 A，第二次的 A 会被当成重复投递（和第一个版本内容相同）。这种情况极少，需要时人工处理
- **来源**（`source`）：`fmp` / `yfinance` / `sp500_github` / `sec` ……；每个来源的拉取都记一个 `meta.ingestion` 批次（`batch_id` 必填）
- **`security_id`**：写入时可为空（证券主表可能还没有这只证券），对应上以后**回填一次**（规则见下文"回填 security_id"）
- **只追加保护**（触发器 `trg_corporate_events_protect`）：
  - 不允许删除
  - 只允许把 `security_id` 从空回填成有值；已有值不能再改
  - 其他字段一律不能改，修改请追加新版本
- **上游**：`batch_id` → `meta.ingestion`；`security_id` → `ref.securities`
- **下游**：`ref.security_identifiers.event_id` 引用本表（某段代码由哪个事件引起）；以后的 5 张明细表按 `event_id` 关联

### 导入流程（拉取 → 整理 → 写库）

| 层 | 代码 | 做什么 |
|---|---|---|
| 拉取 | `fetch_data/fetch.py` 的 `FetchData` | 按 `config/endpoints.py` 的 `iterate` 请求：`symbol`（每只股票）/ `none`（一次全市场）/ `page`（按页翻，空页停止） |
| 整理 | `fetch_data/events/`（`normalize_dividends` 等 5 个纯函数） | 原始行 → 标准化事件（键名与 `record_xxx()` 参数一致）；有问题的行进 `rejected`，按约定不记的进 `skipped` |
| 写库 | `db/event_store.py` 的 `EventStore.load()` | 逐条调用 `record_xxx()`；`rejected` 记为质量问题 `XXX001`，被数据库约束拒绝的记为 `XXX002`（单条失败不影响其他） |
| 串联 | `pipelines/event_loader.py` | 每类事件一个入口；每次导入记一个 `meta.ingestion` 批次。分红、拆股**分批写库**（默认每 100 只股票写一次，同一个批次）：能看到进度，中途中断已写入的部分保留，重跑判为 duplicate |

**代码写法**：项目内部统一用点（`BRK.B`），FMP 用横杠（`BRK-B`）。映射登记在 [`config/symbol_map.py`](../../config/symbol_map.py)（目前只有 BRK.B、BF.B）：`FetchData` 请求 FMP 时自动转成横杠，整理函数的 `to_symbol` 把读回的横杠转回点。

```bash
python -m pipelines.event_loader sp500                      # S&P 500 成分股变更（GitHub）
python -m pipelines.event_loader symbol_change              # 代码变更（FMP，全市场）
python -m pipelines.event_loader acquisition                # 收购（FMP，按页翻）
python -m pipelines.event_loader dividend --universe sp500  # 分红（FMP，2010 年以后进过 S&P 500 的代码）
python -m pipelines.event_loader split --universe sp500     # 拆股
```

事件导入的质量规则（以代码为准，定义在 `db/event_store.py` 的 `EVENT_RULES`，写库前自动同步）：

| 规则 | 含义 |
|---|---|
| `EVD001` / `EVS001` / `EVC001` / `EVA001` / `EVI001` | 分红 / 拆股 / 代码变更 / 收购 / 成分股变更的原始数据被整理函数拒绝（`detail.reason` 是原因，`detail.row` 是原始行） |
| `EVD002` / `EVS002` / `EVC002` / `EVA002` / `EVI002` | 事件写库时被数据库约束拒绝（`detail.error` 是错误信息） |

### 回填 security_id

代码：[`pipelines/event_security_backfill.py`](../../pipelines/event_security_backfill.py)。**不可逆**：回填后不能改，证券主表也不能再清空重建。默认只试算、出报告（`data/event_security_backfill_report.csv`），加 `--apply` 才写库；只填空的，可以重复执行（补新导入的事件）。

```bash
python -m pipelines.event_security_backfill            # 试算
python -m pipelines.event_security_backfill --apply    # 写库
```

**原则：拿不准的不填**。空的以后还能补，填错了改不回来。
**日期用明细表里的业务日期**（除息日、拆股生效日、调入 / 调出日、改名日、收购文件日）。总表的 `event_date` 不一定是业务日期，比如 FMP 分红的 `event_date` 是导入日。

| 事件 | 对应规则 |
|---|---|
| 分红 / 拆股（FMP 按代码拉的全部历史） | ① 业务日期那天谁用这个代码，就归谁（GOOG 2014 年以前的记录归 A 类股）<br>② 那天没人用：如果这个代码是某只证券**最后使用的代码**，且日期在它的存续期内，就归它。这是因为 FMP 按现在的代码记历史，例如 META 改名前的分红也记在 META 下<br>③ 代码现在被主表以外的公司使用（profile 仍在交易，但主表里现在没人用这个代码，比如 STI 现在是 Solidion）：只填那家公司上市以前的，且只用第 ① 条 |
| S&P 500 成分变更 | 调入日（调出则用前一天）谁用这个代码，就归谁；查不到时（社区数据集把代码回溯成了后来的代码，比如 2006 年的 GOOGL），用生成主表时的成分区间对应；**两种方法结果不一致的不填**（IR、UA） |
| 代码变更 | 改名前一天谁用旧代码；查不到再看改名当天谁用新代码；`config/symbol_change_fixes.csv` 里标了 `ignore` 的不填 |
| 收购 | 被收购方：文件日谁用这个代码；查不到时用 CIK，但只在**只有一只证券**用这个 CIK 时才填。收购方（`events.acquisitions.acquirer_security_id`）规则相同 |

**2026-10-12 首次回填结果**：回填了 64,981 条事件和 495 条收购方。

| 类型 | 总数 | 已回填 | 没填的主要原因 |
|---|---|---|---|
| 分红 | 63,563 | 61,229 | 用旧代码拉回来的重复历史（如 PEAK，同样的记录用 DOC 拉时已经对上了）；别家公司的数据；首个交易日之前的记录 |
| 拆股 | 2,376 | 2,093 | 同上 |
| 成分变更 | 2,021 | 1,270 | 2010 年以前就调出、不在股票池里的（747 条）；IR / UA 有 4 条两种方法结果不一致 |
| 代码变更 | 5,498 | 195 | 全市场数据，绝大多数不在股票池里 |
| 收购 | 2,056 | 194（收购方 495） | 绝大多数不在股票池里 |

**已知待处理**：IR 1996–2020 的成分区间实际属于 TT（老英格索兰）；Under Armour C 类股 2016-04-08 的调入事件。这两处目前没填，等做成分股区间表时人工补。

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `event_id` | bigint | 否 | 自增 | 主键（一个版本一个 id） |
| `event_type` | text | 否 | — | `dividend` / `split` / `symbol_change` / `index_change` / `acquisition` |
| `symbol` | text | 否 | — | 数据源给出的原始代码 |
| `security_id` | bigint | 是 | — | 对应到 `ref.securities` 后回填 |
| `event_date` | date | 否 | — | 生效日：除息日 / 拆股生效日 / 代码变更日 / 成分股变更生效日 / 收购完成日 |
| `announced_at` | timestamptz | 是 | — | 公告时间；数据源不提供则为空 |
| `known_at` | timestamptz | 否 | `now()` | **我们得知这个版本的时间**，按时点查询靠它 |
| `status` | text | 否 | — | `announced` 已公告 / `confirmed` 已确认或已生效 / `cancelled` 已取消 |
| `revision` | integer | 否 | — | 版本号，从 1 开始 |
| `source` | text | 否 | — | 数据来源 |
| `source_event_key` | text | 否 | — | 数据源里的唯一标识；没有则按规则拼，如 `AAPL\|2020-08-31\|split` |
| `payload_hash` | text | 否 | — | 内容哈希（64 位十六进制），`record_event` 计算 |
| `batch_id` | text | 否 | — | 来自哪次拉取 |
| `payload` | jsonb | 否 | — | 原始内容 |

### 约束

| 名称 | 规则 |
|---|---|
| `pk_corporate_events` | `event_id` 唯一 |
| `fk_corporate_events_security` | `security_id` 必须存在于 `ref.securities` |
| `fk_corporate_events_batch` | `batch_id` 必须存在于 `meta.ingestion` |
| `chk_corporate_events_type` | 只能是 5 类事件 |
| `chk_corporate_events_status` | 只能是 3 种状态 |
| `chk_corporate_events_revision_positive` | 版本号 ≥ 1 |
| `chk_corporate_events_symbol_format` | 大写字母、数字、`.`、`-` |
| `chk_corporate_events_key_not_blank` | `source_event_key` 不能为空白 |
| `chk_corporate_events_hash_format` | 64 位十六进制 |
| `uq_corporate_events_revision` | `(source, source_event_key, revision)` 唯一 |
| `uq_corporate_events_content` | `(source, source_event_key, payload_hash)` 唯一 |

### 索引

| 索引 | 用途 |
|---|---|
| `idx_corporate_events_symbol_date` | 按代码、日期查 |
| `idx_corporate_events_type_date` | 按事件类型、日期查 |
| `idx_corporate_events_security` | 按证券查 |
| `idx_corporate_events_unresolved` WHERE `security_id IS NULL` | 待匹配到证券的事件 |
| `idx_corporate_events_known_at` | 按时点查询 |

## 查询接口

| 接口 | 用途 |
|---|---|
| `events.record_event(...)` | 写入事件，返回 `(event_id, revision, action)` |
| `events.v_corporate_events_current` | 每个事件的当前版本 |
| `events.corporate_events_asof(时间)` | 站在某个时间点，每个事件的最新版本（`known_at <=` 该时间） |

```sql
-- 写入（Kafka 消费者、拉取脚本都这样写）
SELECT * FROM events.record_event(
    'symbol_change', 'FB', '2022-06-09', 'confirmed',
    'fmp', 'FB|META|2022-06-09', '<batch_id>',
    '{"oldSymbol": "FB", "newSymbol": "META"}'::jsonb);

-- 某只证券的所有事件（当前版本）
SELECT event_type, event_date, status, payload
FROM events.v_corporate_events_current WHERE security_id = 3 ORDER BY event_date;

-- 回测：站在 2022-05-15，我们知道哪些事件
SELECT * FROM events.corporate_events_asof('2022-05-15') WHERE event_type = 'split';

-- 还没对应到证券的事件
SELECT event_id, symbol, event_type, event_date FROM events.corporate_events WHERE security_id IS NULL;

-- 回填 security_id：不要手写 UPDATE，用 pipelines/event_security_backfill.py（按事件类型用不同日期和规则）

-- 某只证券的全部分红（按 security_id，不受改名、代码重复使用影响）
SELECT ex_date, amount, symbol FROM events.v_dividends_current WHERE security_id = 3 ORDER BY ex_date;
```
