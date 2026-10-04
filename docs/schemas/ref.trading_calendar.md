# ref.trading_calendar

> PostgreSQL `data_engine.ref` · 建表 SQL：[009_ref_trading_calendar.sql](../../sql/postgres/009_ref_trading_calendar.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

交易所日历：**每个自然日一行**，标明是否交易、全天还是提前收盘、开收盘时间、前后一个交易日。

用途：
- **完整性检查**：某只股票在某段时间应该有哪些交易日的数据，缺了哪天
- **财报生效日**：收盘后公布的财报，从 `next_trading_day` 开始生效
- **N 个交易日后**：`trading_day_seq + N`
- **回测**：持仓天数、调仓日、事件日对齐

## Grain

**一行 = 一个交易所的一个自然日**（含周末和节假日）。

主键：`(exchange, date)`。目前覆盖 XNYS、XNAS 两个交易所，1990-01-01 到"今年 + 2 年"的年底（首次落库为 1990-01-01 ~ 2028-12-31，每个交易所 14245 天，其中交易日 9820 天）。

## Source and lineage

- **主数据源**：Python 库 [`exchange_calendars`](https://github.com/gerrymanoim/exchange_calendars)（版本记在 `source_version`）。它内置了纽交所 / 纳斯达克的完整规则，包括提前收盘、临时休市（如 2012 年飓风桑迪、2018 年老布什国葬、2025 年卡特国葬），不需要调 API
- **生成与落库**：[`pipelines/trading_calendar.py`](../../pipelines/trading_calendar.py)
  - `build_calendar(exchange, start, end)` 生成
  - `upsert_calendar()` 写入：新日期插入，**有变化才更新**（`updated_at` 随之更新），没变化的行不动
- **交叉核对**：同一次运行里拉 FMP `holidays-by-exchange`（记一个 `meta.ingestion` 批次，endpoint = `holidays_by_exchange`），逐日比对，差异作为 **warning** 记入 `meta.quality_issues`，**不自动覆盖日历**，由人判断：

| 规则 | 含义 |
|---|---|
| CAL001 | FMP 标为休市，但日历为交易日 |
| CAL002 | 日历为休市（非周末），但 FMP 没有标为休市 |
| CAL003 | 提前收盘不一致（只核对 2012-01-01 之后，FMP 更早的年份没有提前收盘数据） |

  这些问题记录的 `symbol` 字段填的是交易所代码（XNYS / XNAS）。

- **维护**：定期运行（建议每周）：
  ```bash
  python -m pipelines.trading_calendar            # 生成 + 落库 + FMP 核对
  python -m pipelines.trading_calendar --no-fmp   # 只生成 + 落库
  ```
  日历会自动延长到"今年 + 2 年"。临时休市（如国葬）宣布后，`exchange_calendars` 要发新版本才有，FMP 可能先有，会以 CAL001 的形式提示出来；确认后升级库（`pip install -U exchange_calendars`）再重跑。

- **ClickHouse 读取**：数据只存 Postgres 一份，ClickHouse 直接跨库读：
  ```sql
  SELECT * FROM postgresql('postgres:5432', 'data_engine', 'trading_calendar', 'data_engine', '<密码>', 'ref')
  WHERE exchange = 'XNYS' AND is_trading_day
  ```

### FMP 接口的已知问题（首次核对 2026-10-04，10 条差异全部是 FMP 的问题）

| 日期 | 问题 |
|---|---|
| 2027-12-31 | FMP 标为休市（名字写成 Christmas）。实际元旦是周六时纽交所前一个周五**照常交易** |
| 2015-11-27 | FMP 写的提前收盘时间是 13:30，股票市场实际是 13:00 |
| 2012-07-03 | FMP 的 NYSE 漏了这天提前收盘（NASDAQ 有） |
| 2027-11-26、2028-07-03、2028-11-24 | 未来的提前收盘 FMP 还没录入 |
| 2012 年以前 | FMP 没有任何提前收盘数据 |
| 接口参数 | `from` 不含当天（`from=2015-01-01` 会漏掉元旦），调用时要往前放一天 |

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `exchange` | text | 否 | — | 交易所 MIC 代码：XNYS=纽交所，XNAS=纳斯达克 |
| `date` | date | 否 | — | 自然日 |
| `is_trading_day` | boolean | 否 | — | 是否交易 |
| `session_type` | text | 否 | — | `full` 全天 / `early_close` 提前收盘 / `closed` 休市（含周末） |
| `open_time` | time | 是 | — | 开盘时间（美东当地时间）；非交易日为空 |
| `close_time` | time | 是 | — | 收盘时间；提前收盘日为 13:00 |
| `holiday_name` | text | 是 | — | 节假日名称；临时休市为 `Special closure`；提前收盘为 `Early close`；普通周末和交易日为空 |
| `trading_day_seq` | integer | 是 | — | 交易日序号，从 1 开始（1990-01-02 = 1）；非交易日为空 |
| `prev_trading_day` | date | 是 | — | 严格早于当天的最近一个交易日 |
| `next_trading_day` | date | 是 | — | 严格晚于当天的最近一个交易日 |
| `source` | text | 否 | — | `exchange_calendars` |
| `source_version` | text | 是 | — | 库的版本，如 `4.13.2` |
| `updated_at` | timestamptz | 否 | `now()` | 最后一次插入或更新的时间（UTC） |

### 约束与索引

| 名称 | 规则 |
|---|---|
| `pk_trading_calendar` | `(exchange, date)` 唯一 |
| `chk_trading_calendar_exchange_mic` | `exchange` 必须是 MIC 格式（X + 3 个大写字母） |
| `chk_trading_calendar_session_type` | 只能是 full / early_close / closed |
| `chk_trading_calendar_trading_iff_session` | `is_trading_day` 与 `session_type` 一致 |
| `chk_trading_calendar_times` | 交易日必须有开收盘时间且开盘早于收盘；非交易日不能有 |
| `chk_trading_calendar_seq_iff_trading` | 交易日才有序号 |
| `chk_trading_calendar_prev_before` / `_next_after` | 前一个交易日早于当天，后一个晚于当天 |
| `uq_trading_calendar_seq` (`exchange, trading_day_seq`) | 同一交易所交易日序号唯一 |
| `idx_trading_calendar_sessions` (`exchange, date`) WHERE `is_trading_day` | 查某段时间的交易日 |

## 常用查询

```sql
-- 某段时间的交易日
SELECT date FROM ref.trading_calendar
WHERE exchange = 'XNYS' AND is_trading_day AND date BETWEEN '2024-01-01' AND '2024-12-31';

-- 某天之后第 5 个交易日
SELECT t2.date
FROM ref.trading_calendar t1
JOIN ref.trading_calendar t2
  ON t2.exchange = t1.exchange AND t2.trading_day_seq = t1.trading_day_seq + 5
WHERE t1.exchange = 'XNYS' AND t1.date = '2024-03-01';

-- 收盘后公布的财报从哪天生效
SELECT next_trading_day FROM ref.trading_calendar WHERE exchange = 'XNYS' AND date = '2024-10-31';

-- 今年的提前收盘日
SELECT date, close_time FROM ref.trading_calendar
WHERE exchange = 'XNYS' AND session_type = 'early_close' AND date_part('year', date) = 2025;
```
