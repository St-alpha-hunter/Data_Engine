# meta.quality_rules

> PostgreSQL `data_engine.meta` · 建表 SQL：[002_meta_quality.sql](../../sql/postgres/002_meta_quality.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

数据质量校验规则的**登记簿**：每条规则的编号、描述、严重程度、阈值，以及在代码里对应哪个方法。

**规则以代码为准**：规则完整定义在清洗类的 `rules` 列表里（编号、版本、严重程度、阈值、检查方法），本表由代码在运行时自动同步，**不要直接改本表**。本表的作用是：给 `quality_issues` / `data_corrections` 提供外键，以及审计时查"某个版本的规则当时是什么"。

规则带版本：阈值或判定逻辑变了（比如单日振幅从 15% 改成 20%），**新增一行 `rule_version + 1`，旧版本保留并设为不生效**，不修改旧行。这样 `quality_issues` 里的每条历史问题，都能查到当时用的是哪版规则、什么阈值。

## Grain

**一行 = 一条规则的一个版本**。

主键：`(rule_id, rule_version)`。每个 `rule_id` 同一时间只有一个 `is_active = true` 的版本。

## Source and lineage

- **规则定义**：清洗类的 `rules` 列表，每条是一个 `Rule`（[`clean/base_clean.py`](../../clean/base_clean.py)）。目前都在 [`clean/clean_price_volume_fmp_dev.py`](../../clean/clean_price_volume_fmp_dev.py) 的 `PriceVolume` 类
- **写入方**：[`db/meta_store.py`](../../db/meta_store.py) 的 `MetaStore.sync_rules(PriceVolume.rule_definitions())`，由 [`pipelines/clean_runner.py`](../../pipelines/clean_runner.py) 在每次清洗前自动调用
  - 代码里有、本表没有的版本 → 插入，同一规则的旧版本设为不生效
  - 本表已有这个版本，但 severity / params 和代码不一致 → **报错**（改了规则必须升版本号）
  - 代码里的版本低于本表最高版本 → **报错**（防止旧代码覆盖新规则）
  - 整次同步在一个事务里，任何一条报错都整体回滚
- **初始数据**：[002](../../sql/postgres/002_meta_quality.sql) 写入了 PV001–PV004 的 v1，之后都由代码同步
- **与代码的对应**：`impl` 字段 = `类名.检查方法名`
- **下游**：
  - `meta.quality_issues (rule_id, rule_version)` 外键引用本表
  - `meta.data_corrections (rule_id, rule_version)` 外键引用本表（规则自动处理时）

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `rule_id` | text | 否 | — | 规则编号，「接口缩写 + 3 位序号」，如 `PV001` |
| `rule_version` | integer | 否 | 1 | 规则版本，从 1 开始 |
| `endpoint` | text | 否 | — | 适用的 FMP 接口 |
| `description` | text | 否 | — | 规则描述 |
| `severity` | text | 否 | — | `warning` / `danger` |
| `params` | jsonb | 否 | `{}` | 阈值等参数，如 `{"max_range": 0.15}` |
| `impl` | text | 是 | — | 代码里对应的方法 |
| `is_active` | boolean | 否 | true | 是否为当前生效版本 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间（UTC） |

### 约束与索引

| 名称 | 规则 |
|---|---|
| `pk_quality_rules` | `(rule_id, rule_version)` 唯一 |
| `chk_quality_rules_version_positive` | `rule_version ≥ 1` |
| `chk_quality_rules_severity` | `severity` 只能是 `warning` / `danger` |
| `uq_quality_rules_active` (`rule_id`) WHERE `is_active` | 每条规则同一时间只有一个生效版本 |

## 当前规则（代码中的定义）

| rule_id | 版本 | severity | 描述 | params | impl |
|---|---|---|---|---|---|
| PV001 | v1 | danger | 价格 ≤ 0 或成交量 < 0（成交量为 0 不算，可能是停牌） | — | `PriceVolume._check_positive_price` |
| PV002 | v1 | danger | OHLC 逻辑矛盾：开 / 收盘价不在最高最低价之间，或最高价 < 最低价 | — | `PriceVolume._check_logic_relation` |
| PV003 | **v2** | warning | 单日振幅过大：OHLC 最大值 > 最小值 × (1 + max_range)；**价格 ≤ 0 的行不检查** | `{"max_range": 0.15}` | `PriceVolume._check_daily_volatility` |
| PV004 | v1 | warning | 收益率跳变：相邻两日收益率之差的绝对值 > max_jump | `{"max_jump": 0.15}` | `PriceVolume._check_jump_soar` |
| PV005 | v1 | danger | 价格或成交量缺失（NaN 参与比较永远为 False，其他规则抓不到） | — | `PriceVolume._check_missing_values` |

版本变更记录：

| 规则 | 版本 | 变更 |
|---|---|---|
| PV003 | v1 → v2 | 跳过价格 ≤ 0 的行。这类行已由 PV001 隔离，最小值为 0 会让振幅检查必然触发，产生多余的 warning |
| PV005 | 新增 v1 | 拦截价格 / 成交量缺失的行 |

> 2026-10-04 第一次运行 C 步骤时已自动同步：正式库里 PV003 v1 已设为不生效，v2 生效；PV005 v1 已插入。

## 升级规则的做法

只改代码，不碰数据库：

```python
# clean/clean_price_volume_fmp_dev.py：例如 PV003 阈值从 15% 改成 20%
Rule("PV003", 3, "warning", "单日振幅过大：...",          # 版本号 2 → 3
     check="_check_daily_volatility", params={"max_range": 0.20}),
```

下次运行时 `sync_rules()` 会插入 v3、把 v2 设为不生效。以下情况都要升版本号：
- 改了 `params`（阈值）或 `severity` —— 不升会被 `sync_rules()` 拒绝
- 改了检查方法里的**判定逻辑**（如 PV003 v2 加了"跳过价格 ≤ 0"）—— `sync_rules()` 检测不到逻辑变化，需要自觉升版本
