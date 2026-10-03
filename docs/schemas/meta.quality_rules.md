# meta.quality_rules

> PostgreSQL `data_engine.meta` · 建表 SQL：[002_meta_quality.sql](../../sql/postgres/002_meta_quality.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

数据质量校验规则的**清单**：每条规则的编号、描述、严重程度、阈值，以及在代码里对应哪个方法。

规则带版本：阈值或判定逻辑变了（比如单日振幅从 15% 改成 20%），**新增一行 `rule_version + 1`，旧版本保留并设为不生效**，不修改旧行。这样 `quality_issues` 里的每条历史问题，都能查到当时用的是哪版规则、什么阈值。

## Grain

**一行 = 一条规则的一个版本**。

主键：`(rule_id, rule_version)`。每个 `rule_id` 同一时间只有一个 `is_active = true` 的版本。

## Source and lineage

- **写入方**：人工维护，通过 `sql/postgres/` 下的编号 SQL 文件新增或升级规则
- **与代码的对应**：`impl` 字段指向实现这条规则的方法（目前都在 [`fetch_data/fetch_price_volume_fmp_dev.py`](../../fetch_data/fetch_price_volume_fmp_dev.py) 的 `PriceVolume` 类）。改阈值时**代码和本表要同时改**
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

## 当前规则（v1）

| rule_id | severity | 描述 | params | impl |
|---|---|---|---|---|
| PV001 | danger | 价格 ≤ 0 或成交量 < 0 | — | `PriceVolume._check_positive_price` |
| PV002 | danger | OHLC 逻辑矛盾：开 / 收盘价不在最高最低价之间，或最高价 < 最低价 | — | `PriceVolume._check_logic_relation` |
| PV003 | warning | 单日振幅过大：OHLC 最大值 > 最小值 × (1 + max_range) | `{"max_range": 0.15}` | `PriceVolume._check_daily_volatility` |
| PV004 | warning | 收益率跳变：相邻两日收益率之差的绝对值 > max_jump | `{"max_jump": 0.15}` | `PriceVolume._check_jump_soar` |

## 升级规则的做法

```sql
-- 例：PV003 阈值从 15% 改成 20%
BEGIN;
UPDATE meta.quality_rules SET is_active = false WHERE rule_id = 'PV003' AND is_active;
INSERT INTO meta.quality_rules (rule_id, rule_version, endpoint, description, severity, params, impl)
VALUES ('PV003', 2, 'price_volume', '单日振幅过大：OHLC 最大值 > 最小值 × (1 + max_range)',
        'warning', '{"max_range": 0.20}', 'PriceVolume._check_daily_volatility');
COMMIT;
```

先关旧版本、再插新版本，必须放在同一个事务里，否则会违反 `uq_quality_rules_active`。
