# ref.security_identifiers

> PostgreSQL `data_engine.ref` · 建表 SQL：[010_ref_securities.sql](../../sql/postgres/010_ref_securities.sql) · 返回 [数据字典](../data-dictionary.md)

## Purpose

证券的**代码历史**：每只证券在什么时间用什么 symbol / CIK / CUSIP / ISIN。

解决的核心问题：**按代码 + 日期，找到当时是哪只证券**。股票代码会改名、会被重复使用，只按代码去拉数据会拿到别的公司（实测：按 WB 拉 2008 年的美联银行，拿回来的是微博 2014 年以后的数据）。以后写入事件、拉行情、导入成分股，都先通过 `ref.resolve_security()` 把代码对应到 `security_id`。

## Grain

**一行 = 一只证券的一种代码的一段有效期**。

有效期约定 **`[valid_from, valid_to)`：包含开始日、不包含结束日**；`valid_to` 为空表示现在仍有效。前后两段首尾相接，不会重叠也不会空一天：

```
security_id=3  symbol  FB    [2012-05-18, 2022-06-09)
security_id=3  symbol  META  [2022-06-09, )          ← 交界日 2022-06-09 属于 META
```

主键：`identifier_id`（自增）。

## Source and lineage

- **上游**：`security_id` → `ref.securities`
- **来源**（`source` 字段）：

| source | 内容 |
|---|---|
| `fmp_profile` | FMP profile 里的 CIK / CUSIP / ISIN（只有当前值，有效期从首个交易日开始，是近似值；上市日存疑的从 profile 上市日开始） |
| `fmp_symbol_change` | 用 FMP 代码变更补上的改名前代码（比如 ATH → WLP → ANTM） |
| `sec` | SEC 官方代码表里的 CIK（profile 没有 CIK 时才用） |
| `fmp_acquisition` | 收购记录里被收购方的 CIK（已退市、没有 profile 的） |
| `sp500_github` | S&P 500 历史成分里的代码 |
| `manual` | 人工对照（`config/security_overrides.csv`） |

**2026-10-12 首次生成时的分布**：symbol 993 条（sp500_github 837、fmp_symbol_change 126、manual 30），CIK 684 条，CUSIP 656 条，ISIN 660 条。

- **CIK / CUSIP / ISIN 的有效期是近似值**：FMP 只给当前值，所以一律从首个交易日开始。如果中间换过（比如谷歌 2015 年重组为 Alphabet，CIK 变了），早年的值是不对的。目前没有历史数据可以补
- **`event_id`**：由哪个事件引起（如某次代码变更）。首次生成时没有填，以后增量维护改名时填上
- **写入方**：[`pipelines/security_master.py`](../../pipelines/security_master.py)（首次生成，流程见 [ref.securities](ref.securities.md)）；之后增量维护

## Schemas

| 字段 | 类型 | 可空 | 默认值 | 说明 |
|---|---|---|---|---|
| `identifier_id` | bigint | 否 | 自增 | 主键 |
| `security_id` | bigint | 否 | — | 属于哪只证券 |
| `id_type` | text | 否 | — | `symbol` / `cik` / `cusip` / `isin` |
| `id_value` | text | 否 | — | 代码值 |
| `valid_from` | date | 否 | — | 开始日（含） |
| `valid_to` | date | 是 | — | 结束日（不含）；为空表示仍有效 |
| `source` | text | 否 | — | 数据来源，见上表 |
| `event_id` | bigint | 是 | — | 引起这段变化的事件 |
| `created_at` | timestamptz | 否 | `now()` | |

### 约束

| 名称 | 规则 |
|---|---|
| `pk_security_identifiers` | `identifier_id` 唯一 |
| `fk_security_identifiers_security` | `security_id` 必须存在于 `ref.securities` |
| `chk_security_identifiers_type` | 只能是 4 种代码 |
| `chk_security_identifiers_range` | `valid_to` 晚于 `valid_from` |
| `chk_security_identifiers_symbol_format` | 大写字母、数字、`.`、`-`（如 `BRK.B`） |
| `chk_security_identifiers_cik_format` | 10 位数字（不足补零，如 `0001326801`） |
| `chk_security_identifiers_cusip_format` | 9 位 |
| `chk_security_identifiers_isin_format` | 2 位国家代码 + 9 位 + 1 位校验数字 |
| `exc_security_identifiers_value_one_security` | **排除约束：同一个 symbol / CUSIP / ISIN，同一时间只能指向一只证券**（WB 2009 年前是美联银行、2014 年后是微博，时间不重叠所以能共存）。**CIK 不受限制**（[018](../../sql/postgres/018_identifiers_cik_shared.sql)）：CIK 代表公司，同一家公司的多类股票共用一个，比如 GOOG 和 GOOGL |
| `idx_security_identifiers_cik` | 按 CIK 查证券的索引（018） |
| `exc_security_identifiers_one_value_per_type` | **排除约束：同一只证券、同一种代码，同一时间只能有一个值** |

排除约束需要 `btree_gist` 扩展（010 里已启用）。

## 查询接口

| 接口 | 用途 |
|---|---|
| `ref.resolve_security(id_type, id_value, date)` | 某个代码在某一天指向哪只证券；没有则返回 NULL。**按 CIK 查时可能多只共用**，这个函数只返回其中一只，所以按 CIK 查要直接查表 |
| `ref.securities_asof(date)` | 某一天每只证券的 symbol / CIK / CUSIP / ISIN（回测用当天的代码，不用今天的） |
| `ref.v_securities_current` | 当前视图（大宽表）：每只证券一行，代码为当前值 |

```sql
-- 2008 年的 WB 是谁？→ 美联银行；今天的 WB 是谁？→ 微博
SELECT ref.resolve_security('symbol', 'WB', '2008-06-30'), ref.resolve_security('symbol', 'WB', CURRENT_DATE);

-- 2020-01-02 那天每只证券用什么代码
SELECT * FROM ref.securities_asof('2020-01-02');

-- 某只证券的全部代码历史
SELECT id_type, id_value, valid_from, valid_to, source
FROM ref.security_identifiers WHERE security_id = 3 ORDER BY id_type, valid_from;

-- 代码变更：关闭旧代码、开始新代码（同一个事务）
BEGIN;
UPDATE ref.security_identifiers SET valid_to = '2022-06-09'
WHERE security_id = 3 AND id_type = 'symbol' AND id_value = 'FB' AND valid_to IS NULL;
INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, source)
VALUES (3, 'symbol', 'META', '2022-06-09', 'fmp_symbol_change');
COMMIT;
```
