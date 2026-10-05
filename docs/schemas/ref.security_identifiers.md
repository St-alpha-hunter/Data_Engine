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
| `fmp_profile` | 仍在交易的证券的当前 symbol / CIK / CUSIP / ISIN |
| `fmp_symbol_change` | 代码变更（FB → META）：关闭旧代码的一段、开始新代码的一段 |
| `sec` | SEC EDGAR：退市公司的 CIK |
| `sp500_github` | S&P 500 历史成分里的代码（需要结合代码变更核对） |
| `manual` | 人工核对后补录 |

- **`event_id`**：由哪个事件引起（如某次代码变更）。事件总表建好后再加外键
- **写入方**：证券主表的生成脚本（待写），之后由事件流维护

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
| `exc_security_identifiers_value_one_security` | **排除约束：同一个代码，同一时间只能指向一只证券**（WB 2009 年前美联银行、2014 年后微博，时间不重叠所以能共存） |
| `exc_security_identifiers_one_value_per_type` | **排除约束：同一只证券、同一种代码，同一时间只能有一个值** |

排除约束需要 `btree_gist` 扩展（010 里已启用）。

## 查询接口

| 接口 | 用途 |
|---|---|
| `ref.resolve_security(id_type, id_value, date)` | 某个代码在某一天指向哪只证券；没有则返回 NULL |
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
