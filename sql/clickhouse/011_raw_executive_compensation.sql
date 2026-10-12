-- =========================================================
-- raw.executive_compensation：高管薪酬（FMP governance-executive-compensation）：一份委托书里一位高管一年的薪酬
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol, filingDate, nameAndPosition
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.executive_compensation
(
    -- FMP 原始字段
    cik                        Nullable(String),
    symbol                     LowCardinality(String),
    companyName                Nullable(String),
    filingDate                 Date32 COMMENT '向 SEC 提交日期；按时点（point-in-time）查询靠它',
    acceptedDate               Nullable(DateTime) COMMENT 'SEC 受理时间，FMP 原样（美东时间），没有换算时区',
    nameAndPosition            String,
    year                       Nullable(UInt16),
    salary                     Nullable(Float64),
    bonus                      Nullable(Float64),
    stockAward                 Nullable(Float64),
    optionAward                Nullable(Float64),
    incentivePlanCompensation  Nullable(Float64),
    allOtherCompensation       Nullable(Float64),
    total                      Nullable(Float64),
    link                       Nullable(String),
    extra                      String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'executive_compensation',
    source_logic_version  UInt16 DEFAULT 1                         COMMENT 'FMP 计算口径版本，来自 config/endpoints.py',
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '入库时间（UTC）',

    -- ClickHouse 写入 NULL 到非 Nullable 列时会静默转成默认值（'' / 1900-01-01），这里兜底拦截
    CONSTRAINT chk_symbol_not_empty   CHECK symbol != '',
    CONSTRAINT chk_filing_date_not_default CHECK filingDate > toDate32('1900-01-01'),
    CONSTRAINT chk_name_not_empty     CHECK nameAndPosition != '',
    CONSTRAINT chk_batch_id_not_empty CHECK batch_id != ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (batch_id, symbol, filingDate, nameAndPosition)
COMMENT '高管薪酬原始数据；只追加、不去重';
