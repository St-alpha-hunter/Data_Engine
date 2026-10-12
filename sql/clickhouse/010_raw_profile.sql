-- =========================================================
-- raw.profile：公司概况（FMP profile）：拉取当时的快照，没有日期字段，快照时间看 ingested_at
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.profile
(
    -- FMP 原始字段
    symbol             LowCardinality(String),
    price              Nullable(Float64),
    marketCap          Nullable(Float64),
    beta               Nullable(Float64),
    lastDividend       Nullable(Float64),
    range              Nullable(String),
    change             Nullable(Float64),
    changePercentage   Nullable(Float64),
    volume             Nullable(Float64),
    averageVolume      Nullable(Float64),
    companyName        Nullable(String),
    currency           LowCardinality(Nullable(String)),
    cik                Nullable(String),
    isin               Nullable(String),
    cusip              Nullable(String),
    exchangeFullName   LowCardinality(Nullable(String)),
    exchange           LowCardinality(Nullable(String)),
    industry           LowCardinality(Nullable(String)),
    website            Nullable(String),
    description        Nullable(String),
    ceo                Nullable(String),
    sector             LowCardinality(Nullable(String)),
    country            LowCardinality(Nullable(String)),
    fullTimeEmployees  Nullable(UInt32),
    phone              Nullable(String),
    address            Nullable(String),
    city               Nullable(String),
    state              Nullable(String),
    zip                Nullable(String),
    image              Nullable(String),
    ipoDate            Nullable(Date32),
    defaultImage       Nullable(Bool),
    isEtf              Nullable(Bool),
    isActivelyTrading  Nullable(Bool),
    isAdr              Nullable(Bool),
    isFund             Nullable(Bool),
    extra              String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'profile',
    source_logic_version  UInt16 DEFAULT 1                         COMMENT 'FMP 计算口径版本，来自 config/endpoints.py',
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '入库时间（UTC）',

    -- ClickHouse 写入 NULL 到非 Nullable 列时会静默转成默认值（'' / 1900-01-01），这里兜底拦截
    CONSTRAINT chk_symbol_not_empty   CHECK symbol != '',
    CONSTRAINT chk_batch_id_not_empty CHECK batch_id != ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (batch_id, symbol)
COMMENT '公司概况原始数据；只追加、不去重';
