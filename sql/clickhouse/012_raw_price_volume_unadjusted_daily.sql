-- =========================================================
-- raw.price_volume_unadjusted_daily：FMP 不复权日线（historical-price-eod/non-split-adjusted）
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查
-- 只追加、不去重：同一 (symbol, date) 被拉多次就存多行，用 batch_id 区分
-- ⚠ 字段名照抄 FMP（adjOpen / adjClose …），但这个接口返回的是**当时真实成交的价格和成交量**，没有任何复权：
--   实测 AAPL 2020-08-28 收盘 499.24、拆股后 08-31 收盘 129.04；拆股前成交量也没有乘以 4
-- golden 以它为主：不复权价不会因为后来的分红 / 拆股被改写；复权价由事件库算出的复权因子在视图里现算
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.price_volume_unadjusted_daily
(
    -- FMP 原始字段（字段名与 FMP 返回一致；实际都是不复权的）
    symbol                LowCardinality(String),
    date                  Date32,
    adjOpen               Nullable(Float64)                        COMMENT '不复权开盘价（FMP 字段名带 adj，但没有复权）',
    adjHigh               Nullable(Float64)                        COMMENT '不复权最高价',
    adjLow                Nullable(Float64)                        COMMENT '不复权最低价',
    adjClose              Nullable(Float64)                        COMMENT '不复权收盘价',
    volume                Nullable(Float64)                        COMMENT '当时的真实成交量（拆股前没有按拆股比例放大）',
    extra                 String DEFAULT '{}'                      COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'price_volume_unadjusted',
    source_logic_version  UInt16 DEFAULT 1                         COMMENT 'FMP 计算口径版本，来自 config/endpoints.py',
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '入库时间（UTC）',

    -- ClickHouse 写入 NULL 到非 Nullable 列时会静默转成默认值（'' / 1900-01-01），这里兜底拦截
    CONSTRAINT chk_symbol_not_empty   CHECK symbol != '',
    CONSTRAINT chk_date_not_default   CHECK date > toDate32('1900-01-01'),
    CONSTRAINT chk_batch_id_not_empty CHECK batch_id != ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (batch_id, symbol, date)
COMMENT 'FMP 不复权日线原始数据；只追加、不去重';
