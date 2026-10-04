-- =========================================================
-- raw.price_volume_daily：FMP 日线原始数据（historical-price-eod/dividend-adjusted）
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查
-- 只追加、不去重：同一 (symbol, date) 被拉多次就存多行，用 batch_id 区分
-- 去重 / 版本 / quality_status 都在 golden 层处理
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.price_volume_daily
(
    -- FMP 原始字段（字段名与 FMP 返回一致）
    symbol                LowCardinality(String),
    date                  Date,
    adjOpen               Nullable(Float64),
    adjHigh               Nullable(Float64),
    adjLow                Nullable(Float64),
    adjClose              Nullable(Float64),
    volume                Nullable(Float64),
    extra                 String DEFAULT '{}'                      COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'price_volume',
    source_logic_version  UInt16 DEFAULT 1                         COMMENT 'FMP 计算口径版本，来自 config/endpoints.py',
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '入库时间（UTC）',

    -- ClickHouse 写入 NULL 到非 Nullable 列时会静默转成默认值（'' / 1970-01-01），这里兜底拦截
    CONSTRAINT chk_symbol_not_empty   CHECK symbol != '',
    CONSTRAINT chk_date_not_default   CHECK date > toDate('1970-01-01'),
    CONSTRAINT chk_batch_id_not_empty CHECK batch_id != ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (batch_id, symbol, date)
COMMENT 'FMP 日线原始数据；只追加、不去重';
