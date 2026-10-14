-- =========================================================
-- golden.price_daily：不复权日线（golden 主数据），带版本
-- 来源：raw.price_volume_unadjusted_daily → 清洗 → 中间态 → 本表（pipelines/golden_loader.py）
-- 按证券编号存（security_id），不按代码：代码改名、被别的公司重复使用时数据不会串
-- 只追加：同一证券同一天，数值（或 quality_status）有变化才写新版本；当前版本看 golden.v_price_daily
-- 复权价不存：由 golden.adjustment_events 的复权因子在 golden.v_price_daily_adjusted 里现算（不复权价不会被后来的分红改写）
-- =========================================================
CREATE TABLE IF NOT EXISTS golden.price_daily
(
    security_id           UInt64                                   COMMENT 'ref.securities.security_id',
    date                  Date32,
    open                  Nullable(Float64)                        COMMENT '不复权开盘价',
    high                  Nullable(Float64)                        COMMENT '不复权最高价',
    low                   Nullable(Float64)                        COMMENT '不复权最低价',
    close                 Nullable(Float64)                        COMMENT '不复权收盘价',
    volume                Nullable(Float64)                        COMMENT '当时真实成交量',
    quality_status        LowCardinality(String)                   COMMENT 'health / warning / released（人工放行的隔离行）',
    source                LowCardinality(String)                   COMMENT '数据来源，如 fmp',
    source_symbol         LowCardinality(String)                   COMMENT '向数据源请求时用的代码（追溯用）',
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source_logic_version  UInt16,
    version               UInt32                                   COMMENT '同一证券同一天的第几个版本，从 1 开始',
    loaded_at             DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '写入 golden 的时间；按时点查询用它',

    CONSTRAINT chk_security_id CHECK security_id > 0,
    CONSTRAINT chk_version     CHECK version >= 1,
    CONSTRAINT chk_status      CHECK quality_status IN ('health', 'warning', 'released')
)
ENGINE = MergeTree
PARTITION BY toYear(date)
ORDER BY (security_id, date, version)
COMMENT '不复权日线（golden）；只追加，数值有变化才写新版本';
