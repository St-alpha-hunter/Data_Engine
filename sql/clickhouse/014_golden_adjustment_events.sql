-- =========================================================
-- golden.adjustment_events：复权事件和复权因子，带版本
-- 来源：Postgres events.splits / events.dividends（已回填 security_id）+ golden.price_daily（除权前一天收盘价）
-- 复权方式（CRSP 同类做法）：除权日以前的价格 × price_factor，成交量 × volume_factor
--     拆股（含合股、股票股利）：price_factor = 1 / 比例，volume_factor = 比例（4 拆 1：比例 = 4）
--     现金分红（常规 + 特别）：price_factor = 1 − 每股金额 / 除息前一天不复权收盘价，volume_factor = 1
-- 只追加：事件被修订（revision 变了）或因子变了才写新版本；当前版本和累积因子看 golden.v_adjustment_factors
-- =========================================================
CREATE TABLE IF NOT EXISTS golden.adjustment_events
(
    security_id     UInt64,
    ex_date         Date32                                   COMMENT '除权 / 除息日',
    kind            LowCardinality(String)                   COMMENT 'split / dividend',
    value           Float64                                  COMMENT '拆股：比例（新股数 / 旧股数）；分红：每股金额（不复权）',
    prev_close      Nullable(Float64)                        COMMENT '除权前一个交易日的不复权收盘价（算分红因子用）',
    price_factor    Float64                                  COMMENT '除权日以前的价格要乘的系数',
    volume_factor   Float64                                  COMMENT '除权日以前的成交量要乘的系数',
    event_id        UInt64                                   COMMENT 'events.corporate_events.event_id',
    event_revision  UInt32,
    known_at        DateTime64(3, 'UTC')                     COMMENT '事件被我们得知的时间（按时点查询用）',
    version         UInt32,
    loaded_at       DateTime64(3, 'UTC') DEFAULT now64(3),

    CONSTRAINT chk_kind         CHECK kind IN ('split', 'dividend'),
    CONSTRAINT chk_price_factor CHECK price_factor > 0,
    CONSTRAINT chk_vol_factor   CHECK volume_factor > 0,
    CONSTRAINT chk_version      CHECK version >= 1
)
ENGINE = MergeTree
ORDER BY (security_id, ex_date, kind, event_id, version)
COMMENT '复权事件与复权因子（golden）；只追加，有变化才写新版本';
