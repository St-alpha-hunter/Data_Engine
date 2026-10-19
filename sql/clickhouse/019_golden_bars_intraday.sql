-- golden.bars_intraday：分钟线（5min / 15min / 1h / 1d）的查询副本
--   正本在湖里（lakehouse/bars，Spark 流任务写）；每日流程的 sync_intraday 把当天的结果同步过来（先删这一天再写入，重跑不会重复）
--   只同步通过校验的：5 分钟线里被隔离的（quality_status = quarantined）不同步，合成时也没用它们
--   5 分钟线只含常规时段；1d 是分钟线合成的日线（不含收盘集合竞价、盘前盘后），日线以 golden.price_daily 为准
CREATE TABLE IF NOT EXISTS golden.bars_intraday
(
    security_id     UInt64                                   COMMENT 'ref.securities.security_id',
    symbol          LowCardinality(String),
    `interval`      LowCardinality(String)                   COMMENT '5min / 15min / 1h / 1d',
    bar_start       DateTime64(0, 'UTC')                     COMMENT 'K 线开始时刻（UTC）；1h 从纽约 9:30 对齐',
    trade_date      Date32                                   COMMENT '纽约日期',
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    n_bars          UInt16                                   COMMENT '由几根 5 分钟线合成（5min 为 1）',
    is_complete     UInt8                                    COMMENT '根数够不够（按交易日历；提前收盘的日子 1d 只要 42 根）',
    quality_status  LowCardinality(String)                   COMMENT 'health / warning（合成的 K 线取所含 5 分钟线里最差的）/ unchecked（加清洗之前的旧数据）',
    batch_id        LowCardinality(String)                   COMMENT '5min：对应 meta.ingestion.batch_id；合成的为空',
    computed_at     DateTime64(3, 'UTC')                     COMMENT 'Spark 计算的时间',
    synced_at       DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '同步进 ClickHouse 的时间',

    CONSTRAINT chk_interval CHECK `interval` IN ('5min', '15min', '1h', '1d'),
    CONSTRAINT chk_status   CHECK quality_status IN ('health', 'warning', 'unchecked')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (`interval`, security_id, bar_start)
COMMENT '分钟线（湖 lakehouse/bars 的查询副本，每日按交易日整天替换）';
