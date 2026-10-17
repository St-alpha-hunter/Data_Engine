-- =========================================================
-- golden.securities：证券基本信息（看板和查询里把 security_id 换成代码 / 名称用）
-- 来源：PostgreSQL 证券主表（ref.securities + 最后使用的代码）+ 最近一次 raw.profile 的行业 / 板块
-- 每次写 golden 时整表刷新（pipelines/golden_loader.refresh_securities，几百行）；不是主数据，主数据以 ref.securities 为准
-- =========================================================
CREATE TABLE IF NOT EXISTS golden.securities
(
    security_id       UInt64                                   COMMENT 'ref.securities.security_id',
    symbol            LowCardinality(String)                   COMMENT '现在使用的代码；已退市的是退市前最后使用的代码',
    name              String,
    status            LowCardinality(String)                   COMMENT 'active / delisted',
    security_type     LowCardinality(String),
    primary_exchange  LowCardinality(Nullable(String)),
    sector            LowCardinality(Nullable(String))         COMMENT '板块，来自最近一次拉取的公司概况',
    industry          LowCardinality(Nullable(String))         COMMENT '行业，来自最近一次拉取的公司概况',
    first_trade_date  Nullable(Date32),
    last_trade_date   Nullable(Date32),
    refreshed_at      DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '这次整表刷新的时间'
)
ENGINE = MergeTree
ORDER BY security_id
COMMENT '证券基本信息（从证券主表整表刷新，看板用）';
