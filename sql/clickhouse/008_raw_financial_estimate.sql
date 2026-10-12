-- =========================================================
-- raw.financial_estimate：分析师一致预期（FMP analyst-estimates），只拉年度预测；date = 被预测的财年结束日，可以在未来
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol, date
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.financial_estimate
(
    -- FMP 原始字段
    symbol              LowCardinality(String),
    date                Date32,
    revenueLow          Nullable(Float64),
    revenueHigh         Nullable(Float64),
    revenueAvg          Nullable(Float64),
    ebitdaLow           Nullable(Float64),
    ebitdaHigh          Nullable(Float64),
    ebitdaAvg           Nullable(Float64),
    ebitLow             Nullable(Float64),
    ebitHigh            Nullable(Float64),
    ebitAvg             Nullable(Float64),
    netIncomeLow        Nullable(Float64),
    netIncomeHigh       Nullable(Float64),
    netIncomeAvg        Nullable(Float64),
    sgaExpenseLow       Nullable(Float64),
    sgaExpenseHigh      Nullable(Float64),
    sgaExpenseAvg       Nullable(Float64),
    epsAvg              Nullable(Float64),
    epsHigh             Nullable(Float64),
    epsLow              Nullable(Float64),
    numAnalystsRevenue  Nullable(UInt32),
    numAnalystsEps      Nullable(UInt32),
    extra               String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'financial_estimate',
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
COMMENT '分析师一致预期原始数据；只追加、不去重';
