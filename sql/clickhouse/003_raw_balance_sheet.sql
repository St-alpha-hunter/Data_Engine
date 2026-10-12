-- =========================================================
-- raw.balance_sheet：资产负债表（FMP balance-sheet-statement）；年报和季报都在这张表，用 period 区分
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol, date, period
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.balance_sheet
(
    -- FMP 原始字段
    date                                     Date32 COMMENT '报告期结束日（FMP 的 date）',
    symbol                                   LowCardinality(String),
    reportedCurrency                         LowCardinality(Nullable(String)),
    cik                                      Nullable(String),
    filingDate                               Nullable(Date32) COMMENT '向 SEC 提交日期；按时点（point-in-time）查询靠它',
    acceptedDate                             Nullable(DateTime) COMMENT 'SEC 受理时间，FMP 原样（美东时间），没有换算时区',
    fiscalYear                               Nullable(String) COMMENT '财年（FMP 原样字符串）',
    period                                   LowCardinality(String) COMMENT 'FY = 年报；Q1–Q4 = 季报',
    cashAndCashEquivalents                   Nullable(Float64),
    shortTermInvestments                     Nullable(Float64),
    cashAndShortTermInvestments              Nullable(Float64),
    netReceivables                           Nullable(Float64),
    accountsReceivables                      Nullable(Float64),
    otherReceivables                         Nullable(Float64),
    inventory                                Nullable(Float64),
    prepaids                                 Nullable(Float64),
    otherCurrentAssets                       Nullable(Float64),
    totalCurrentAssets                       Nullable(Float64),
    propertyPlantEquipmentNet                Nullable(Float64),
    goodwill                                 Nullable(Float64),
    intangibleAssets                         Nullable(Float64),
    goodwillAndIntangibleAssets              Nullable(Float64),
    longTermInvestments                      Nullable(Float64),
    taxAssets                                Nullable(Float64),
    otherNonCurrentAssets                    Nullable(Float64),
    totalNonCurrentAssets                    Nullable(Float64),
    otherAssets                              Nullable(Float64),
    totalAssets                              Nullable(Float64),
    totalPayables                            Nullable(Float64),
    accountPayables                          Nullable(Float64),
    otherPayables                            Nullable(Float64),
    accruedExpenses                          Nullable(Float64),
    shortTermDebt                            Nullable(Float64),
    capitalLeaseObligationsCurrent           Nullable(Float64),
    taxPayables                              Nullable(Float64),
    deferredRevenue                          Nullable(Float64),
    otherCurrentLiabilities                  Nullable(Float64),
    totalCurrentLiabilities                  Nullable(Float64),
    longTermDebt                             Nullable(Float64),
    capitalLeaseObligationsNonCurrent        Nullable(Float64),
    deferredRevenueNonCurrent                Nullable(Float64),
    deferredTaxLiabilitiesNonCurrent         Nullable(Float64),
    otherNonCurrentLiabilities               Nullable(Float64),
    totalNonCurrentLiabilities               Nullable(Float64),
    otherLiabilities                         Nullable(Float64),
    capitalLeaseObligations                  Nullable(Float64),
    totalLiabilities                         Nullable(Float64),
    treasuryStock                            Nullable(Float64),
    preferredStock                           Nullable(Float64),
    commonStock                              Nullable(Float64),
    retainedEarnings                         Nullable(Float64),
    additionalPaidInCapital                  Nullable(Float64),
    accumulatedOtherComprehensiveIncomeLoss  Nullable(Float64),
    otherTotalStockholdersEquity             Nullable(Float64),
    totalStockholdersEquity                  Nullable(Float64),
    totalEquity                              Nullable(Float64),
    minorityInterest                         Nullable(Float64),
    totalLiabilitiesAndTotalEquity           Nullable(Float64),
    totalInvestments                         Nullable(Float64),
    totalDebt                                Nullable(Float64),
    netDebt                                  Nullable(Float64),
    extra                                    String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'balance_sheet',
    source_logic_version  UInt16 DEFAULT 1                         COMMENT 'FMP 计算口径版本，来自 config/endpoints.py',
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3)    COMMENT '入库时间（UTC）',

    -- ClickHouse 写入 NULL 到非 Nullable 列时会静默转成默认值（'' / 1900-01-01），这里兜底拦截
    CONSTRAINT chk_symbol_not_empty   CHECK symbol != '',
    CONSTRAINT chk_date_not_default   CHECK date > toDate32('1900-01-01'),
    CONSTRAINT chk_period_not_empty   CHECK period != '',
    CONSTRAINT chk_batch_id_not_empty CHECK batch_id != ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (batch_id, symbol, date, period)
COMMENT '资产负债表原始数据；只追加、不去重';
