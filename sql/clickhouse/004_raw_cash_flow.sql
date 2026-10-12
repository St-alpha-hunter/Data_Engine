-- =========================================================
-- raw.cash_flow：现金流量表（FMP cash-flow-statement）；年报和季报都在这张表，用 period 区分
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol, date, period
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.cash_flow
(
    -- FMP 原始字段
    date                                    Date32 COMMENT '报告期结束日（FMP 的 date）',
    symbol                                  LowCardinality(String),
    reportedCurrency                        LowCardinality(Nullable(String)),
    cik                                     Nullable(String),
    filingDate                              Nullable(Date32) COMMENT '向 SEC 提交日期；按时点（point-in-time）查询靠它',
    acceptedDate                            Nullable(DateTime) COMMENT 'SEC 受理时间，FMP 原样（美东时间），没有换算时区',
    fiscalYear                              Nullable(String) COMMENT '财年（FMP 原样字符串）',
    period                                  LowCardinality(String) COMMENT 'FY = 年报；Q1–Q4 = 季报',
    netIncome                               Nullable(Float64),
    depreciationAndAmortization             Nullable(Float64),
    deferredIncomeTax                       Nullable(Float64),
    stockBasedCompensation                  Nullable(Float64),
    changeInWorkingCapital                  Nullable(Float64),
    accountsReceivables                     Nullable(Float64),
    inventory                               Nullable(Float64),
    accountsPayables                        Nullable(Float64),
    otherWorkingCapital                     Nullable(Float64),
    otherNonCashItems                       Nullable(Float64),
    netCashProvidedByOperatingActivities    Nullable(Float64),
    investmentsInPropertyPlantAndEquipment  Nullable(Float64),
    acquisitionsNet                         Nullable(Float64),
    purchasesOfInvestments                  Nullable(Float64),
    salesMaturitiesOfInvestments            Nullable(Float64),
    otherInvestingActivities                Nullable(Float64),
    netCashProvidedByInvestingActivities    Nullable(Float64),
    netDebtIssuance                         Nullable(Float64),
    longTermNetDebtIssuance                 Nullable(Float64),
    shortTermNetDebtIssuance                Nullable(Float64),
    netStockIssuance                        Nullable(Float64),
    netCommonStockIssuance                  Nullable(Float64),
    commonStockIssuance                     Nullable(Float64),
    commonStockRepurchased                  Nullable(Float64),
    netPreferredStockIssuance               Nullable(Float64),
    netDividendsPaid                        Nullable(Float64),
    commonDividendsPaid                     Nullable(Float64),
    preferredDividendsPaid                  Nullable(Float64),
    otherFinancingActivities                Nullable(Float64),
    netCashProvidedByFinancingActivities    Nullable(Float64),
    effectOfForexChangesOnCash              Nullable(Float64),
    netChangeInCash                         Nullable(Float64),
    cashAtEndOfPeriod                       Nullable(Float64),
    cashAtBeginningOfPeriod                 Nullable(Float64),
    operatingCashFlow                       Nullable(Float64),
    capitalExpenditure                      Nullable(Float64),
    freeCashFlow                            Nullable(Float64),
    incomeTaxesPaid                         Nullable(Float64),
    interestPaid                            Nullable(Float64),
    extra                                   String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'cash_flow',
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
COMMENT '现金流量表原始数据；只追加、不去重';
