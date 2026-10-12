-- =========================================================
-- raw.financial_ratio：财务比率（FMP ratios）；年报和季报都在这张表，用 period 区分
-- 定位：FMP 给什么存什么 + 追溯字段；只做结构检查，不做质量检查（质量检查在 clean/ 的清洗类）
-- 只追加、不去重：同一条被拉多次就存多行，用 batch_id 区分；去重 / 版本在 golden 层处理
-- 字段名与 FMP 返回一致；金额 / 比率一律 Nullable(Float64)（FMP 返回美元整数，Float64 可精确到 9e15）
-- 日期用 Date32（1900–2299）：Date 只能存 1970 年以后，老公司的上市日期等会写不进去
-- 唯一键（非空）：symbol, date, period
-- =========================================================
CREATE TABLE IF NOT EXISTS raw.financial_ratio
(
    -- FMP 原始字段
    symbol                                   LowCardinality(String),
    date                                     Date32 COMMENT '报告期结束日（FMP 的 date）',
    fiscalYear                               Nullable(String) COMMENT '财年（FMP 原样字符串）',
    period                                   LowCardinality(String) COMMENT 'FY = 年报；Q1–Q4 = 季报',
    reportedCurrency                         LowCardinality(Nullable(String)),
    grossProfitMargin                        Nullable(Float64),
    ebitMargin                               Nullable(Float64),
    ebitdaMargin                             Nullable(Float64),
    operatingProfitMargin                    Nullable(Float64),
    pretaxProfitMargin                       Nullable(Float64),
    continuousOperationsProfitMargin         Nullable(Float64),
    netProfitMargin                          Nullable(Float64),
    bottomLineProfitMargin                   Nullable(Float64),
    receivablesTurnover                      Nullable(Float64),
    payablesTurnover                         Nullable(Float64),
    inventoryTurnover                        Nullable(Float64),
    fixedAssetTurnover                       Nullable(Float64),
    assetTurnover                            Nullable(Float64),
    currentRatio                             Nullable(Float64),
    quickRatio                               Nullable(Float64),
    solvencyRatio                            Nullable(Float64),
    cashRatio                                Nullable(Float64),
    priceToEarningsRatio                     Nullable(Float64),
    priceToEarningsGrowthRatio               Nullable(Float64),
    forwardPriceToEarningsGrowthRatio        Nullable(Float64),
    priceToEarningsDilutedRatio              Nullable(Float64),
    priceToEarningsDilutedGrowthRatio        Nullable(Float64),
    priceToBookRatio                         Nullable(Float64),
    priceToSalesRatio                        Nullable(Float64),
    priceToFreeCashFlowRatio                 Nullable(Float64),
    priceToOperatingCashFlowRatio            Nullable(Float64),
    debtToAssetsRatio                        Nullable(Float64),
    debtToEquityRatio                        Nullable(Float64),
    debtToCapitalRatio                       Nullable(Float64),
    longTermDebtToCapitalRatio               Nullable(Float64),
    financialLeverageRatio                   Nullable(Float64),
    workingCapitalTurnoverRatio              Nullable(Float64),
    operatingCashFlowRatio                   Nullable(Float64),
    operatingCashFlowSalesRatio              Nullable(Float64),
    freeCashFlowOperatingCashFlowRatio       Nullable(Float64),
    debtServiceCoverageRatio                 Nullable(Float64),
    interestCoverageRatio                    Nullable(Float64),
    shortTermOperatingCashFlowCoverageRatio  Nullable(Float64),
    operatingCashFlowCoverageRatio           Nullable(Float64),
    capitalExpenditureCoverageRatio          Nullable(Float64),
    dividendPaidAndCapexCoverageRatio        Nullable(Float64),
    dividendPayoutRatio                      Nullable(Float64),
    dividendYield                            Nullable(Float64),
    dividendYieldPercentage                  Nullable(Float64),
    revenuePerShare                          Nullable(Float64),
    netIncomePerShare                        Nullable(Float64),
    interestDebtPerShare                     Nullable(Float64),
    cashPerShare                             Nullable(Float64),
    bookValuePerShare                        Nullable(Float64),
    tangibleBookValuePerShare                Nullable(Float64),
    shareholdersEquityPerShare               Nullable(Float64),
    operatingCashFlowPerShare                Nullable(Float64),
    capexPerShare                            Nullable(Float64),
    freeCashFlowPerShare                     Nullable(Float64),
    netIncomePerEBT                          Nullable(Float64),
    ebtPerEbit                               Nullable(Float64),
    priceToFairValue                         Nullable(Float64),
    debtToMarketCap                          Nullable(Float64),
    effectiveTaxRate                         Nullable(Float64),
    enterpriseValueMultiple                  Nullable(Float64),
    dividendPerShare                         Nullable(Float64),
    extra                                    String DEFAULT '{}' COMMENT 'FMP 多返回的、表里没定义的字段，打包成 JSON',

    -- 追溯字段
    batch_id              LowCardinality(String)                   COMMENT '对应 meta.ingestion.batch_id',
    source                LowCardinality(String) DEFAULT 'fmp',
    endpoint              LowCardinality(String) DEFAULT 'financial_ratio',
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
COMMENT '财务比率原始数据；只追加、不去重';
