-- =========================================================
-- 股票代码字段统一叫 symbol（与 FMP 返回字段、raw / golden 表一致）
-- 只改名，不改数据；CHECK 约束、唯一约束、外键里的列名会自动跟着改
-- =========================================================

-- ---------- meta.ingestion ----------
ALTER TABLE meta.ingestion RENAME COLUMN n_ticker        TO n_symbols;
ALTER TABLE meta.ingestion RENAME COLUMN failed_tickers  TO failed_symbols;
ALTER TABLE meta.ingestion RENAME COLUMN missing_tickers TO missing_symbols;

ALTER TABLE meta.ingestion RENAME CONSTRAINT chk_ingestion_n_ticker_nonneg  TO chk_ingestion_n_symbols_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT chk_ingestion_counts_le_ticker TO chk_ingestion_counts_le_symbols;

-- ---------- meta.quality_issues ----------
ALTER TABLE meta.quality_issues RENAME COLUMN ticker TO symbol;
ALTER INDEX meta.idx_quality_issues_ticker_date RENAME TO idx_quality_issues_symbol_date;

-- ---------- meta.quarantine_records ----------
ALTER TABLE meta.quarantine_records RENAME COLUMN ticker TO symbol;
ALTER INDEX meta.idx_quarantine_ticker_date RENAME TO idx_quarantine_symbol_date;

-- ---------- 注释 ----------
COMMENT ON COLUMN meta.ingestion.n_symbols       IS '请求的股票数';
COMMENT ON COLUMN meta.ingestion.failed_symbols  IS '失败的股票列表';
COMMENT ON COLUMN meta.ingestion.missing_symbols IS '返回空数据的股票列表';
COMMENT ON COLUMN meta.quality_issues.symbol     IS '股票代码（与 FMP 字段名一致）';
COMMENT ON COLUMN meta.quarantine_records.symbol IS '股票代码（与 FMP 字段名一致）';
