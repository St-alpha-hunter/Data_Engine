-- =========================================================
-- 把 001~003 里 PostgreSQL 自动生成的约束名改成有意义的名字
-- 只改名，不改约束内容，不影响数据
-- 命名规则：pk_ 主键 / uq_ 唯一 / chk_ 检查 / fk_ 外键
-- （主键、唯一约束改名时，背后的索引会一起改名）
-- =========================================================

-- ---------- meta.ingestion ----------
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_pkey               TO pk_ingestion;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_status_check       TO chk_ingestion_status;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_n_ticker_check     TO chk_ingestion_n_ticker_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_n_success_check    TO chk_ingestion_n_success_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_n_missing_check    TO chk_ingestion_n_missing_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_n_failed_check     TO chk_ingestion_n_failed_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_rows_fetched_check TO chk_ingestion_rows_fetched_nonneg;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_check              TO chk_ingestion_finished_after_started;
ALTER TABLE meta.ingestion RENAME CONSTRAINT ingestion_check1             TO chk_ingestion_counts_le_ticker;

-- ---------- meta.quality_rules ----------
ALTER TABLE meta.quality_rules RENAME CONSTRAINT quality_rules_pkey               TO pk_quality_rules;
ALTER TABLE meta.quality_rules RENAME CONSTRAINT quality_rules_rule_version_check TO chk_quality_rules_version_positive;
ALTER TABLE meta.quality_rules RENAME CONSTRAINT quality_rules_severity_check     TO chk_quality_rules_severity;

-- ---------- meta.quality_issues ----------
ALTER TABLE meta.quality_issues RENAME CONSTRAINT quality_issues_pkey                      TO pk_quality_issues;
ALTER TABLE meta.quality_issues RENAME CONSTRAINT quality_issues_batch_id_fkey             TO fk_quality_issues_batch;
ALTER TABLE meta.quality_issues RENAME CONSTRAINT quality_issues_rule_id_rule_version_fkey TO fk_quality_issues_rule;
ALTER TABLE meta.quality_issues RENAME CONSTRAINT quality_issues_severity_check            TO chk_quality_issues_severity;

-- ---------- meta.quarantine_records ----------
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_pkey                              TO pk_quarantine_records;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_batch_id_fkey                     TO fk_quarantine_records_batch;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_batch_id_endpoint_ticker_date_key TO uq_quarantine_records_record;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_status_check                      TO chk_quarantine_status;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_check                             TO chk_quarantine_resolved_iff_not_pending;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_check1                            TO chk_quarantine_discarded_not_written;
ALTER TABLE meta.quarantine_records RENAME CONSTRAINT quarantine_records_check2                            TO chk_quarantine_resolved_after_quarantined;
