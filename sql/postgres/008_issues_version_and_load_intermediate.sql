-- =========================================================
-- 1) quality_issues 去重键加上 rule_version
--    原来 (batch_id, rule_id, symbol, date)：同一批数据先用 v1 清洗、规则升到 v2 后重新清洗，
--    v2 的问题会被当成重复跳过，表里永远是 v1 的结果
-- 2) load_log 支持记录中间态 parquet
--    layer 增加 intermediate；target_table 写逻辑名 intermediate.xxx；新增 location 存文件实际地址
-- =========================================================

-- ---------- 1) quality_issues ----------
DROP INDEX IF EXISTS meta.uq_quality_issues_dedup;
CREATE UNIQUE INDEX uq_quality_issues_dedup
    ON meta.quality_issues (batch_id, rule_id, rule_version, symbol, date) NULLS NOT DISTINCT;

-- ---------- 2) load_log ----------
ALTER TABLE meta.load_log DROP CONSTRAINT chk_load_log_layer;
ALTER TABLE meta.load_log ADD CONSTRAINT chk_load_log_layer
    CHECK (layer IN ('raw', 'intermediate', 'quarantine', 'golden'));

ALTER TABLE meta.load_log ADD COLUMN location text;
-- intermediate 必须记录文件地址；其他层写的是数据库表，不需要
ALTER TABLE meta.load_log ADD CONSTRAINT chk_load_log_intermediate_location
    CHECK (layer <> 'intermediate' OR location IS NOT NULL);

COMMENT ON COLUMN meta.load_log.layer        IS 'raw=原始层；intermediate=清洗后的中间态 parquet；quarantine=隔离区；golden=清洗后';
COMMENT ON COLUMN meta.load_log.target_table IS '带库名前缀的表名，如 raw.price_volume_daily；intermediate 层写逻辑名，如 intermediate.price_volume_daily';
COMMENT ON COLUMN meta.load_log.location     IS 'intermediate 层：parquet 文件的实际地址（本地路径或 s3://...）';
