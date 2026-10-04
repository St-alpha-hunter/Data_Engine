-- =========================================================
-- meta.load_log：每个批次写入了哪些表、写了多少行（每写一次表记一行）
-- ingestion.status 只表示"拉取"是否成功；每一步写入是否成功看本表
-- 对账：raw 行数 = ingestion.rows_fetched；golden + quarantine 行数 = raw 行数
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.load_log (
    load_id       bigint      GENERATED ALWAYS AS IDENTITY,
    batch_id      text        NOT NULL,
    layer         text        NOT NULL,                       -- raw / golden / quarantine
    target_table  text        NOT NULL,                       -- 如 raw.price_volume_daily
    rows_written  bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,                       -- success / failed
    error         text,                                       -- 失败原因；成功时为空
    loaded_at     timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_load_log
        PRIMARY KEY (load_id),
    CONSTRAINT fk_load_log_batch
        FOREIGN KEY (batch_id) REFERENCES meta.ingestion (batch_id),
    CONSTRAINT chk_load_log_layer
        CHECK (layer IN ('raw', 'golden', 'quarantine')),
    CONSTRAINT chk_load_log_status
        CHECK (status IN ('success', 'failed')),
    CONSTRAINT chk_load_log_rows_nonneg
        CHECK (rows_written >= 0),
    -- 失败必须写原因；成功不能有错误信息
    CONSTRAINT chk_load_log_error_iff_failed
        CHECK ((status = 'failed') = (error IS NOT NULL)),
    -- 表名必须带库名前缀，如 raw.xxx / golden.xxx / meta.xxx
    CONSTRAINT chk_load_log_table_qualified
        CHECK (target_table ~ '^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$')
);

CREATE INDEX IF NOT EXISTS idx_load_log_batch        ON meta.load_log (batch_id, loaded_at);
CREATE INDEX IF NOT EXISTS idx_load_log_target_table ON meta.load_log (target_table, loaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_load_log_failed       ON meta.load_log (loaded_at DESC) WHERE status = 'failed';

COMMENT ON TABLE  meta.load_log              IS '每个批次写入了哪些表、写了多少行；每写一次表记一行';
COMMENT ON COLUMN meta.load_log.layer        IS 'raw=原始层；golden=清洗后；quarantine=隔离区';
COMMENT ON COLUMN meta.load_log.target_table IS '带库名前缀的表名，如 raw.price_volume_daily、meta.quarantine_records';
COMMENT ON COLUMN meta.load_log.rows_written IS '本次实际写入行数；失败时为 0 或已写入的部分';
COMMENT ON COLUMN meta.load_log.error        IS '失败原因；status=success 时必须为空';
