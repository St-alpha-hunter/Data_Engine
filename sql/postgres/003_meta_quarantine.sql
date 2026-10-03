-- =========================================================
-- meta.quarantine_records：被阻断、不允许直接进 golden 的记录（每条被隔离的数据一行）
-- 触发了哪些问题：用 (batch_id, ticker, date) 去 meta.quality_issues 查
-- 放行后写入 golden 采用 outbox 模式：
--   1) 放行只改本表 status='released'
--   2) 定时任务扫描 status='released' AND golden_written_at IS NULL 的行，写入 ClickHouse golden
--      （quality_status 标 warning），成功后回填 golden_written_at；失败下次重试
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.quarantine_records (
    quarantine_id      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id           text        NOT NULL REFERENCES meta.ingestion (batch_id),
    endpoint           text        NOT NULL,                  -- 数据类别，如 price_volume
    ticker             text        NOT NULL,
    date               date        NOT NULL,
    record             jsonb       NOT NULL,                  -- 被阻断的整行原始数据

    status             text        NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'released', 'discarded')),
    quarantined_at     timestamptz NOT NULL DEFAULT now(),
    resolved_at        timestamptz,                           -- 处理完成时间；pending 时为空
    golden_written_at  timestamptz,                           -- 写入 golden 的时间；只有 released 才可能有值

    UNIQUE (batch_id, endpoint, ticker, date),
    -- pending 没有处理时间；released / discarded 必须有
    CHECK ((status = 'pending') = (resolved_at IS NULL)),
    -- 只有放行的数据才能写进 golden
    CHECK (golden_written_at IS NULL OR status = 'released'),
    CHECK (resolved_at IS NULL OR resolved_at >= quarantined_at)
);

-- outbox 扫描：已放行但还没写入 golden 的行
CREATE INDEX IF NOT EXISTS idx_quarantine_outbox
    ON meta.quarantine_records (resolved_at)
    WHERE status = 'released' AND golden_written_at IS NULL;

-- 待处理队列
CREATE INDEX IF NOT EXISTS idx_quarantine_pending
    ON meta.quarantine_records (quarantined_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_quarantine_ticker_date ON meta.quarantine_records (ticker, date);

COMMENT ON TABLE  meta.quarantine_records                   IS '被阻断、不允许直接进 golden 的记录；每条被隔离的数据一行';
COMMENT ON COLUMN meta.quarantine_records.record            IS '被阻断的整行原始数据（jsonb）';
COMMENT ON COLUMN meta.quarantine_records.status            IS 'pending=待处理；released=放行（由定时任务写入 golden，标 warning）；discarded=丢弃';
COMMENT ON COLUMN meta.quarantine_records.golden_written_at IS 'outbox 标记：released 且为空 = 待写入 golden；写入成功后回填';
