-- =========================================================
-- meta.ingestion：每次 FMP 拉取记一行
-- 生命周期：拉取开始时插入 status='running'，结束时更新计数、状态和 finished_at
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.ingestion (
    batch_id         text        PRIMARY KEY,                 -- 可读批次号：{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}
    endpoint         text        NOT NULL,                    -- FMP 接口名，对应 FMP_ENDPOINTS 的 key
    params           jsonb       NOT NULL DEFAULT '{}',       -- 请求参数，如 {"from_date": "...", "to_date": "..."}

    n_ticker         integer     NOT NULL CHECK (n_ticker >= 0),          -- 请求的股票数
    n_success        integer     NOT NULL DEFAULT 0 CHECK (n_success >= 0),
    n_missing        integer     NOT NULL DEFAULT 0 CHECK (n_missing >= 0),
    n_failed         integer     NOT NULL DEFAULT 0 CHECK (n_failed  >= 0),

    failed_tickers   text[]      NOT NULL DEFAULT '{}',       -- 失败的股票
    missing_tickers  text[]      NOT NULL DEFAULT '{}',       -- 返回空数据的股票
    failure_detail   jsonb       NOT NULL DEFAULT '[]',       -- fetch_fmp_batch 返回的 failed_list 原样存
    rows_fetched     bigint      NOT NULL DEFAULT 0 CHECK (rows_fetched >= 0),

    status           text        NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'success', 'partial', 'failed')),
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,                             -- running 时为空

    git_version      text,                                    -- 运行时代码的 git commit
    report           text,                                    -- FeedErrorDeputy 生成的错误报告 JSON 文件路径

    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (n_success + n_missing + n_failed <= n_ticker)
);

-- 常用查询：某接口最近的批次；找出卡在 running / 不完整的批次
CREATE INDEX IF NOT EXISTS idx_ingestion_endpoint_started ON meta.ingestion (endpoint, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_status           ON meta.ingestion (status) WHERE status <> 'success';

COMMENT ON TABLE  meta.ingestion                IS '每次 FMP 拉取的批次记录';
COMMENT ON COLUMN meta.ingestion.batch_id       IS '可读批次号：{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}';
COMMENT ON COLUMN meta.ingestion.status         IS 'running=进行中；success=全部成功；partial=部分失败；failed=全部失败或程序中途报错。无数据(missing)不算失败';
COMMENT ON COLUMN meta.ingestion.failure_detail IS 'fetch_fmp_batch 返回的 failed_list：symbol/endpoint/error_type/status_code/attempts/error';
COMMENT ON COLUMN meta.ingestion.report         IS 'FeedErrorDeputy 生成的错误报告 JSON 文件路径';
