-- =========================================================
-- meta.quality_rules：校验规则清单
-- 规则阈值或逻辑变化时，新增一行 rule_version+1，旧版本保留（is_active=false），便于审计
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.quality_rules (
    rule_id       text        NOT NULL,                       -- 规则编号，如 PV001
    rule_version  integer     NOT NULL DEFAULT 1 CHECK (rule_version >= 1),
    endpoint      text        NOT NULL,                       -- 适用的 FMP 接口
    description   text        NOT NULL,
    severity      text        NOT NULL CHECK (severity IN ('warning', 'danger')),
    params        jsonb       NOT NULL DEFAULT '{}',          -- 阈值等参数，如 {"max_range": 0.15}
    impl          text,                                       -- 代码里对应的方法
    is_active     boolean     NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, rule_version)
);

-- 每个规则同一时间只能有一个生效版本
CREATE UNIQUE INDEX IF NOT EXISTS uq_quality_rules_active ON meta.quality_rules (rule_id) WHERE is_active;

COMMENT ON TABLE meta.quality_rules IS '数据质量校验规则清单；改阈值/逻辑时新增版本，不改旧行';

INSERT INTO meta.quality_rules (rule_id, rule_version, endpoint, description, severity, params, impl) VALUES
    ('PV001', 1, 'price_volume', '价格 <= 0 或成交量 < 0',                         'danger',  '{}',                      'PriceVolume._check_positive_price'),
    ('PV002', 1, 'price_volume', 'OHLC 逻辑矛盾：开/收盘价不在最高最低价之间，或最高价 < 最低价', 'danger',  '{}',                      'PriceVolume._check_logic_relation'),
    ('PV003', 1, 'price_volume', '单日振幅过大：OHLC 最大值 > 最小值 × (1 + max_range)',  'warning', '{"max_range": 0.15}',     'PriceVolume._check_daily_volatility'),
    ('PV004', 1, 'price_volume', '收益率跳变：相邻两日收益率之差的绝对值 > max_jump',       'warning', '{"max_jump": 0.15}',      'PriceVolume._check_jump_soar')
ON CONFLICT (rule_id, rule_version) DO NOTHING;


-- =========================================================
-- meta.quality_issues：校验发现的问题，每条有问题的记录（一只股票的一天）一行
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.quality_issues (
    issue_id      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id      text        NOT NULL REFERENCES meta.ingestion (batch_id),
    rule_id       text        NOT NULL,
    rule_version  integer     NOT NULL,
    severity      text        NOT NULL CHECK (severity IN ('warning', 'danger')),
    ticker        text        NOT NULL,
    date          date,                                       -- 股票层面的问题可为空
    detail        jsonb       NOT NULL DEFAULT '{}',          -- 出问题的具体数值，如 {"adjLow": 0}
    detected_at   timestamptz NOT NULL DEFAULT now(),

    FOREIGN KEY (rule_id, rule_version) REFERENCES meta.quality_rules (rule_id, rule_version)
);

-- 同一批数据重复校验时不重复记录（date 为空也视为相同）
CREATE UNIQUE INDEX IF NOT EXISTS uq_quality_issues_dedup
    ON meta.quality_issues (batch_id, rule_id, ticker, date) NULLS NOT DISTINCT;

CREATE INDEX IF NOT EXISTS idx_quality_issues_ticker_date ON meta.quality_issues (ticker, date);
CREATE INDEX IF NOT EXISTS idx_quality_issues_rule        ON meta.quality_issues (rule_id, detected_at DESC);

COMMENT ON TABLE  meta.quality_issues          IS '校验发现的问题；每条有问题的记录（一只股票的一天）一行';
COMMENT ON COLUMN meta.quality_issues.date     IS '出问题的交易日；股票层面的问题为空';
COMMENT ON COLUMN meta.quality_issues.severity IS '冗余自 quality_rules，方便直接筛选 danger';
COMMENT ON COLUMN meta.quality_issues.detail   IS '出问题的具体数值，如 {"adjLow": 0, "adjHigh": 175.3}';
