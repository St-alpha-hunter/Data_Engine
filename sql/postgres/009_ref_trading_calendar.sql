-- =========================================================
-- ref schema：由数据或规则推导出来的"状态"表（交易日历、证券主表、成分股区间……）
-- ref.trading_calendar：交易所日历，每个自然日一行
--   主数据源：exchange_calendars（Python 库，含提前收盘、临时休市的完整规则）
--   交叉核对：FMP holidays-by-exchange，差异记入 meta.quality_issues，不自动覆盖
-- =========================================================
CREATE SCHEMA IF NOT EXISTS ref;

CREATE TABLE IF NOT EXISTS ref.trading_calendar (
    exchange          text        NOT NULL,                   -- 交易所 MIC 代码：XNYS / XNAS
    date              date        NOT NULL,                   -- 自然日
    is_trading_day    boolean     NOT NULL,
    session_type      text        NOT NULL,                   -- full / early_close / closed
    open_time         time,                                   -- 开盘时间（交易所当地时间，美东）；非交易日为空
    close_time        time,                                   -- 收盘时间；提前收盘日如 13:00
    holiday_name      text,                                   -- 节假日 / 临时休市名称；普通周末为空
    trading_day_seq   integer,                                -- 第几个交易日（从 1 开始）；非交易日为空
    prev_trading_day  date,                                   -- 严格早于当天的最近一个交易日
    next_trading_day  date,                                   -- 严格晚于当天的最近一个交易日
    source            text        NOT NULL,                   -- 数据来源，如 exchange_calendars
    source_version    text,                                   -- 来源版本，如 4.13.2
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_trading_calendar
        PRIMARY KEY (exchange, date),
    CONSTRAINT chk_trading_calendar_exchange_mic
        CHECK (exchange ~ '^X[A-Z]{3}$'),
    CONSTRAINT chk_trading_calendar_session_type
        CHECK (session_type IN ('full', 'early_close', 'closed')),
    -- 是否交易与 session_type 一致
    CONSTRAINT chk_trading_calendar_trading_iff_session
        CHECK (is_trading_day = (session_type <> 'closed')),
    -- 交易日必须有开收盘时间且开盘早于收盘；非交易日不能有
    CONSTRAINT chk_trading_calendar_times
        CHECK (CASE WHEN is_trading_day
                    THEN open_time IS NOT NULL AND close_time IS NOT NULL AND open_time < close_time
                    ELSE open_time IS NULL AND close_time IS NULL END),
    -- 交易日才有序号
    CONSTRAINT chk_trading_calendar_seq_iff_trading
        CHECK (is_trading_day = (trading_day_seq IS NOT NULL)),
    CONSTRAINT chk_trading_calendar_prev_before
        CHECK (prev_trading_day IS NULL OR prev_trading_day < date),
    CONSTRAINT chk_trading_calendar_next_after
        CHECK (next_trading_day IS NULL OR next_trading_day > date)
);

-- 同一交易所交易日序号唯一（用于"N 个交易日后"的计算）
CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_calendar_seq
    ON ref.trading_calendar (exchange, trading_day_seq) WHERE trading_day_seq IS NOT NULL;

-- 常用查询：某段时间的交易日
CREATE INDEX IF NOT EXISTS idx_trading_calendar_sessions
    ON ref.trading_calendar (exchange, date) WHERE is_trading_day;

COMMENT ON TABLE  ref.trading_calendar                  IS '交易所日历，每个自然日一行；主数据源 exchange_calendars，FMP 交叉核对';
COMMENT ON COLUMN ref.trading_calendar.exchange         IS '交易所 MIC 代码：XNYS=纽交所，XNAS=纳斯达克';
COMMENT ON COLUMN ref.trading_calendar.session_type     IS 'full=全天交易；early_close=提前收盘（半天）；closed=休市（含周末）';
COMMENT ON COLUMN ref.trading_calendar.open_time        IS '开盘时间，交易所当地时间（美东）';
COMMENT ON COLUMN ref.trading_calendar.close_time       IS '收盘时间，交易所当地时间（美东）；提前收盘日如 13:00';
COMMENT ON COLUMN ref.trading_calendar.trading_day_seq  IS '交易日序号，从 1 开始；非交易日为空。N 个交易日后 = 序号 + N';
COMMENT ON COLUMN ref.trading_calendar.prev_trading_day IS '严格早于当天的最近一个交易日';
COMMENT ON COLUMN ref.trading_calendar.next_trading_day IS '严格晚于当天的最近一个交易日；收盘后公布的财报从这一天生效';
