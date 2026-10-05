-- =========================================================
-- 证券主表
--   ref.securities             一只证券一行：自己分配的永久编号 security_id + 不随时间变化（或只需当前值）的属性
--   ref.security_identifiers   代码历史：symbol / cik / cusip / isin 各自的有效期
--   ref.v_securities_current   当前视图（"大宽表"）：每只证券一行，代码都是当前值
--   ref.resolve_security()     某个代码在某一天指向哪只证券
--   ref.securities_asof()      某一天每只证券的代码（回测用）
-- 有效期约定：[valid_from, valid_to)，包含开始日、不包含结束日；valid_to 为空表示现在仍有效
-- =========================================================
CREATE EXTENSION IF NOT EXISTS btree_gist;     -- 排除约束需要：让 = 和 && 能放进同一个 GiST 索引

-- 通用：更新时自动刷新 updated_at
CREATE OR REPLACE FUNCTION ref.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- =========================================================
-- ref.securities
-- =========================================================
CREATE TABLE IF NOT EXISTS ref.securities (
    security_id       bigint      GENERATED ALWAYS AS IDENTITY,
    name              text        NOT NULL,                 -- 当前（已退市则为最后使用的）名称
    security_type     text        NOT NULL,
    share_class       text,                                 -- A / B / C
    country           text,                                 -- 注册国家，ISO 两位代码
    primary_exchange  text,                                 -- 当前主要上市交易所，MIC 代码
    first_trade_date  date,
    last_trade_date   date,                                 -- 仍在交易为空
    status            text        NOT NULL DEFAULT 'active',
    delisting_reason  text,
    review_status     text        NOT NULL DEFAULT 'needs_review',
    notes             text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_securities
        PRIMARY KEY (security_id),
    CONSTRAINT chk_securities_name_not_blank
        CHECK (btrim(name) <> ''),
    CONSTRAINT chk_securities_type
        CHECK (security_type IN ('common_stock', 'adr', 'etf', 'preferred', 'other')),
    CONSTRAINT chk_securities_status
        CHECK (status IN ('active', 'delisted')),
    -- 已退市必须有最后交易日；交易中不能有
    CONSTRAINT chk_securities_delisted_iff_last_date
        CHECK ((status = 'delisted') = (last_trade_date IS NOT NULL)),
    CONSTRAINT chk_securities_trade_dates
        CHECK (first_trade_date IS NULL OR last_trade_date IS NULL OR first_trade_date <= last_trade_date),
    -- 退市原因只对已退市的证券有意义
    CONSTRAINT chk_securities_delisting_reason
        CHECK (delisting_reason IS NULL OR (status = 'delisted' AND delisting_reason IN ('acquired', 'bankruptcy', 'other'))),
    CONSTRAINT chk_securities_review_status
        CHECK (review_status IN ('verified', 'auto', 'needs_review')),
    CONSTRAINT chk_securities_exchange_mic
        CHECK (primary_exchange IS NULL OR primary_exchange ~ '^X[A-Z]{3}$'),
    CONSTRAINT chk_securities_country
        CHECK (country IS NULL OR country ~ '^[A-Z]{2}$')
);

CREATE INDEX IF NOT EXISTS idx_securities_review
    ON ref.securities (review_status) WHERE review_status = 'needs_review';

DROP TRIGGER IF EXISTS trg_securities_updated_at ON ref.securities;
CREATE TRIGGER trg_securities_updated_at
    BEFORE UPDATE ON ref.securities
    FOR EACH ROW EXECUTE FUNCTION ref.set_updated_at();

COMMENT ON TABLE  ref.securities                  IS '证券主表：一只证券一行；security_id 自己分配，永久不变、不复用';
COMMENT ON COLUMN ref.securities.security_id      IS '内部永久编号；外部代码（symbol/cik/cusip/isin）记在 ref.security_identifiers';
COMMENT ON COLUMN ref.securities.name             IS '当前名称；已退市的为最后使用的名称';
COMMENT ON COLUMN ref.securities.security_type    IS 'common_stock / adr / etf / preferred / other';
COMMENT ON COLUMN ref.securities.primary_exchange IS '当前主要上市交易所（MIC），历史变化以后放 security_attributes';
COMMENT ON COLUMN ref.securities.last_trade_date  IS '最后一个交易日；仍在交易为空';
COMMENT ON COLUMN ref.securities.review_status    IS 'verified=人工确认；auto=自动匹配且可信；needs_review=待人工核对';


-- =========================================================
-- ref.security_identifiers
-- =========================================================
CREATE TABLE IF NOT EXISTS ref.security_identifiers (
    identifier_id  bigint      GENERATED ALWAYS AS IDENTITY,
    security_id    bigint      NOT NULL,
    id_type        text        NOT NULL,
    id_value       text        NOT NULL,
    valid_from     date        NOT NULL,
    valid_to       date,                                    -- 不含当天；为空表示仍有效
    source         text        NOT NULL,
    event_id       bigint,                                  -- 由哪个事件引起；事件总表建好后再加外键
    created_at     timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_security_identifiers
        PRIMARY KEY (identifier_id),
    CONSTRAINT fk_security_identifiers_security
        FOREIGN KEY (security_id) REFERENCES ref.securities (security_id),
    CONSTRAINT chk_security_identifiers_type
        CHECK (id_type IN ('symbol', 'cik', 'cusip', 'isin')),
    CONSTRAINT chk_security_identifiers_range
        CHECK (valid_to IS NULL OR valid_to > valid_from),
    -- 格式
    CONSTRAINT chk_security_identifiers_symbol_format
        CHECK (id_type <> 'symbol' OR id_value ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_security_identifiers_cik_format
        CHECK (id_type <> 'cik' OR id_value ~ '^[0-9]{10}$'),
    CONSTRAINT chk_security_identifiers_cusip_format
        CHECK (id_type <> 'cusip' OR id_value ~ '^[0-9A-Z]{9}$'),
    CONSTRAINT chk_security_identifiers_isin_format
        CHECK (id_type <> 'isin' OR id_value ~ '^[A-Z]{2}[0-9A-Z]{9}[0-9]$'),
    -- 同一个代码，同一时间只能指向一只证券（WB：2009 年前美联银行、2014 年后微博，不重叠所以能共存）
    CONSTRAINT exc_security_identifiers_value_one_security
        EXCLUDE USING gist (id_type WITH =, id_value WITH =, daterange(valid_from, valid_to, '[)') WITH &&),
    -- 同一只证券、同一种代码，同一时间只能有一个值
    CONSTRAINT exc_security_identifiers_one_value_per_type
        EXCLUDE USING gist (security_id WITH =, id_type WITH =, daterange(valid_from, valid_to, '[)') WITH &&)
);

CREATE INDEX IF NOT EXISTS idx_security_identifiers_security ON ref.security_identifiers (security_id, id_type);

COMMENT ON TABLE  ref.security_identifiers            IS '证券代码历史：每个代码的一段有效期一行；有效期 [valid_from, valid_to)';
COMMENT ON COLUMN ref.security_identifiers.id_type    IS 'symbol / cik / cusip / isin';
COMMENT ON COLUMN ref.security_identifiers.valid_to   IS '不含当天；为空表示仍有效。FB [2012-05-18, 2022-06-09) 与 META [2022-06-09, ) 首尾相接';
COMMENT ON COLUMN ref.security_identifiers.source     IS 'fmp_profile / fmp_symbol_change / sec / sp500_github / manual';


-- =========================================================
-- 查询接口
-- =========================================================
-- 某个代码在某一天指向哪只证券（没有则返回 NULL；排除约束保证最多一只）
CREATE OR REPLACE FUNCTION ref.resolve_security(p_id_type text, p_id_value text, p_date date)
RETURNS bigint AS $$
    SELECT security_id
    FROM ref.security_identifiers
    WHERE id_type = p_id_type
      AND id_value = p_id_value
      AND daterange(valid_from, valid_to, '[)') @> p_date
$$ LANGUAGE sql STABLE;

-- 某一天每只证券的代码（回测用：用当天的代码，不用今天的）
CREATE OR REPLACE FUNCTION ref.securities_asof(p_date date)
RETURNS TABLE (security_id bigint, symbol text, cik text, cusip text, isin text) AS $$
    SELECT i.security_id,
           max(i.id_value) FILTER (WHERE i.id_type = 'symbol'),
           max(i.id_value) FILTER (WHERE i.id_type = 'cik'),
           max(i.id_value) FILTER (WHERE i.id_type = 'cusip'),
           max(i.id_value) FILTER (WHERE i.id_type = 'isin')
    FROM ref.security_identifiers i
    WHERE daterange(i.valid_from, i.valid_to, '[)') @> p_date
    GROUP BY i.security_id
$$ LANGUAGE sql STABLE;

-- 当前视图："大宽表"，每只证券一行，代码取当前有效（valid_to 为空）的值
CREATE OR REPLACE VIEW ref.v_securities_current AS
SELECT s.security_id,
       cur.symbol, cur.cik, cur.cusip, cur.isin,
       s.name, s.security_type, s.share_class, s.country, s.primary_exchange,
       s.first_trade_date, s.last_trade_date, s.status, s.delisting_reason, s.review_status
FROM ref.securities s
LEFT JOIN LATERAL (
    SELECT max(id_value) FILTER (WHERE id_type = 'symbol') AS symbol,
           max(id_value) FILTER (WHERE id_type = 'cik')    AS cik,
           max(id_value) FILTER (WHERE id_type = 'cusip')  AS cusip,
           max(id_value) FILTER (WHERE id_type = 'isin')   AS isin
    FROM ref.security_identifiers i
    WHERE i.security_id = s.security_id AND i.valid_to IS NULL
) cur ON true;

COMMENT ON FUNCTION ref.resolve_security(text, text, date) IS '某个代码在某一天指向哪只证券；如 resolve_security(''symbol'', ''WB'', ''2008-06-30'')';
COMMENT ON FUNCTION ref.securities_asof(date)              IS '某一天每只证券的 symbol/cik/cusip/isin（回测用）';
COMMENT ON VIEW     ref.v_securities_current               IS '证券当前视图（大宽表）：每只证券一行，代码为当前值';
