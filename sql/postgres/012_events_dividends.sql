-- =========================================================
-- events.dividends：分红明细（与 events.corporate_events 一对一，按 event_id 关联）
--   只存分红特有的字段；事件的版本、来源、原始内容都在总表
--   和总表一样只追加：明细不能改、不能删，修改 = 总表追加新版本 + 新明细
--   写入一律通过 events.record_dividend()：总表 + 明细在同一个事务里
-- 日期之间的先后关系（公告日 ≤ 除息日 ≤ 派息日等）不在这里硬性约束，
-- 数据源本身有缺失和异常，留给 C 步骤的质量规则检查
-- =========================================================
CREATE TABLE IF NOT EXISTS events.dividends (
    event_id          bigint      NOT NULL,
    ex_date           date        NOT NULL,                 -- 除息日，等于总表的 event_date
    record_date       date,                                 -- 登记日
    payment_date      date,                                 -- 派息日
    declaration_date  date,                                 -- 公告日（FMP 约 40% 缺失）
    amount            numeric     NOT NULL,                 -- 每股分红（原始金额）
    adj_amount        numeric,                              -- 按拆股调整后的每股分红
    frequency         text,                                 -- 派息频率
    dividend_type     text        NOT NULL,                 -- regular / special
    currency          text        NOT NULL DEFAULT 'USD',

    CONSTRAINT pk_dividends
        PRIMARY KEY (event_id),
    CONSTRAINT fk_dividends_event
        FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id),
    CONSTRAINT chk_dividends_amount_positive
        CHECK (amount > 0),
    CONSTRAINT chk_dividends_adj_amount_positive
        CHECK (adj_amount IS NULL OR adj_amount > 0),
    CONSTRAINT chk_dividends_frequency
        CHECK (frequency IS NULL OR frequency IN
               ('Weekly', 'Bi-Weekly', 'Monthly', 'Quarterly', 'Semi-Annual', 'Annual', 'Irregular', 'Special')),
    CONSTRAINT chk_dividends_type
        CHECK (dividend_type IN ('regular', 'special')),
    CONSTRAINT chk_dividends_currency
        CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE INDEX IF NOT EXISTS idx_dividends_ex_date ON events.dividends (ex_date);


-- =========================================================
-- 跨表检查：对应的事件必须是 dividend 类型，且除息日 = 总表 event_date
-- 只追加：禁止修改、删除
-- =========================================================
CREATE OR REPLACE FUNCTION events.check_dividend_event() RETURNS trigger AS $$
DECLARE
    v_type text;
    v_date date;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'events.dividends 只允许追加，不允许%；修改请追加新版本', CASE TG_OP WHEN 'UPDATE' THEN '修改' ELSE '删除' END;
    END IF;
    SELECT event_type, event_date INTO v_type, v_date FROM events.corporate_events WHERE event_id = NEW.event_id;
    IF NOT FOUND THEN
        RETURN NEW;                                  -- 交给外键报错
    END IF;
    IF v_type <> 'dividend' THEN
        RAISE EXCEPTION 'event_id % 的类型是 %，不是 dividend', NEW.event_id, v_type;
    END IF;
    IF v_date <> NEW.ex_date THEN
        RAISE EXCEPTION '除息日 % 和总表 event_date % 不一致', NEW.ex_date, v_date;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividends_check ON events.dividends;
CREATE TRIGGER trg_dividends_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.dividends
    FOR EACH ROW EXECUTE FUNCTION events.check_dividend_event();


-- =========================================================
-- 写入：record_dividend = record_event + 写明细，同一个事务
--   duplicate：总表没写新版本，明细也不写（已存在）
--   inserted / revised：总表新版本 + 新明细
-- =========================================================
CREATE OR REPLACE FUNCTION events.record_dividend(
    p_symbol           text,
    p_ex_date          date,
    p_amount           numeric,
    p_source           text,
    p_source_event_key text,
    p_batch_id         text,
    p_payload          jsonb,
    p_status           text        DEFAULT 'confirmed',
    p_dividend_type    text        DEFAULT 'regular',
    p_adj_amount       numeric     DEFAULT NULL,
    p_record_date      date        DEFAULT NULL,
    p_payment_date     date        DEFAULT NULL,
    p_declaration_date date        DEFAULT NULL,
    p_frequency        text        DEFAULT NULL,
    p_currency         text        DEFAULT 'USD',
    p_announced_at     timestamptz DEFAULT NULL,
    p_known_at         timestamptz DEFAULT now(),
    p_security_id      bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM events.record_event(
        'dividend', p_symbol, p_ex_date, p_status, p_source, p_source_event_key, p_batch_id, p_payload,
        p_announced_at, p_known_at, p_security_id);

    IF r.action <> 'duplicate' THEN
        INSERT INTO events.dividends (event_id, ex_date, record_date, payment_date, declaration_date,
                                      amount, adj_amount, frequency, dividend_type, currency)
        VALUES (r.event_id, p_ex_date, p_record_date, p_payment_date, p_declaration_date,
                p_amount, p_adj_amount, p_frequency, p_dividend_type, p_currency);
    END IF;

    RETURN QUERY SELECT r.event_id, r.revision, r.action;
END;
$$ LANGUAGE plpgsql;


-- =========================================================
-- 当前版本：总表当前版本 + 分红明细
-- =========================================================
CREATE OR REPLACE VIEW events.v_dividends_current AS
SELECT e.event_id, e.symbol, e.security_id, e.status, e.revision, e.source, e.source_event_key, e.known_at,
       d.ex_date, d.record_date, d.payment_date, d.declaration_date,
       d.amount, d.adj_amount, d.frequency, d.dividend_type, d.currency
FROM events.v_corporate_events_current e
JOIN events.dividends d USING (event_id)
WHERE e.event_type = 'dividend';


COMMENT ON TABLE  events.dividends                  IS '分红明细，与 events.corporate_events 一对一；只追加；写入请用 events.record_dividend()';
COMMENT ON COLUMN events.dividends.ex_date          IS '除息日，等于总表 event_date（触发器检查）';
COMMENT ON COLUMN events.dividends.declaration_date IS '公告日；FMP 约 40% 缺失';
COMMENT ON COLUMN events.dividends.amount           IS '每股分红（原始金额）';
COMMENT ON COLUMN events.dividends.adj_amount       IS '按拆股调整后的每股分红（FMP adjDividend）';
COMMENT ON COLUMN events.dividends.frequency        IS 'Weekly / Bi-Weekly / Monthly / Quarterly / Semi-Annual / Annual / Irregular / Special';
COMMENT ON COLUMN events.dividends.dividend_type    IS 'regular=定期；special=特别股息（frequency 为 Special 或 Irregular）。股票股利记在拆股表';
COMMENT ON COLUMN events.dividends.currency         IS '币种；FMP 不提供，默认 USD';
COMMENT ON FUNCTION events.record_dividend(text, date, numeric, text, text, text, jsonb, text, text, numeric, date, date, date, text, text, timestamptz, timestamptz, bigint)
    IS '写入分红：总表（去重、版本）+ 明细，同一事务；返回 (event_id, revision, action)';
COMMENT ON VIEW   events.v_dividends_current         IS '每个分红事件的当前版本（总表 + 明细）';
