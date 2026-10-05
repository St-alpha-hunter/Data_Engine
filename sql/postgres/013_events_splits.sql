-- =========================================================
-- 1) 通用的明细表触发器：events.check_detail_event(事件类型, 生效日列名)
--    ① 对应事件必须是指定类型 ② 生效日列必须等于总表 event_date ③ 只追加
--    5 张明细表共用；分红表改用它（替换 012 里的专用函数）
-- 2) events.splits：拆股明细（与 events.corporate_events 一对一）
-- =========================================================

CREATE OR REPLACE FUNCTION events.check_detail_event() RETURNS trigger AS $$
DECLARE
    v_expected_type text := TG_ARGV[0];
    v_date_column   text := TG_ARGV[1];
    v_type          text;
    v_event_date    date;
    v_detail_date   date;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION '%.% 只允许追加，不允许%；修改请追加新版本',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, CASE TG_OP WHEN 'UPDATE' THEN '修改' ELSE '删除' END;
    END IF;
    SELECT event_type, event_date INTO v_type, v_event_date FROM events.corporate_events WHERE event_id = NEW.event_id;
    IF NOT FOUND THEN
        RETURN NEW;                                  -- 交给外键报错
    END IF;
    IF v_type <> v_expected_type THEN
        RAISE EXCEPTION 'event_id % 的类型是 %，不是 %', NEW.event_id, v_type, v_expected_type;
    END IF;
    v_detail_date := (to_jsonb(NEW) ->> v_date_column)::date;
    IF v_detail_date IS DISTINCT FROM v_event_date THEN
        RAISE EXCEPTION '% % 和总表 event_date % 不一致', v_date_column, v_detail_date, v_event_date;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION events.check_detail_event() IS
    '明细表通用触发器，参数 (事件类型, 生效日列名)：检查事件类型、生效日与总表一致；只追加';

-- 分红表改用通用触发器
DROP TRIGGER IF EXISTS trg_dividends_check ON events.dividends;
CREATE TRIGGER trg_dividends_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.dividends
    FOR EACH ROW EXECUTE FUNCTION events.check_detail_event('dividend', 'ex_date');
DROP FUNCTION IF EXISTS events.check_dividend_event();


-- =========================================================
-- events.splits
-- =========================================================
CREATE TABLE IF NOT EXISTS events.splits (
    event_id        bigint  NOT NULL,
    effective_date  date    NOT NULL,                       -- 生效日，等于总表 event_date
    numerator       numeric NOT NULL,                       -- 拆股后股数
    denominator     numeric NOT NULL,                       -- 拆股前股数
    split_ratio     numeric GENERATED ALWAYS AS (numerator / denominator) STORED,
    split_type      text    NOT NULL,                       -- forward / reverse / stock_dividend

    CONSTRAINT pk_splits
        PRIMARY KEY (event_id),
    CONSTRAINT fk_splits_event
        FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id),
    CONSTRAINT chk_splits_positive
        CHECK (numerator > 0 AND denominator > 0),
    CONSTRAINT chk_splits_not_one_to_one
        CHECK (numerator <> denominator),
    CONSTRAINT chk_splits_type
        CHECK (split_type IN ('forward', 'reverse', 'stock_dividend')),
    -- 合股 ⇔ 分子 < 分母；拆股和股票股利都会增加股数（分子 > 分母）
    CONSTRAINT chk_splits_type_matches_ratio
        CHECK ((split_type = 'reverse') = (numerator < denominator))
);

CREATE INDEX IF NOT EXISTS idx_splits_effective_date ON events.splits (effective_date);

DROP TRIGGER IF EXISTS trg_splits_check ON events.splits;
CREATE TRIGGER trg_splits_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.splits
    FOR EACH ROW EXECUTE FUNCTION events.check_detail_event('split', 'effective_date');


-- =========================================================
-- 写入：record_split = record_event + 写明细，同一个事务
--   p_split_type 不传时按比例推导：分子 < 分母 → reverse，否则 forward（股票股利需显式传 stock_dividend）
-- =========================================================
CREATE OR REPLACE FUNCTION events.record_split(
    p_symbol           text,
    p_effective_date   date,
    p_numerator        numeric,
    p_denominator      numeric,
    p_source           text,
    p_source_event_key text,
    p_batch_id         text,
    p_payload          jsonb,
    p_split_type       text        DEFAULT NULL,
    p_status           text        DEFAULT 'confirmed',
    p_announced_at     timestamptz DEFAULT NULL,
    p_known_at         timestamptz DEFAULT now(),
    p_security_id      bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM events.record_event(
        'split', p_symbol, p_effective_date, p_status, p_source, p_source_event_key, p_batch_id, p_payload,
        p_announced_at, p_known_at, p_security_id);

    IF r.action <> 'duplicate' THEN
        INSERT INTO events.splits (event_id, effective_date, numerator, denominator, split_type)
        VALUES (r.event_id, p_effective_date, p_numerator, p_denominator,
                coalesce(p_split_type, CASE WHEN p_numerator < p_denominator THEN 'reverse' ELSE 'forward' END));
    END IF;

    RETURN QUERY SELECT r.event_id, r.revision, r.action;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW events.v_splits_current AS
SELECT e.event_id, e.symbol, e.security_id, e.status, e.revision, e.source, e.source_event_key, e.known_at,
       s.effective_date, s.numerator, s.denominator, s.split_ratio, s.split_type
FROM events.v_corporate_events_current e
JOIN events.splits s USING (event_id)
WHERE e.event_type = 'split';


COMMENT ON TABLE  events.splits                IS '拆股明细（含合股、股票股利），与 events.corporate_events 一对一；只追加；写入请用 events.record_split()';
COMMENT ON COLUMN events.splits.effective_date IS '生效日，等于总表 event_date（触发器检查）';
COMMENT ON COLUMN events.splits.numerator      IS '拆股后股数（分子），按原值保存，可以不是整数，如 GOOGL 2014 年 999:500';
COMMENT ON COLUMN events.splits.denominator    IS '拆股前股数（分母）';
COMMENT ON COLUMN events.splits.split_ratio    IS '生成列 = numerator / denominator；4 拆 1 为 4，1 合 20 为 0.05';
COMMENT ON COLUMN events.splits.split_type     IS 'forward=拆股；reverse=合股（分子<分母）；stock_dividend=股票股利';
COMMENT ON FUNCTION events.record_split(text, date, numeric, numeric, text, text, text, jsonb, text, text, timestamptz, timestamptz, bigint)
    IS '写入拆股：总表（去重、版本）+ 明细，同一事务；不传 split_type 时按比例推导';
COMMENT ON VIEW   events.v_splits_current       IS '每个拆股事件的当前版本（总表 + 明细）';
