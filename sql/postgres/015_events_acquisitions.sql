-- =========================================================
-- 1) 通用明细触发器增加可选的第三个参数：允许"从空回填一次"的列
--    check_detail_event(事件类型, 生效日列名 [, 可回填列名])；不传第三个参数时行为不变
-- 2) events.acquisitions：收购明细（与 events.corporate_events 一对一）
--    "中立"的表结构：一笔交易一条事件，记在被收购方名下（总表 symbol / security_id = 被收购方）
--    收购方信息在明细里（acquirer_security_id 可回填一次）
--    读的时候用两个视图从不同视角看：v_acquisitions_as_target / v_acquisitions_as_acquirer
--    只记被收购方是上市公司（有代码）的交易
-- =========================================================

CREATE OR REPLACE FUNCTION events.check_detail_event() RETURNS trigger AS $$
DECLARE
    v_expected_type text := TG_ARGV[0];
    v_date_column   text := TG_ARGV[1];
    v_backfill_col  text := CASE WHEN TG_NARGS >= 3 THEN TG_ARGV[2] END;
    v_type          text;
    v_event_date    date;
    v_detail_date   date;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION '%.% 只允许追加，不允许删除；修改请追加新版本', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        -- 只允许把指定的列从空回填成有值，其他一律不能改
        IF v_backfill_col IS NULL
           OR (to_jsonb(NEW) - v_backfill_col) IS DISTINCT FROM (to_jsonb(OLD) - v_backfill_col) THEN
            RAISE EXCEPTION '%.% 只允许追加，不允许修改%；修改请追加新版本', TG_TABLE_SCHEMA, TG_TABLE_NAME,
                CASE WHEN v_backfill_col IS NOT NULL THEN '（只能回填 ' || v_backfill_col || '）' ELSE '' END;
        END IF;
        IF (to_jsonb(OLD) -> v_backfill_col) <> 'null'::jsonb
           AND (to_jsonb(NEW) -> v_backfill_col) IS DISTINCT FROM (to_jsonb(OLD) -> v_backfill_col) THEN
            RAISE EXCEPTION '% 已有值，只能从空回填，不能改成其他值', v_backfill_col;
        END IF;
        RETURN NEW;
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
    '明细表通用触发器，参数 (事件类型, 生效日列名 [, 可回填列名])：检查事件类型、生效日与总表一致；只追加，可选允许某一列从空回填一次';


-- =========================================================
-- events.acquisitions
-- =========================================================
CREATE TABLE IF NOT EXISTS events.acquisitions (
    event_id              bigint      NOT NULL,
    deal_key              text        NOT NULL,             -- 被收购方 CIK | 收购方 CIK
    effective_date        date        NOT NULL,             -- = 总表 event_date = coalesce(completion_date, filing_date)
    filing_date           date        NOT NULL,             -- 这笔交易最早的 SEC 文件日期（FMP transactionDate 最小值）
    completion_date       date,                             -- 完成日（被收购方最后交易日）；未知为空
    target_symbol         text        NOT NULL,
    target_name           text,
    target_cik            text,
    acquirer_symbol       text,
    acquirer_name         text,
    acquirer_cik          text,
    acquirer_security_id  bigint,                           -- 收购方是上市公司时回填（只能回填一次）
    filing_url            text,                             -- 最早那份文件的链接，人工核对用
    accepted_at           timestamptz,                      -- 最早那份文件的 SEC 接收时间

    CONSTRAINT pk_acquisitions
        PRIMARY KEY (event_id),
    CONSTRAINT fk_acquisitions_event
        FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id),
    CONSTRAINT fk_acquisitions_acquirer_security
        FOREIGN KEY (acquirer_security_id) REFERENCES ref.securities (security_id),
    -- 公告阶段生效日 = 文件日；完成后 = 完成日
    CONSTRAINT chk_acquisitions_effective_date
        CHECK (effective_date = coalesce(completion_date, filing_date)),
    CONSTRAINT chk_acquisitions_deal_key_not_blank
        CHECK (btrim(deal_key) <> ''),
    CONSTRAINT chk_acquisitions_target_symbol_format
        CHECK (target_symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_acquisitions_acquirer_symbol_format
        CHECK (acquirer_symbol IS NULL OR acquirer_symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_acquisitions_target_cik_format
        CHECK (target_cik IS NULL OR target_cik ~ '^[0-9]{10}$'),
    CONSTRAINT chk_acquisitions_acquirer_cik_format
        CHECK (acquirer_cik IS NULL OR acquirer_cik ~ '^[0-9]{10}$')
);

CREATE INDEX IF NOT EXISTS idx_acquisitions_deal_key         ON events.acquisitions (deal_key);
CREATE INDEX IF NOT EXISTS idx_acquisitions_acquirer_security ON events.acquisitions (acquirer_security_id) WHERE acquirer_security_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_acquisitions_target_cik       ON events.acquisitions (target_cik);
CREATE INDEX IF NOT EXISTS idx_acquisitions_acquirer_cik     ON events.acquisitions (acquirer_cik);

DROP TRIGGER IF EXISTS trg_acquisitions_check ON events.acquisitions;
CREATE TRIGGER trg_acquisitions_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.acquisitions
    FOR EACH ROW EXECUTE FUNCTION events.check_detail_event('acquisition', 'effective_date', 'acquirer_security_id');


-- =========================================================
-- 写入：record_acquisition
--   event_date = coalesce(完成日, 文件日)；状态不传时：有完成日 → confirmed，否则 announced
--   source_event_key 默认用 deal_key（同一笔交易的公告版、完成版是同一个事件的不同版本）
-- =========================================================
CREATE OR REPLACE FUNCTION events.record_acquisition(
    p_target_symbol        text,
    p_filing_date          date,
    p_deal_key             text,
    p_source               text,
    p_batch_id             text,
    p_payload              jsonb,
    p_completion_date      date        DEFAULT NULL,
    p_target_name          text        DEFAULT NULL,
    p_target_cik           text        DEFAULT NULL,
    p_acquirer_symbol      text        DEFAULT NULL,
    p_acquirer_name        text        DEFAULT NULL,
    p_acquirer_cik         text        DEFAULT NULL,
    p_acquirer_security_id bigint      DEFAULT NULL,
    p_filing_url           text        DEFAULT NULL,
    p_accepted_at          timestamptz DEFAULT NULL,
    p_status               text        DEFAULT NULL,
    p_source_event_key     text        DEFAULT NULL,
    p_announced_at         timestamptz DEFAULT NULL,
    p_known_at             timestamptz DEFAULT now(),
    p_security_id          bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    r      record;
    v_date date := coalesce(p_completion_date, p_filing_date);
BEGIN
    SELECT * INTO r FROM events.record_event(
        'acquisition', p_target_symbol, v_date,
        coalesce(p_status, CASE WHEN p_completion_date IS NULL THEN 'announced' ELSE 'confirmed' END),
        p_source, coalesce(p_source_event_key, p_deal_key), p_batch_id, p_payload,
        p_announced_at, p_known_at, p_security_id);

    IF r.action <> 'duplicate' THEN
        INSERT INTO events.acquisitions (event_id, deal_key, effective_date, filing_date, completion_date,
                                         target_symbol, target_name, target_cik,
                                         acquirer_symbol, acquirer_name, acquirer_cik, acquirer_security_id,
                                         filing_url, accepted_at)
        VALUES (r.event_id, p_deal_key, v_date, p_filing_date, p_completion_date,
                p_target_symbol, p_target_name, p_target_cik,
                p_acquirer_symbol, p_acquirer_name, p_acquirer_cik, p_acquirer_security_id,
                p_filing_url, p_accepted_at);
    END IF;

    RETURN QUERY SELECT r.event_id, r.revision, r.action;
END;
$$ LANGUAGE plpgsql;


-- =========================================================
-- 视图：同一份中立数据，两个视角
-- =========================================================
CREATE OR REPLACE VIEW events.v_acquisitions_current AS
SELECT e.event_id, e.status, e.revision, e.source, e.known_at,
       e.security_id AS target_security_id,
       a.deal_key, a.effective_date, a.filing_date, a.completion_date,
       a.target_symbol, a.target_name, a.target_cik,
       a.acquirer_symbol, a.acquirer_name, a.acquirer_cik, a.acquirer_security_id,
       a.filing_url, a.accepted_at
FROM events.v_corporate_events_current e
JOIN events.acquisitions a USING (event_id)
WHERE e.event_type = 'acquisition';

-- 被收购方视角："我被谁收购"
CREATE OR REPLACE VIEW events.v_acquisitions_as_target AS
SELECT target_security_id AS security_id, target_symbol AS symbol, target_name AS name,
       'target'::text AS role,
       acquirer_security_id AS counterparty_security_id, acquirer_symbol AS counterparty_symbol,
       acquirer_name AS counterparty_name,
       status, filing_date, completion_date, effective_date, deal_key, event_id, filing_url
FROM events.v_acquisitions_current;

-- 收购方视角："我收购了谁"（收购方是上市公司、已回填 acquirer_security_id 或至少有代码的）
CREATE OR REPLACE VIEW events.v_acquisitions_as_acquirer AS
SELECT acquirer_security_id AS security_id, acquirer_symbol AS symbol, acquirer_name AS name,
       'acquirer'::text AS role,
       target_security_id AS counterparty_security_id, target_symbol AS counterparty_symbol,
       target_name AS counterparty_name,
       status, filing_date, completion_date, effective_date, deal_key, event_id, filing_url
FROM events.v_acquisitions_current
WHERE acquirer_security_id IS NOT NULL OR acquirer_symbol IS NOT NULL;


COMMENT ON TABLE  events.acquisitions                      IS '收购明细（中立结构）：一笔交易一条，记在被收购方名下；两个视图分别从被收购方、收购方视角读；写入请用 events.record_acquisition()';
COMMENT ON COLUMN events.acquisitions.deal_key             IS '交易编号 = 被收购方 CIK | 收购方 CIK；FMP 同一笔交易的多行（多个代码、多份文件）先按它合并';
COMMENT ON COLUMN events.acquisitions.effective_date       IS '= 总表 event_date = coalesce(completion_date, filing_date)';
COMMENT ON COLUMN events.acquisitions.filing_date          IS '这笔交易最早的 SEC 文件日期（FMP transactionDate 的最小值），大致是市场开始知道这笔交易的时间';
COMMENT ON COLUMN events.acquisitions.completion_date      IS '完成日：被收购方最后一个交易日；未知为空';
COMMENT ON COLUMN events.acquisitions.acquirer_security_id IS '收购方在证券主表里的编号；收购方是上市公司时回填，只能从空回填一次';
COMMENT ON FUNCTION events.record_acquisition(text, date, text, text, text, jsonb, date, text, text, text, text, text, bigint, text, timestamptz, text, text, timestamptz, timestamptz, bigint)
    IS '写入收购：总表（symbol=被收购方，去重、版本）+ 明细，同一事务；状态不传时按是否有完成日推导';
COMMENT ON VIEW events.v_acquisitions_current     IS '每笔收购的当前版本（中立视角）';
COMMENT ON VIEW events.v_acquisitions_as_target   IS '被收购方视角：security_id = 被收购方，counterparty = 收购方';
COMMENT ON VIEW events.v_acquisitions_as_acquirer IS '收购方视角：security_id = 收购方，counterparty = 被收购方';
