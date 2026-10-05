-- =========================================================
-- events.sp500_index_changes：S&P 500 成分股变更明细（与 events.corporate_events 一对一）
--   按指数分表：以后纳斯达克 100 用 events.nasdaq100_index_changes（数据源、字段可能不同）
--   总表 event_type 统一是 index_change；source_event_key 以 SP500| 开头区分指数
--   数据源：GitHub fja05680/sp500 的 sp500_ticker_start_end.csv，每段区间拆成 add / remove 两个事件
--   注意：该数据集的代码有时是公司"后来的代码"，security_id 先留空，证券主表核对后再回填
--   以后由本表推导 ref.sp500_membership（每对 add / remove 组成一段区间）
-- =========================================================
CREATE TABLE IF NOT EXISTS events.sp500_index_changes (
    event_id            bigint NOT NULL,
    effective_date      date   NOT NULL,                    -- 生效日，等于总表 event_date
    index_code          text   NOT NULL DEFAULT 'SP500',
    action              text   NOT NULL,                    -- add 调入 / remove 调出
    company_name        text,                               -- 人工核对用
    counterpart_symbol  text,                               -- 同一天对应调出 / 调入的那只股票；数据集没有，先留空
    reason              text,                               -- 调整原因；数据集没有，以后可从维基百科补

    CONSTRAINT pk_sp500_index_changes
        PRIMARY KEY (event_id),
    CONSTRAINT fk_sp500_index_changes_event
        FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id),
    CONSTRAINT chk_sp500_index_changes_index_code
        CHECK (index_code = 'SP500'),
    CONSTRAINT chk_sp500_index_changes_action
        CHECK (action IN ('add', 'remove')),
    CONSTRAINT chk_sp500_index_changes_counterpart_format
        CHECK (counterpart_symbol IS NULL OR counterpart_symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$')
);

CREATE INDEX IF NOT EXISTS idx_sp500_index_changes_date ON events.sp500_index_changes (effective_date, action);

DROP TRIGGER IF EXISTS trg_sp500_index_changes_check ON events.sp500_index_changes;
CREATE TRIGGER trg_sp500_index_changes_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.sp500_index_changes
    FOR EACH ROW EXECUTE FUNCTION events.check_detail_event('index_change', 'effective_date');


-- 写入：source_event_key 默认 SP500|代码|动作|日期
CREATE OR REPLACE FUNCTION events.record_sp500_index_change(
    p_symbol             text,
    p_effective_date     date,
    p_action             text,
    p_source             text,
    p_batch_id           text,
    p_payload            jsonb,
    p_company_name       text        DEFAULT NULL,
    p_counterpart_symbol text        DEFAULT NULL,
    p_reason             text        DEFAULT NULL,
    p_status             text        DEFAULT 'confirmed',
    p_source_event_key   text        DEFAULT NULL,
    p_announced_at       timestamptz DEFAULT NULL,
    p_known_at           timestamptz DEFAULT now(),
    p_security_id        bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM events.record_event(
        'index_change', p_symbol, p_effective_date, p_status, p_source,
        coalesce(p_source_event_key, 'SP500|' || p_symbol || '|' || p_action || '|' || p_effective_date::text),
        p_batch_id, p_payload, p_announced_at, p_known_at, p_security_id);

    IF r.action <> 'duplicate' THEN
        INSERT INTO events.sp500_index_changes (event_id, effective_date, action, company_name, counterpart_symbol, reason)
        VALUES (r.event_id, p_effective_date, p_action, p_company_name, p_counterpart_symbol, p_reason);
    END IF;

    RETURN QUERY SELECT r.event_id, r.revision, r.action;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW events.v_sp500_index_changes_current AS
SELECT e.event_id, e.symbol, e.security_id, e.status, e.revision, e.source, e.source_event_key, e.known_at,
       c.index_code, c.effective_date, c.action, c.company_name, c.counterpart_symbol, c.reason
FROM events.v_corporate_events_current e
JOIN events.sp500_index_changes c USING (event_id)
WHERE e.event_type = 'index_change';


COMMENT ON TABLE  events.sp500_index_changes                    IS 'S&P 500 成分股变更明细，与 events.corporate_events 一对一；只追加；写入请用 events.record_sp500_index_change()';
COMMENT ON COLUMN events.sp500_index_changes.effective_date     IS '生效日，等于总表 event_date（触发器检查）';
COMMENT ON COLUMN events.sp500_index_changes.index_code         IS '固定为 SP500（便于以后和纳斯达克 100 的表合并成视图）';
COMMENT ON COLUMN events.sp500_index_changes.action             IS 'add=调入；remove=调出';
COMMENT ON COLUMN events.sp500_index_changes.counterpart_symbol IS '同一天对应调出 / 调入的那只股票；社区数据集没有，先留空';
COMMENT ON COLUMN events.sp500_index_changes.reason             IS '调整原因（被收购、市值不达标等）；数据集没有，以后可从维基百科补';
COMMENT ON FUNCTION events.record_sp500_index_change(text, date, text, text, text, jsonb, text, text, text, text, text, timestamptz, timestamptz, bigint)
    IS '写入 S&P 500 成分股变更：总表（event_type=index_change，key=SP500|代码|动作|日期）+ 明细，同一事务';
COMMENT ON VIEW   events.v_sp500_index_changes_current           IS '每个 S&P 500 成分股变更事件的当前版本（总表 + 明细）';
