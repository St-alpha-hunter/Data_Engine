-- =========================================================
-- events.symbol_changes：代码变更明细（与 events.corporate_events 一对一）
--   总表的 symbol 填旧代码（事件发生时证券还在用旧代码）；record_symbol_change() 自动处理
--   无效日期（如 FMP 的 1969-12-31）不在这里拦截，导入时作为质量问题处理
--   以后由本表推导证券主表的代码历史：关闭旧代码的一段，同一天开始新代码的一段
-- =========================================================
CREATE TABLE IF NOT EXISTS events.symbol_changes (
    event_id      bigint NOT NULL,
    change_date   date   NOT NULL,                          -- 变更生效日，等于总表 event_date
    old_symbol    text   NOT NULL,
    new_symbol    text   NOT NULL,
    company_name  text,                                     -- 公司名称，人工核对用

    CONSTRAINT pk_symbol_changes
        PRIMARY KEY (event_id),
    CONSTRAINT fk_symbol_changes_event
        FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id),
    CONSTRAINT chk_symbol_changes_old_format
        CHECK (old_symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_symbol_changes_new_format
        CHECK (new_symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_symbol_changes_different
        CHECK (old_symbol <> new_symbol)
);

CREATE INDEX IF NOT EXISTS idx_symbol_changes_old ON events.symbol_changes (old_symbol, change_date);
CREATE INDEX IF NOT EXISTS idx_symbol_changes_new ON events.symbol_changes (new_symbol, change_date);

DROP TRIGGER IF EXISTS trg_symbol_changes_check ON events.symbol_changes;
CREATE TRIGGER trg_symbol_changes_check
    BEFORE INSERT OR UPDATE OR DELETE ON events.symbol_changes
    FOR EACH ROW EXECUTE FUNCTION events.check_detail_event('symbol_change', 'change_date');


-- 写入：总表 symbol 自动用旧代码
CREATE OR REPLACE FUNCTION events.record_symbol_change(
    p_change_date      date,
    p_old_symbol       text,
    p_new_symbol       text,
    p_source           text,
    p_source_event_key text,
    p_batch_id         text,
    p_payload          jsonb,
    p_company_name     text        DEFAULT NULL,
    p_status           text        DEFAULT 'confirmed',
    p_announced_at     timestamptz DEFAULT NULL,
    p_known_at         timestamptz DEFAULT now(),
    p_security_id      bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM events.record_event(
        'symbol_change', p_old_symbol, p_change_date, p_status, p_source, p_source_event_key, p_batch_id, p_payload,
        p_announced_at, p_known_at, p_security_id);

    IF r.action <> 'duplicate' THEN
        INSERT INTO events.symbol_changes (event_id, change_date, old_symbol, new_symbol, company_name)
        VALUES (r.event_id, p_change_date, p_old_symbol, p_new_symbol, p_company_name);
    END IF;

    RETURN QUERY SELECT r.event_id, r.revision, r.action;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW events.v_symbol_changes_current AS
SELECT e.event_id, e.security_id, e.status, e.revision, e.source, e.source_event_key, e.known_at,
       c.change_date, c.old_symbol, c.new_symbol, c.company_name
FROM events.v_corporate_events_current e
JOIN events.symbol_changes c USING (event_id)
WHERE e.event_type = 'symbol_change';


COMMENT ON TABLE  events.symbol_changes             IS '代码变更明细，与 events.corporate_events 一对一；只追加；写入请用 events.record_symbol_change()';
COMMENT ON COLUMN events.symbol_changes.change_date IS '变更生效日，等于总表 event_date（触发器检查）；从这天起使用新代码';
COMMENT ON COLUMN events.symbol_changes.old_symbol  IS '旧代码；总表的 symbol 也填它';
COMMENT ON COLUMN events.symbol_changes.new_symbol  IS '新代码';
COMMENT ON FUNCTION events.record_symbol_change(date, text, text, text, text, text, jsonb, text, text, timestamptz, timestamptz, bigint)
    IS '写入代码变更：总表（symbol=旧代码，去重、版本）+ 明细，同一事务';
COMMENT ON VIEW   events.v_symbol_changes_current    IS '每个代码变更事件的当前版本（总表 + 明细）';
