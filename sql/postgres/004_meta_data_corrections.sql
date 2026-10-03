-- =========================================================
-- meta.data_corrections：对问题 / 隔离数据的处理记录（只追加、不修改）
-- 同一个问题可以有多次处理记录，完整保留处理历史
-- release / discard 时，应在同一个 Postgres 事务里同步更新 quarantine_records.status
-- =========================================================
CREATE TABLE IF NOT EXISTS meta.data_corrections (
    correction_id     bigint      GENERATED ALWAYS AS IDENTITY,
    quarantine_id     bigint,                                 -- 处理的是哪条被隔离的数据
    issue_id          bigint,                                 -- 处理的是哪个问题
    action            text        NOT NULL,
    reason            text        NOT NULL,                   -- 最终判定：为什么这样处理
    decided_by        text        NOT NULL,                   -- 处理人：人名，或 'auto'（规则自动处理）
    rule_id           text,                                   -- 规则自动处理时：哪条规则
    rule_version      integer,                                -- 规则自动处理时：哪个版本
    new_batch_id      text,                                   -- refetch 时：重跑的批次
    corrected_values  jsonb,                                  -- manual_fix 时：{"before": {...}, "after": {...}}
    corrected_at      timestamptz NOT NULL DEFAULT now(),     -- 修复时间

    CONSTRAINT pk_data_corrections
        PRIMARY KEY (correction_id),
    CONSTRAINT fk_data_corrections_quarantine
        FOREIGN KEY (quarantine_id) REFERENCES meta.quarantine_records (quarantine_id),
    CONSTRAINT fk_data_corrections_issue
        FOREIGN KEY (issue_id) REFERENCES meta.quality_issues (issue_id),
    CONSTRAINT fk_data_corrections_rule
        FOREIGN KEY (rule_id, rule_version) REFERENCES meta.quality_rules (rule_id, rule_version),
    CONSTRAINT fk_data_corrections_new_batch
        FOREIGN KEY (new_batch_id) REFERENCES meta.ingestion (batch_id),

    -- 至少关联一个处理对象
    CONSTRAINT chk_data_corrections_has_target
        CHECK (quarantine_id IS NOT NULL OR issue_id IS NOT NULL),
    CONSTRAINT chk_data_corrections_action
        CHECK (action IN ('release', 'discard', 'refetch', 'manual_fix')),
    -- release / discard 是针对隔离数据的操作
    CONSTRAINT chk_data_corrections_quarantine_action
        CHECK (action NOT IN ('release', 'discard') OR quarantine_id IS NOT NULL),
    -- refetch 必须记录重跑的批次
    CONSTRAINT chk_data_corrections_refetch_batch
        CHECK (action <> 'refetch' OR new_batch_id IS NOT NULL),
    -- manual_fix 必须记录修改前后的值
    CONSTRAINT chk_data_corrections_manual_values
        CHECK (action <> 'manual_fix' OR corrected_values IS NOT NULL),
    -- 规则编号和版本要么都填，要么都不填
    CONSTRAINT chk_data_corrections_rule_pair
        CHECK ((rule_id IS NULL) = (rule_version IS NULL)),
    CONSTRAINT chk_data_corrections_reason_not_blank
        CHECK (btrim(reason) <> ''),
    CONSTRAINT chk_data_corrections_decided_by_not_blank
        CHECK (btrim(decided_by) <> '')
);

CREATE INDEX IF NOT EXISTS idx_data_corrections_quarantine ON meta.data_corrections (quarantine_id, corrected_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_corrections_issue      ON meta.data_corrections (issue_id, corrected_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_corrections_new_batch  ON meta.data_corrections (new_batch_id) WHERE new_batch_id IS NOT NULL;

-- 只追加、不修改：禁止 UPDATE / DELETE，改错了就再追加一条新的处理记录
CREATE OR REPLACE FUNCTION meta.forbid_modify_data_corrections() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'meta.data_corrections 只允许追加，不允许 %；如需更正请追加一条新记录', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_data_corrections_append_only ON meta.data_corrections;
CREATE TRIGGER trg_data_corrections_append_only
    BEFORE UPDATE OR DELETE ON meta.data_corrections
    FOR EACH ROW EXECUTE FUNCTION meta.forbid_modify_data_corrections();

COMMENT ON TABLE  meta.data_corrections                  IS '问题/隔离数据的处理记录；只追加、不修改（有触发器保护）';
COMMENT ON COLUMN meta.data_corrections.action           IS 'release=放行隔离数据；discard=丢弃隔离数据；refetch=重新拉取；manual_fix=人工修正数值';
COMMENT ON COLUMN meta.data_corrections.reason           IS '最终判定：为什么这样处理';
COMMENT ON COLUMN meta.data_corrections.decided_by       IS '处理人：人名，或 auto（规则自动处理，此时填 rule_id/rule_version）';
COMMENT ON COLUMN meta.data_corrections.new_batch_id     IS 'refetch 时重新拉取的批次号';
COMMENT ON COLUMN meta.data_corrections.corrected_values IS 'manual_fix 时修改前后的值：{"before": {...}, "after": {...}}';
