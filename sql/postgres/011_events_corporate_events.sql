-- =========================================================
-- events schema：公司事件流（拆股、分红、代码变更、成分股变更、收购……）
-- events.corporate_events：事件总表，每个事件的每个版本一行，只追加
--   修改 = 追加新版本（revision + 1），旧版本保留
--   去重 = (source, source_event_key, payload_hash) 唯一：内容完全相同的消息（如 Kafka 重复投递）不会重复写入
--   只允许回填 security_id（空 → 有值），其他字段一律不能改、不能删
-- 写入一律通过 events.record_event()：在数据库里判断"重复投递"还是"新版本"，并分配版本号
-- =========================================================
CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.corporate_events (
    event_id          bigint      GENERATED ALWAYS AS IDENTITY,
    event_type        text        NOT NULL,
    symbol            text        NOT NULL,                 -- 数据源给出的原始代码
    security_id       bigint,                               -- 对应到证券主表后回填
    event_date        date        NOT NULL,                 -- 生效日：除息日 / 拆股生效日 / 代码变更日 / 成分股变更生效日 / 收购完成日
    announced_at      timestamptz,                          -- 公告时间，数据源不提供则为空
    known_at          timestamptz NOT NULL DEFAULT now(),   -- 我们得知的时间（按时点查询靠它）
    status            text        NOT NULL,
    revision          integer     NOT NULL,
    source            text        NOT NULL,
    source_event_key  text        NOT NULL,                 -- 数据源里的唯一标识；没有就按规则拼，如 AAPL|2020-08-31|split
    payload_hash      text        NOT NULL,                 -- 内容哈希（record_event 计算），用于去重
    batch_id          text        NOT NULL,
    payload           jsonb       NOT NULL,                 -- 原始内容

    CONSTRAINT pk_corporate_events
        PRIMARY KEY (event_id),
    CONSTRAINT fk_corporate_events_security
        FOREIGN KEY (security_id) REFERENCES ref.securities (security_id),
    CONSTRAINT fk_corporate_events_batch
        FOREIGN KEY (batch_id) REFERENCES meta.ingestion (batch_id),
    CONSTRAINT chk_corporate_events_type
        CHECK (event_type IN ('dividend', 'split', 'symbol_change', 'index_change', 'acquisition')),
    CONSTRAINT chk_corporate_events_status
        CHECK (status IN ('announced', 'confirmed', 'cancelled')),
    CONSTRAINT chk_corporate_events_revision_positive
        CHECK (revision >= 1),
    CONSTRAINT chk_corporate_events_symbol_format
        CHECK (symbol ~ '^[A-Z0-9][A-Z0-9.\-]*$'),
    CONSTRAINT chk_corporate_events_key_not_blank
        CHECK (btrim(source_event_key) <> ''),
    CONSTRAINT chk_corporate_events_hash_format
        CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    -- 同一事件的版本号唯一
    CONSTRAINT uq_corporate_events_revision
        UNIQUE (source, source_event_key, revision),
    -- 内容完全相同的不重复写入（Kafka 重复投递）
    CONSTRAINT uq_corporate_events_content
        UNIQUE (source, source_event_key, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_corporate_events_symbol_date ON events.corporate_events (symbol, event_date);
CREATE INDEX IF NOT EXISTS idx_corporate_events_type_date   ON events.corporate_events (event_type, event_date);
CREATE INDEX IF NOT EXISTS idx_corporate_events_security    ON events.corporate_events (security_id) WHERE security_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_corporate_events_unresolved  ON events.corporate_events (event_id) WHERE security_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_corporate_events_known_at    ON events.corporate_events (known_at);


-- =========================================================
-- 只追加：禁止删除；更新只允许把 security_id 从空回填成有值
-- =========================================================
CREATE OR REPLACE FUNCTION events.protect_corporate_events() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'events.corporate_events 只允许追加，不允许删除；修改请追加新版本';
    END IF;
    IF (to_jsonb(NEW) - 'security_id') IS DISTINCT FROM (to_jsonb(OLD) - 'security_id') THEN
        RAISE EXCEPTION 'events.corporate_events 只允许回填 security_id，其他字段不能修改；修改请追加新版本';
    END IF;
    IF OLD.security_id IS NOT NULL AND NEW.security_id IS DISTINCT FROM OLD.security_id THEN
        RAISE EXCEPTION 'security_id 已有值（%），只能从空回填，不能改成其他值', OLD.security_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_corporate_events_protect ON events.corporate_events;
CREATE TRIGGER trg_corporate_events_protect
    BEFORE UPDATE OR DELETE ON events.corporate_events
    FOR EACH ROW EXECUTE FUNCTION events.protect_corporate_events();


-- =========================================================
-- 写入：record_event
--   内容和已有某个版本完全相同 → duplicate（不写入，返回已有版本）
--   同一事件已存在但内容不同   → revised（写入新版本，revision + 1）
--   全新事件                   → inserted（revision = 1）
--   同一 (source, source_event_key) 加事务级咨询锁，多个消费者并发写同一事件时排队，版本号不会冲突
-- =========================================================
CREATE OR REPLACE FUNCTION events.record_event(
    p_event_type       text,
    p_symbol           text,
    p_event_date       date,
    p_status           text,
    p_source           text,
    p_source_event_key text,
    p_batch_id         text,
    p_payload          jsonb,
    p_announced_at     timestamptz DEFAULT NULL,
    p_known_at         timestamptz DEFAULT now(),
    p_security_id      bigint      DEFAULT NULL
) RETURNS TABLE (event_id bigint, revision integer, action text) AS $$
DECLARE
    v_hash     text;
    v_existing events.corporate_events%ROWTYPE;
    v_rev      integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_source || '|' || p_source_event_key, 0));

    -- 哈希覆盖事件的全部业务内容（类型、代码、生效日、状态、公告时间、原始内容）
    v_hash := encode(sha256(convert_to(
        jsonb_build_array(p_event_type, p_symbol, p_event_date::text, p_status,
                          p_announced_at::text, p_payload)::text, 'UTF8')), 'hex');

    SELECT * INTO v_existing FROM events.corporate_events e
    WHERE e.source = p_source AND e.source_event_key = p_source_event_key AND e.payload_hash = v_hash;
    IF FOUND THEN
        RETURN QUERY SELECT v_existing.event_id, v_existing.revision, 'duplicate'::text;
        RETURN;
    END IF;

    SELECT coalesce(max(e.revision), 0) + 1 INTO v_rev FROM events.corporate_events e
    WHERE e.source = p_source AND e.source_event_key = p_source_event_key;

    RETURN QUERY
    INSERT INTO events.corporate_events AS e
        (event_type, symbol, security_id, event_date, announced_at, known_at, status, revision,
         source, source_event_key, payload_hash, batch_id, payload)
    VALUES (p_event_type, p_symbol, p_security_id, p_event_date, p_announced_at, coalesce(p_known_at, now()),
            p_status, v_rev, p_source, p_source_event_key, v_hash, p_batch_id, p_payload)
    RETURNING e.event_id, e.revision, CASE WHEN v_rev = 1 THEN 'inserted' ELSE 'revised' END;
END;
$$ LANGUAGE plpgsql;


-- =========================================================
-- 查询
-- =========================================================
-- 当前版本：每个事件取版本号最大的一行
CREATE OR REPLACE VIEW events.v_corporate_events_current AS
SELECT DISTINCT ON (source, source_event_key) *
FROM events.corporate_events
ORDER BY source, source_event_key, revision DESC;

-- 按时点：站在某个时间点，我们知道的每个事件的最新版本
CREATE OR REPLACE FUNCTION events.corporate_events_asof(p_as_of timestamptz)
RETURNS SETOF events.corporate_events AS $$
    SELECT DISTINCT ON (source, source_event_key) *
    FROM events.corporate_events
    WHERE known_at <= p_as_of
    ORDER BY source, source_event_key, revision DESC
$$ LANGUAGE sql STABLE;


-- 010 里预留的外键：代码历史的某一段由哪个事件引起
ALTER TABLE ref.security_identifiers
    ADD CONSTRAINT fk_security_identifiers_event
    FOREIGN KEY (event_id) REFERENCES events.corporate_events (event_id);


COMMENT ON TABLE  events.corporate_events                  IS '公司事件总表：每个事件的每个版本一行，只追加；写入请用 events.record_event()';
COMMENT ON COLUMN events.corporate_events.symbol           IS '数据源给出的原始代码；对应到证券用 security_id';
COMMENT ON COLUMN events.corporate_events.security_id      IS '对应到 ref.securities 后回填；只能从空回填一次';
COMMENT ON COLUMN events.corporate_events.event_date       IS '生效日：除息日 / 拆股生效日 / 代码变更日 / 成分股变更生效日 / 收购完成日';
COMMENT ON COLUMN events.corporate_events.known_at         IS '我们得知这个版本的时间；按时点查询用';
COMMENT ON COLUMN events.corporate_events.status           IS 'announced=已公告；confirmed=已确认/已生效；cancelled=已取消';
COMMENT ON COLUMN events.corporate_events.revision         IS '版本号，从 1 开始；内容变化时追加新版本';
COMMENT ON COLUMN events.corporate_events.source_event_key IS '数据源里的唯一标识；没有则按规则拼，如 AAPL|2020-08-31|split';
COMMENT ON COLUMN events.corporate_events.payload_hash     IS 'sha256(类型, 代码, 生效日, 状态, 公告时间, 原始内容)；同一事件内容相同则视为重复投递';
COMMENT ON FUNCTION events.record_event(text, text, date, text, text, text, text, jsonb, timestamptz, timestamptz, bigint)
    IS '写入事件：返回 (event_id, revision, action)，action = inserted / revised / duplicate';
COMMENT ON VIEW     events.v_corporate_events_current   IS '每个事件的当前版本';
COMMENT ON FUNCTION events.corporate_events_asof(timestamptz) IS '站在某个时间点，每个事件的最新版本（known_at <= 该时间）';
