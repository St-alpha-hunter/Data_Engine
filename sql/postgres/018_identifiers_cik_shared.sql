-- =========================================================
-- CIK 允许多只证券同时共用
--   CIK 代表的是公司（SEC 登记的发行人），同一家公司的多类股票共用一个 CIK，
--   如 GOOG（C 类股）和 GOOGL（A 类股）都是 Alphabet 的 0001652044，而且两只都在 S&P 500 里。
--   原来的排除约束要求"同一个代码同一时间只能指向一只证券"，对 CIK 也生效，第二只写不进去。
--   改为只对 symbol / cusip / isin 生效（这三种是证券级别的代码）。
-- "同一只证券、同一种代码同一时间只能有一个值"不变：一只证券同一时间仍然只有一个 CIK。
-- =========================================================
ALTER TABLE ref.security_identifiers DROP CONSTRAINT exc_security_identifiers_value_one_security;
ALTER TABLE ref.security_identifiers ADD CONSTRAINT exc_security_identifiers_value_one_security
    EXCLUDE USING gist (id_type WITH =, id_value WITH =, daterange(valid_from, valid_to, '[)') WITH &&)
    WHERE (id_type <> 'cik');

CREATE INDEX IF NOT EXISTS idx_security_identifiers_cik
    ON ref.security_identifiers (id_value) WHERE id_type = 'cik';

COMMENT ON CONSTRAINT exc_security_identifiers_value_one_security ON ref.security_identifiers IS
    '同一个 symbol / cusip / isin 同一时间只能指向一只证券；CIK 是公司级代码，允许多类股票共用';
