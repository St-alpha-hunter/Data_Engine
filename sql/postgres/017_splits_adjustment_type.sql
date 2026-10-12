-- =========================================================
-- 拆股表增加 split_type = 'adjustment'（价格调整）
--   FMP 把合并、分拆、特别分配等事件对股价的影响，记成"比例小于 1 的 stock-dividend"
--   （如 BK 2007 合并 4717:5000、VTR 2015 分拆 CCP 8757:10000、AIV 2020 分拆 AIR 807:1000）
--   这些不是真正的拆股 / 合股 / 送股，但复权时需要；单独用 adjustment 保存，原值不改
-- 类型与比例的对应关系改为按类型检查：
--   forward / stock_dividend：分子 > 分母（股数增加）
--   reverse：分子 < 分母（股数减少）
--   adjustment：不限方向（含义是价格调整系数，不是股数变化）
-- =========================================================
ALTER TABLE events.splits DROP CONSTRAINT chk_splits_type;
ALTER TABLE events.splits ADD CONSTRAINT chk_splits_type
    CHECK (split_type IN ('forward', 'reverse', 'stock_dividend', 'adjustment'));

ALTER TABLE events.splits DROP CONSTRAINT chk_splits_type_matches_ratio;
ALTER TABLE events.splits ADD CONSTRAINT chk_splits_type_matches_ratio
    CHECK (CASE split_type
               WHEN 'forward'        THEN numerator > denominator
               WHEN 'stock_dividend' THEN numerator > denominator
               WHEN 'reverse'        THEN numerator < denominator
               WHEN 'adjustment'     THEN true
           END);

COMMENT ON COLUMN events.splits.split_type IS
    'forward=拆股；reverse=合股（分子<分母）；stock_dividend=股票股利；adjustment=价格调整（FMP 把合并、分拆、特别分配记成比例<1 的 stock-dividend）';
COMMENT ON TABLE events.splits IS
    '拆股明细（含合股、股票股利、价格调整），与 events.corporate_events 一对一；只追加；写入请用 events.record_split()';
