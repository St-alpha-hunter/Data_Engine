-- golden.v_price_daily：每只证券每天的当前版本（不复权）
--   内层的最大版本号用别名 v：如果直接叫 version，argMax(…, version) 会被 ClickHouse 解析成这个别名
CREATE VIEW IF NOT EXISTS golden.v_price_daily AS
SELECT security_id, date, open, high, low, close, volume, quality_status, source_symbol, batch_id,
       v AS version, loaded_at
FROM
(
    SELECT
        security_id,
        date,
        argMax(open, version)            AS open,
        argMax(high, version)            AS high,
        argMax(low, version)             AS low,
        argMax(close, version)           AS close,
        argMax(volume, version)          AS volume,
        argMax(quality_status, version)  AS quality_status,
        argMax(source_symbol, version)   AS source_symbol,
        argMax(batch_id, version)        AS batch_id,
        max(version)                     AS v,
        argMax(loaded_at, version)       AS loaded_at
    FROM golden.price_daily
    GROUP BY security_id, date
);
