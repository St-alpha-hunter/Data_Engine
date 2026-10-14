-- golden.v_price_daily_adjusted：给回测用的复权价 = 不复权价 × 累积因子（向最新一天对齐，和 FMP 复权价同一口径）
--   ASOF JOIN 找日期之后最近的一个除权日，用它的累积因子；之后再没有事件的日期，因子为 1
--   （ASOF LEFT JOIN 没匹配上时数值列是默认值 0，所以 0 当作 1）
CREATE VIEW IF NOT EXISTS golden.v_price_daily_adjusted AS
SELECT
    p.security_id                                         AS security_id,
    p.date                                                AS date,
    if(f.cum_price_factor = 0, 1, f.cum_price_factor)     AS price_factor,
    if(f.cum_volume_factor = 0, 1, f.cum_volume_factor)   AS volume_factor,
    p.open   * price_factor                               AS adj_open,
    p.high   * price_factor                               AS adj_high,
    p.low    * price_factor                               AS adj_low,
    p.close  * price_factor                               AS adj_close,
    p.volume * volume_factor                              AS adj_volume,
    p.close                                               AS close,
    p.quality_status                                      AS quality_status,
    p.version                                             AS version
FROM golden.v_price_daily AS p
ASOF LEFT JOIN golden.v_adjustment_factors AS f
    ON p.security_id = f.security_id AND p.date < f.ex_date;
