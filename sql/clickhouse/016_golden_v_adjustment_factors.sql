-- golden.v_adjustment_factors：每个除权日的当前因子 + 累积因子
--   cum_price_factor：日期早于 ex_date 的价格要乘的系数 = ex_date 及之后所有事件 price_factor 的乘积
--   同一天有多个事件（如拆股 + 分红）先相乘合并成一天
--   只用当前版本且有效（is_active = 1）的事件；已撤回的不参与
CREATE VIEW IF NOT EXISTS golden.v_adjustment_factors AS
SELECT
    security_id,
    ex_date,
    day_price_factor,
    day_volume_factor,
    exp(sum(log(day_price_factor))  OVER w) AS cum_price_factor,
    exp(sum(log(day_volume_factor)) OVER w) AS cum_volume_factor
FROM
(
    SELECT security_id, ex_date,
           exp(sum(log(pf))) AS day_price_factor,
           exp(sum(log(vf))) AS day_volume_factor
    FROM
    (
        SELECT security_id, ex_date, kind, event_id,
               argMax(price_factor, version)  AS pf,
               argMax(volume_factor, version) AS vf,
               argMax(is_active, version)     AS active
        FROM golden.adjustment_events
        GROUP BY security_id, ex_date, kind, event_id
    )
    WHERE active = 1
    GROUP BY security_id, ex_date
)
WINDOW w AS (PARTITION BY security_id ORDER BY ex_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
