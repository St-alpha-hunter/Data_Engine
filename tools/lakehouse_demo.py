"""
湖仓演示：用 DuckDB 查 Delta 表（面试演示用）

    python tools/lakehouse_demo.py            # 默认 AAPL
    python tools/lakehouse_demo.py MSFT

演示三件事：
    1. 复权价：不复权价 + 复权事件现算（不依赖 ClickHouse）
    2. 版本历史：每天一次导出 = 一个版本，记着当时导到 golden 的哪个时间点
    3. 时间旅行：回到第 0 版看当时的数据，和现在对比哪些行变了
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines import lakehouse  # noqa: E402

ADJUSTED = """
WITH day AS (            -- 同一天多个事件（如拆股 + 分红）先相乘
    SELECT security_id, ex_date, exp(sum(ln(price_factor))) AS pf
    FROM adjustment_events WHERE is_active GROUP BY ALL),
cum AS (                 -- 日期早于 ex_date 的价格要乘的系数 = ex_date 及之后所有因子的乘积
    SELECT security_id, ex_date, exp(sum(ln(pf)) OVER (PARTITION BY security_id ORDER BY ex_date DESC)) AS factor
    FROM day)
SELECT p.date, p.close, round(p.close * coalesce(c.factor, 1), 4) AS adj_close, p.version
FROM price_daily p
JOIN securities s USING (security_id)
ASOF LEFT JOIN cum c ON c.security_id = p.security_id AND p.date < c.ex_date
WHERE s.symbol = ?
QUALIFY p.date = max(p.date) OVER (PARTITION BY year(p.date))     -- 每年最后一个交易日
ORDER BY p.date
"""


def main(symbol: str = "AAPL"):
    con = lakehouse.connect()
    print(f"== 1. {symbol} 每年最后一个交易日：不复权价、现算的复权价 ==")
    print(con.execute(ADJUSTED, [symbol]).df().to_string(index=False))

    print("\n== 2. price_daily 的版本历史 ==")
    dt = lakehouse.open_table("price_daily")
    for c in dt.history()[:8]:
        metrics = c.get("operationMetrics", {})
        print(f"  第 {c['version']} 版  {datetime.fromtimestamp(c['timestamp'] / 1000):%Y-%m-%d %H:%M}  "
              f"{c['operation']:<8} 新增 {metrics.get('num_target_rows_inserted', metrics.get('num_added_rows', '-'))}"
              f"  更新 {metrics.get('num_target_rows_updated', '-')}  golden 水位线 {c.get(lakehouse.WATERMARK_KEY, '-')}")

    print("\n== 3. 时间旅行：第 0 版 vs 现在，哪些行变了 ==")
    if dt.version() == 0:
        print("  只有第 0 版，等每日流程再导出几次就能对比")
        return
    con.register("v0", lakehouse.open_table("price_daily", version=0).to_pyarrow_dataset())
    print(con.sql("""
        SELECT count(*) FILTER (WHERE v0.security_id IS NULL)        AS 新增行,
               count(*) FILTER (WHERE v0.version < price_daily.version) AS 被改写的行
        FROM price_daily LEFT JOIN v0 USING (security_id, date)""").df())


if __name__ == "__main__":
    main(*sys.argv[1:])
