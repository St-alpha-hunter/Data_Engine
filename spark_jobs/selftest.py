"""
Spark 端的自检（Spark 镜像里没有 pytest，用 spark-submit 跑；失败会抛 AssertionError、退出码非 0）

    docker compose exec spark /opt/spark/bin/spark-submit spark_jobs/selftest.py

覆盖：每条质量规则、被隔离的不参与合成、质量状态向上传递、1h / 1d 的应有根数、未校验的旧数据
"""
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from spark_jobs.bars import aggregate, as_base, check_bars, spark_session

UTC = timezone.utc
D = date(2026, 10, 16)
OPEN = datetime(2026, 10, 16, 13, 30, tzinfo=UTC)          # 纽约 9:30（夏令时）


def bar(i, o=10.0, h=10.5, l=9.5, c=10.0, v=100.0, sid=1, start=None):
    return (sid, f"S{sid}", start or OPEN + timedelta(minutes=5 * i), D, o, h, l, c, v, "b1")


def frame(spark, rows):
    cols = ["security_id", "symbol", "bar_start", "trade_date", "open", "high", "low", "close", "volume", "batch_id"]
    return spark.createDataFrame(rows, cols)


def main():
    spark = spark_session("selftest")
    spark.sparkContext.setLogLevel("ERROR")
    cal = spark.createDataFrame([(D, OPEN, OPEN + timedelta(hours=6, minutes=30))], ["trade_date", "open_utc", "close_utc"])

    # 证券 1：完整一天 78 根，第 3 根价格为 0、第 5 根最高价低于收盘、第 7 根成交量为负、第 20 根跳了又回来
    rows = [bar(i) for i in range(78)]
    rows[3] = bar(3, o=0.0, l=0.0)                            # 开盘价 0（最低价也是 0，只违反 IB001）
    rows[5] = bar(5, h=9.9, c=10.0)
    rows[7] = bar(7, v=-1.0)
    rows[20] = bar(20, h=12.0, c=11.5)                        # 10 → 11.5 → 10：跳 15% 又回来
    rows.append(bar(0, sid=1, start=OPEN + timedelta(hours=7)))   # 收盘后的一根（IB004）
    checked = check_bars(frame(spark, rows), cal).cache()
    flags = {r["bar_start"].replace(tzinfo=UTC): (r["quality_flags"], r["quality_status"]) for r in checked.collect()}   # collect 回来不带时区（容器是 UTC）
    t = lambda i: OPEN + timedelta(minutes=5 * i)
    assert flags[t(3)] == (["IB001"], "quarantined"), flags[t(3)]
    assert flags[t(5)] == (["IB002"], "quarantined"), flags[t(5)]
    assert flags[t(7)] == (["IB003"], "quarantined"), flags[t(7)]
    assert flags[t(20)] == (["IB005"], "warning"), flags[t(20)]
    assert flags[OPEN + timedelta(hours=7)] == (["IB004"], "warning")
    assert flags[t(0)] == ([], "health")

    agg = {(r["interval"], r["bar_start"].replace(tzinfo=UTC)): r for r in aggregate(checked, cal).collect()}
    day = agg[("1d", OPEN)]
    assert day["n_bars"] == 76 and not day["is_complete"], day                 # 78 - 3 根隔离 + 1 根收盘后的（warning 照样算）
    assert day["quality_status"] == "warning"                                   # 含 warning 的 5 分钟线
    q = agg[("15min", OPEN)]                                                    # 9:30–9:45：0、1、2 都正常
    assert q["n_bars"] == 3 and q["is_complete"] and q["quality_status"] == "health", q
    h = agg[("1h", OPEN)]                                                       # 9:30–10:30：12 根里隔离 3 根
    assert h["n_bars"] == 9 and not h["is_complete"]
    last = agg[("1h", OPEN + timedelta(hours=6))]                               # 15:30–16:00 只有半小时
    assert last["n_bars"] == 6 and last["is_complete"], last
    assert ("15min", OPEN + timedelta(hours=7)) in agg                          # 收盘后的只是 warning，照样合成

    # 未校验的旧数据（quality_status 为空）合成出来是 unchecked
    old = as_base(checked).withColumn("quality_status", F.lit(None).cast("string"))
    assert {r["quality_status"] for r in aggregate(old, cal).collect()} == {"unchecked"}
    print("selftest ok")


if __name__ == "__main__":
    main()
