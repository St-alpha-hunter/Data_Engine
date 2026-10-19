"""
5 分钟线合成 15min / 1h / 1d（Spark + Delta），流处理和批处理共用同一个合成函数 aggregate()

    stream ：Kafka（bars.5min）→ 每个微批：5 分钟线 MERGE 进 silver → 受影响的（证券, 交易日）重算各周期 → MERGE 进 silver
    verify ：从 bronze 用同一个 aggregate() 批量算一遍，和 silver（流处理的结果）逐行比对；合成日线再和 golden 日线比
    rebuild：从 bronze 批量重建 silver（流处理出问题、或改了合成逻辑后修数据用：Lambda 架构里的批处理层）

运行（Spark 容器里，见 docs/streaming.md）：
    docker compose exec spark spark-submit spark_jobs/bars.py stream
    docker compose exec spark spark-submit spark_jobs/bars.py verify

为什么不用 Spark 自带的"窗口聚合 + 水位线"：
    · 一个查询里要出三种周期，多个流式聚合不能串联 / 合并输出
    · 水位线之后到的迟到数据会被直接丢掉；这里每次从已存的 5 分钟线重算受影响的那一天，迟到多久都能改对
    · foreachBatch 是"至少一次"（失败重启会重放上一个微批），配合按主键 MERGE → 结果等价于"恰好一次"
    代价：每个微批要读一遍受影响那几天的 5 分钟线；一天一只股票只有 78 根，很便宜

silver 表 lakehouse/bars：主键 (security_id, interval, bar_start)，按 interval、trade_date 分区
    interval = 5min（去重后的原始 K 线）/ 15min / 1h / 1d
    1h 从 9:30 对齐（9:30、10:30 … 15:30，最后一根只有半小时）；1d 按纽约交易日
    n_bars：由几根 5 分钟线合成；is_complete：根数够不够（按交易日历算，提前收盘的日子 1d 只要 42 根）
"""
import argparse
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.bar_rules import BY_ID, DANGER

LAKE = os.getenv("LAKEHOUSE_DIR", "/opt/project/data/lakehouse")
BRONZE = f"{LAKE}/bars_5min"
SILVER = f"{LAKE}/bars"
PRICE_DAILY = f"{LAKE}/price_daily"
CHECKPOINT = f"{LAKE}/_checkpoints/bars_stream"
TOPIC = "bars.5min"
NY = "America/New_York"

KEYS = ["security_id", "interval", "bar_start"]
BASE_COLS = ["security_id", "symbol", "bar_start", "trade_date", "open", "high", "low", "close", "volume", "batch_id"]
SILVER_COLS = ["security_id", "symbol", "interval", "bar_start", "trade_date", "open", "high", "low", "close",
               "volume", "n_bars", "is_complete", "quality_status", "quality_flags", "batch_id", "computed_at", "source"]
# 2026-10-18 加清洗时新增的列；之前写的行这几列为空（quality_status 为空 = 未校验，历史数据不回跑）
QUALITY_COLS = {"quality_status": "STRING", "quality_flags": "ARRAY<STRING>", "batch_id": "STRING"}
INTERVALS = {"15min": ("15 minutes", None), "1h": ("1 hour", "30 minutes")}     # 1h 的窗口从 :30 开始

MESSAGE = T.StructType([
    T.StructField("security_id", T.LongType()), T.StructField("symbol", T.StringType()),
    T.StructField("bar_start", T.StringType()), T.StructField("trade_date", T.StringType()),
    T.StructField("open", T.DoubleType()), T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()), T.StructField("close", T.DoubleType()),
    T.StructField("volume", T.DoubleType()), T.StructField("batch_id", T.StringType()),
    T.StructField("sent_at", T.StringType()),
])


def spark_session(app: str) -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")              # 单机小数据，默认 200 个分区太碎
            .config("spark.driver.memory", "2g")
            .getOrCreate())


# =========================
# 交易日历（只读账号从 Postgres 读）
# =========================
def sessions(spark: SparkSession) -> DataFrame:
    """每个交易日的开盘 / 收盘时刻（UTC）；提前收盘的日子收盘是 13:00"""
    cal = (spark.read.format("jdbc")
           .option("url", os.environ["PG_JDBC_URL"])
           .option("user", os.environ["PG_RO_USER"]).option("password", os.environ["PG_RO_PASSWORD"])
           .option("query", "SELECT date AS trade_date, open_time::text AS open_time, close_time::text AS close_time "
                            "FROM ref.trading_calendar WHERE exchange = 'XNYS' AND is_trading_day "
                            "AND date >= '2010-01-01'")
           .load())
    local = lambda col: F.to_utc_timestamp(F.concat_ws(" ", F.col("trade_date").cast("string"), F.col(col)), NY)
    return cal.select("trade_date", local("open_time").alias("open_utc"), local("close_time").alias("close_utc")).cache()


# =========================
# 清洗校验（流、批共用；规则定义在 spark_jobs/bar_rules.py）
# =========================
def check_bars(bars: DataFrame, cal: DataFrame) -> DataFrame:
    """
    每根 5 分钟线 → quality_flags（违反了哪些规则）、quality_status（health / warning / quarantined）
    IB005 要看前后两根，所以传进来的必须是（证券, 交易日）当天的全部 5 分钟线
    """
    tol, jump = BY_ID["IB002"]["params"]["tolerance"], BY_ID["IB005"]["params"]["jump"]
    w = Window.partitionBy("security_id", "trade_date").orderBy("bar_start")
    r1 = F.col("close") / F.lag("close").over(w) - 1
    r2 = F.lead("close").over(w) / F.col("close") - 1
    prices = ["open", "high", "low", "close"]
    conditions = {
        "IB001": (F.greatest(*[F.col(c).isNull().cast("int") for c in prices]) == 1)
                 | (F.least(*[F.col(c) for c in prices]) <= 0),
        "IB002": (F.col("high") < F.greatest("open", "close") - tol) | (F.col("low") > F.least("open", "close") + tol),
        "IB003": F.col("volume") < 0,
        "IB004": F.col("open_utc").isNull() | (F.col("bar_start") < F.col("open_utc"))
                 | (F.col("bar_start") >= F.col("close_utc")),
        "IB005": (F.abs(r1) > jump) & (F.abs(r2) > jump) & (r1 * r2 < 0),
    }
    flags = F.filter(F.array(*[F.when(F.coalesce(c, F.lit(False)), F.lit(rid)) for rid, c in conditions.items()]),
                     lambda x: x.isNotNull())
    danger = F.exists("quality_flags", lambda x: x.isin(DANGER))
    return (bars.join(F.broadcast(cal), "trade_date", "left")
            .withColumn("quality_flags", flags)
            .withColumn("quality_status", F.when(danger, "quarantined")
                        .when(F.size("quality_flags") > 0, "warning").otherwise("health"))
            .drop("open_utc", "close_utc"))


# =========================
# 合成（流、批共用）
# =========================
def aggregate(bars: DataFrame, cal: DataFrame) -> DataFrame:
    """
    检查过的 5 分钟线 → 15min / 1h / 1d；被隔离的不参与（所在的 K 线根数不够 → 不完整）
    开盘 = 最早一根的开盘、收盘 = 最晚一根的收盘（min_by / max_by 按时间取，和到达顺序无关）、最高 / 最低 / 成交量合计
    质量状态取所含 5 分钟线里最差的：有未校验的 → unchecked；有 warning → warning；否则 health
    """
    status = F.col("quality_status")
    b = bars.where(status.isNull() | (status != "quarantined")).join(F.broadcast(cal), "trade_date")
    agg = [F.min_by("open", "bar_start").alias("open"), F.max("high").alias("high"), F.min("low").alias("low"),
           F.max_by("close", "bar_start").alias("close"), F.sum("volume").alias("volume"),
           F.count(F.lit(1)).cast("int").alias("n_bars"),
           F.max(status.isNull().cast("int")).alias("_unchecked"), F.max((status == "warning").cast("int")).alias("_warn")]
    keys = ["security_id", "symbol", "trade_date", "open_utc", "close_utc"]
    parts = []
    for name, (size, start) in INTERVALS.items():
        w = F.window("bar_start", size, size, start) if start else F.window("bar_start", size)
        parts.append(b.groupBy(*keys, w.alias("w")).agg(*agg)
                     .select(*keys, "open", "high", "low", "close", "volume", "n_bars", "_unchecked", "_warn",
                             F.lit(name).alias("interval"), F.col("w.start").alias("bar_start"),
                             F.col("w.end").alias("bar_end")))
    parts.append(b.groupBy(*keys).agg(*agg)
                 .withColumn("interval", F.lit("1d"))
                 .withColumn("bar_start", F.col("open_utc")).withColumn("bar_end", F.col("close_utc")))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    # 应有根数 = 窗口和交易时段重叠的分钟数 / 5（1h 最后一根 15:30–16:00 只有 6 根；提前收盘的日子 1d 只有 42 根）
    secs = (F.unix_timestamp(F.least("bar_end", "close_utc")) - F.unix_timestamp(F.greatest("bar_start", "open_utc")))
    return (out.withColumn("is_complete", F.col("n_bars") >= (secs / 300).cast("int"))
            .withColumn("quality_status", F.when(F.col("_unchecked") == 1, "unchecked")
                        .when(F.col("_warn") == 1, "warning").otherwise("health"))
            .withColumn("quality_flags", F.lit(None).cast("array<string>"))
            .withColumn("batch_id", F.lit(None).cast("string"))
            .select(*[c for c in SILVER_COLS if c not in ("computed_at", "source")]))


def as_base(bars: DataFrame) -> DataFrame:
    """5 分钟线本身也存进 silver（interval = 5min，带检查结果和批次号）：之后各周期都从这里重算"""
    return (bars.select(*BASE_COLS, "quality_status", "quality_flags").withColumn("interval", F.lit("5min"))
            .withColumn("n_bars", F.lit(1)).withColumn("is_complete", F.lit(True)))


def ensure_quality_columns(spark: SparkSession, path: str = SILVER):
    """silver 是加清洗之前建的：缺的列补上（Delta 加列只改元数据，旧文件不重写，旧行这几列为空）"""
    if not DeltaTable.isDeltaTable(spark, path):
        return
    have = set(spark.read.format("delta").load(path).columns)
    missing = [f"{c} {t}" for c, t in QUALITY_COLS.items() if c not in have]
    if missing:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({', '.join(missing)})")
        print(f"[silver] 新增列：{missing}", flush=True)


def with_meta(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("computed_at", F.current_timestamp()).withColumn("source", F.lit(source)).select(*SILVER_COLS)


def upsert(spark: SparkSession, df: DataFrame, path: str = SILVER):
    """按主键 MERGE：值变了才更新、没有就插入 → 重复处理同一批数据结果不变"""
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("interval", "trade_date").save(path)
        return
    dates = [r[0].isoformat() for r in df.select("trade_date").distinct().collect()]
    scope = f"t.trade_date IN ({', '.join(repr(d) for d in dates)}) AND " if dates else ""      # 只扫涉及的分区
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in
                          ["open", "high", "low", "close", "volume", "n_bars", "is_complete",
                           "quality_status", "quality_flags", "batch_id"])
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(df.alias("s"), scope + " AND ".join(f"t.{k} = s.{k}" for k in KEYS))
     .whenMatchedUpdateAll(condition=changed)
     .whenNotMatchedInsertAll()
     .execute())


# =========================
# 流处理
# =========================
def process_batch(spark: SparkSession, cal: DataFrame, batch: DataFrame, batch_id: int):
    batch = batch.dropDuplicates(["security_id", "bar_start"]).cache()        # 同一批里重复的消息
    n = batch.count()
    if n == 0:
        return
    # 这批涉及的（证券, 交易日）当天的全部 5 分钟线 = 已存的 + 新来的（同一根以新来的为准）→ 检查 → 写回 → 重算各周期
    touched = batch.select("security_id", "trade_date").distinct()
    dates = [r[0] for r in touched.select("trade_date").distinct().collect()]
    fresh = batch.select(*BASE_COLS)
    day = fresh
    if DeltaTable.isDeltaTable(spark, SILVER):
        stored = (spark.read.format("delta").load(SILVER)
                  .where((F.col("interval") == "5min") & F.col("trade_date").isin(dates))
                  .join(F.broadcast(touched), ["security_id", "trade_date"]).select(*BASE_COLS))
        day = (stored.join(fresh.select("security_id", "bar_start"), ["security_id", "bar_start"], "left_anti")
               .unionByName(fresh))
    checked = check_bars(day, cal).cache()
    upsert(spark, with_meta(as_base(checked), "stream"))
    upsert(spark, with_meta(aggregate(checked, cal), "stream"))
    bad = {r[0]: r[1] for r in checked.where("quality_status <> 'health'").groupBy("quality_status").count().collect()}
    lag = batch.select(F.max(F.unix_timestamp(F.current_timestamp())
                             - F.unix_timestamp(F.to_timestamp("sent_at")))).first()[0]
    print(f"[stream] 微批 {batch_id}：{n} 根 5 分钟线，涉及 {touched.count()} 个（证券, 交易日），"
          f"最大延迟 {lag} 秒；校验：{bad or '全部正常'}", flush=True)
    checked.unpersist()
    batch.unpersist()


def stream(spark: SparkSession, bootstrap: str):
    cal = sessions(spark)
    ensure_quality_columns(spark)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 50000)           # 一个微批最多读多少条，积压时不会一口吃太多
           .option("failOnDataLoss", "false")               # Kafka 主题被重建（offset 变小）时从现有的接着读，不整个停掉
           .load())
    bars = (raw.select(F.from_json(F.col("value").cast("string"), MESSAGE).alias("m")).select("m.*")
            .withColumn("bar_start", F.to_timestamp("bar_start"))
            .withColumn("trade_date", F.to_date("trade_date")))
    query = (bars.writeStream
             .foreachBatch(lambda df, i: process_batch(spark, cal, df, i))
             .option("checkpointLocation", CHECKPOINT)       # 记录 Kafka 读到哪了；重启后接着读
             .trigger(processingTime="5 seconds")
             .start())
    query.awaitTermination()


# =========================
# 批处理：核对 / 重建
# =========================
def bronze_base(spark: SparkSession) -> DataFrame:
    """bronze 去重：同一根 K 线拉过多次的取最近一次"""
    w = Window.partitionBy("security_id", "bar_start").orderBy(F.col("ingested_at").desc())
    return (spark.read.format("delta").load(BRONZE)
            .withColumn("_r", F.row_number().over(w)).where("_r = 1").select(*BASE_COLS))


def batch_result(spark: SparkSession, cal: DataFrame) -> DataFrame:
    checked = check_bars(bronze_base(spark), cal)
    return as_base(checked).unionByName(aggregate(checked, cal))


def verify(spark: SparkSession) -> int:
    cal = sessions(spark)
    expected = batch_result(spark, cal).alias("e")
    actual = spark.read.format("delta").load(SILVER).alias("a")
    joined = expected.join(actual, KEYS, "full_outer")
    cols = ["open", "high", "low", "close", "volume", "n_bars"]
    # 相对误差：成交量有小数时，浮点数相加的结果和相加顺序有关（流是分批算的、批是一次算的），末几位会不一样
    rel = lambda c: F.abs(F.col(f"e.{c}") - F.col(f"a.{c}")) / F.greatest(F.abs(F.col(f"e.{c}")), F.lit(1.0))
    diff = joined.where(F.col("e.symbol").isNull() | F.col("a.symbol").isNull()
                        | (F.greatest(*[rel(c) for c in cols]) > 1e-12))
    print("== 流处理结果 vs 批处理重算（每个周期的行数、不一致行数）==")
    (joined.groupBy(F.coalesce("e.interval", "a.interval").alias("interval"))
     .agg(F.count(F.col("e.symbol")).alias("batch_rows"), F.count(F.col("a.symbol")).alias("stream_rows"),
          F.sum(F.col("e.is_complete").cast("int")).alias("complete"))
     .join(diff.groupBy(F.coalesce("e.interval", "a.interval").alias("interval")).count()
           .withColumnRenamed("count", "mismatched"), "interval", "left")
     .fillna(0, ["mismatched"]).orderBy("interval").show())
    n_diff = diff.count()
    if n_diff:
        diff.select(*KEYS, *[F.col(f"e.{c}").alias(f"batch_{c}") for c in ["close", "n_bars"]],
                    *[F.col(f"a.{c}").alias(f"stream_{c}") for c in ["close", "n_bars"]]).show(10, truncate=False)

    # 合成日线 vs golden 日线（湖里的 price_daily）：分钟线只有常规时段，收盘价是 15:55 那根的收盘、不含收盘集合竞价
    print("== 合成日线 vs golden 日线（完整的交易日）==")
    golden = spark.read.format("delta").load(PRICE_DAILY).select(
        "security_id", F.col("date").alias("trade_date"), F.col("close").alias("g_close"),
        F.col("volume").alias("g_volume"), "open", "high", "low")
    day = (actual.where("interval = '1d' AND is_complete")
           .join(golden.withColumnRenamed("open", "g_open").withColumnRenamed("high", "g_high")
                 .withColumnRenamed("low", "g_low"), ["security_id", "trade_date"]))
    pct = lambda a, b: F.abs(F.col(a) / F.col(b) - 1) * 100
    (day.select(pct("close", "g_close").alias("close_diff_pct"), pct("open", "g_open").alias("open_diff_pct"),
                (F.col("volume") / F.col("g_volume")).alias("volume_ratio"))
     .agg(F.count(F.lit(1)).alias("days"),
          F.expr("percentile_approx(close_diff_pct, 0.5)").alias("close_diff_median_%"),
          F.expr("percentile_approx(close_diff_pct, 0.99)").alias("close_diff_p99_%"),
          F.expr("percentile_approx(open_diff_pct, 0.5)").alias("open_diff_median_%"),
          F.expr("percentile_approx(volume_ratio, 0.5)").alias("volume_ratio_median"),
          F.sum((F.col("close_diff_pct") > 1).cast("int")).alias("close_diff_over_1%"))
     .show(truncate=False))
    return n_diff


def rebuild(spark: SparkSession):
    df = with_meta(batch_result(spark, sessions(spark)), "batch")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("interval", "trade_date").save(SILVER)
    print(f"[rebuild] silver 重建完成：{spark.read.format('delta').load(SILVER).count()} 行")


def main():
    parser = argparse.ArgumentParser(description="5 分钟线合成 15min / 1h / 1d")
    parser.add_argument("command", choices=["stream", "verify", "rebuild"])
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    args = parser.parse_args()
    spark = spark_session(f"bars-{args.command}")
    spark.sparkContext.setLogLevel("WARN")
    if args.command == "stream":
        stream(spark, args.bootstrap)
    elif args.command == "verify":
        sys.exit(1 if verify(spark) else 0)
    else:
        rebuild(spark)


if __name__ == "__main__":
    main()
