# 湖仓层（Delta Lake + DuckDB）

golden 在 ClickHouse 里生成；每天导出一份到 **Delta Lake 开放表格式**，用 **DuckDB** 查询。
以后 Spark 合成分钟线、上云（GCS）都建立在这一层上。

代码：`pipelines/lakehouse.py`；演示：`python tools/lakehouse_demo.py [代码]`；测试：`test/test_lakehouse.py`。

---

## 1. 为什么加这一层

| | 只有 ClickHouse | 加上 Delta 湖 |
|---|---|---|
| 谁能读 | 只有 ClickHouse | DuckDB、Spark、Databricks、Trino、Polars 都能直接读 |
| 存在哪 | ClickHouse 的数据卷 | 普通目录或对象存储（gs:// / s3://），上云只改一个配置 |
| 写入 | ClickHouse 自己保证 | 事务日志保证：一次导出要么全部可见、要么都不可见 |
| 回看历史 | 靠 golden 的 version / loaded_at 列 | 另外还能按 Delta 版本号或时间点回看整张表（时间旅行） |

**开放表格式** = 一堆 parquet 文件 + 一份事务日志。日志记录"第 N 版由哪些文件组成"，所以普通目录也有了数据库的能力：

```
data/lakehouse/price_daily/
├── _delta_log/
│   ├── 00000000000000000000.json      ← 第 0 版：首次全量（加了哪些文件、行数、水位线）
│   └── 00000000000000000001.json      ← 第 1 版：某天的增量 MERGE（删掉哪些旧文件、加了哪些新文件）
├── year=2010/part-….parquet           ← 按年分区
└── year=2026/part-….parquet
```

---

## 2. 表

| Delta 表 | 来源 | 主键 | 分区 | 写法 |
|---|---|---|---|---|
| `price_daily` | golden.price_daily（不复权） | security_id, date | 按年 | 增量 MERGE |
| `adjustment_events` | golden.adjustment_events | security_id, ex_date, kind, event_id | 无 | 增量 MERGE；已撤回的 `is_active = false` 仍保留，用时过滤 |
| `securities` | golden.securities | security_id | 无 | 内容变了才整表覆盖 |

湖里存的是**当前状态**（每个主键一行 = golden 的当前版本），golden 的 `version`、`loaded_at` 列原样带着；
复权价不单独存，用 DuckDB 现算（和 ClickHouse 的 `golden.v_price_daily_adjusted` 同一口径，已核对 AAPL 两次拆股前后一致）。

ClickHouse 的无符号整数（UInt64 等）在 Delta 里换成有符号的 int64（Delta 没有无符号类型）。

---

## 3. 增量与幂等

1. **水位线**：上次导到 golden 的哪个 `loaded_at`，记在 Delta 那次提交的信息里（`golden_watermark`），不另建表
2. 取 golden 里 `loaded_at >= 水位线` 的行，同一主键取最大版本
3. 和湖里比，只留"湖里没有"或"版本更大"的行（边界上重复取到的、重跑时的都被去掉）
4. 剩下的 `MERGE`：主键匹配且版本更大 → 更新；不存在 → 插入。只扫涉及到的年份分区
5. 没有变化 → **不提交**，不会产生空版本

所以每天导出一次就是一个版本；重跑、补跑都不会重复。

**核对**：导完用 DuckDB 算湖里的行数、版本号之和、收盘价（因子）之和，和 golden 当前版本比；
不一致 → 每日汇总里出一条 `lakehouse_mismatch` 提醒。

---

## 4. 维护（自动，每周一次）

每天 MERGE 会把涉及到的分区文件重写一份，小文件和旧文件越积越多。导出时检查：距上次维护超过 7 天就

| 步骤 | 作用 |
|---|---|
| `optimize.compact()` | 合并小文件 |
| `create_checkpoint()` | 把事务日志汇总成一个检查点，读表时不用从头重放日志 |
| `vacuum(30 天)` | 删掉 30 天前就已不被任何版本使用的文件 → **时间旅行最多回看 30 天** |

手动：`python -m pipelines.lakehouse maintain`（立即做一次）。

---

## 5. 常用命令

```bash
python -m pipelines.lakehouse export              # 导出（每日流程的 export_lakehouse 任务就是调这个）
python -m pipelines.lakehouse verify              # 只核对
python -m pipelines.lakehouse history --table price_daily   # 版本历史
python tools/lakehouse_demo.py AAPL               # 演示：复权价、版本历史、时间旅行
```

Python 里查询：

```python
from pipelines import lakehouse
con = lakehouse.connect()                                    # 三张表注册成 DuckDB 视图
con.sql("SELECT * FROM price_daily WHERE security_id = 1 ORDER BY date DESC LIMIT 5")
old = lakehouse.connect({"price_daily": 3})                  # 时间旅行：price_daily 回到第 3 版
```

位置：环境变量 `LAKEHOUSE_URL`，不设 = `data/lakehouse/`；上云改成 `gs://<桶>/lakehouse`，
认证用 `GOOGLE_APPLICATION_CREDENTIALS`（服务账号密钥文件）。

---

## 6. 面试要点

**Delta Lake vs Apache Iceberg**

| | Delta Lake | Iceberg |
|---|---|---|
| 出身 | Databricks | Netflix → Apache |
| 元数据 | `_delta_log/` 里的 JSON + parquet 检查点，表目录自带 | 元数据文件 + manifest 清单，需要一个**目录服务**（catalog：Hive / REST / Glue / JDBC）记录"表的当前元数据在哪" |
| 分区 | 物理分区列（如 year） | **隐藏分区**：按 `days(ts)` 等变换分区，查询不用写分区列，分区方式还能演进 |
| 生态 | Databricks 原生；Spark、delta-rs、DuckDB、Trino | 引擎中立，Snowflake、AWS（Athena / Glue）、Trino、Flink、Spark 支持都很好 |
| 现状 | 两者功能趋同；Delta 有 UniForm 可同时生成 Iceberg 元数据 | |

选 Delta 的原因：对上 Databricks；`deltalake`（Rust 实现）不需要 Java / Spark 就能读写，单机够用；不用额外部署目录服务。

**时间旅行 vs 双时态（bitemporal）**

- Delta 时间旅行回答"**我们的表**在第 N 版 / 某个时间是什么样"——系统时间，靠保留旧文件实现，有保留期（这里 30 天）
- golden 的 `version` / `loaded_at`（以及事件的 `known_at`）回答"**某一天我们知道的数据**是什么"——永久保留，按时点查询、复现回测靠它
- 两者互补：时间旅行用于排错和回滚（"昨天那次导出改了什么"），时点查询用于业务（"回测在 2026-10-15 看到的价格"）

**其他常被问到的**

- MERGE 怎么做到幂等：版本号条件 `s.version > t.version` + 写之前先剔除没变化的行；没有变化不提交
- 小文件问题：MERGE 是写时复制（copy-on-write），每次重写受影响的文件；按年分区控制每次重写的范围，每周 compact
- 为什么不用 Trino：Java 服务，起步就要 2–4 GB 内存；单机数据量下 DuckDB 嵌入式、零运维更合适
