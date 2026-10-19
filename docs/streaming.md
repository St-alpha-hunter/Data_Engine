# 分钟线：Kafka + Spark（湖仓模式）

5 分钟线经 Kafka 推送，Spark Structured Streaming 一边消费一边**清洗校验**、合成 15 分钟 / 1 小时 / 日线，写进湖（Delta）；
每天的结果同步一份进 ClickHouse；同一套逻辑再用批处理算一遍，逐行核对。

和日线的对比：日线走**数据仓库**（ClickHouse raw → 清洗 → ClickHouse golden，湖里只是导出的副本）；
分钟线走**湖仓**（bronze → silver 都在湖里，ClickHouse 只是查询副本）。两条线的质量规则、问题记录、隔离区是同一套（`meta.*`）。

| 环节 | 代码 | 运行在 |
|---|---|---|
| ① 拉 5 分钟线 → bronze | `pipelines/intraday.py`（每日：`fetch_day`） | Airflow / 本机 |
| ② 推进 Kafka | `pipelines/intraday_sync.py` 的 `publish_day`；回放演示用 `streaming/replay_producer.py` | Airflow / 本机 |
| ③ 清洗校验 + 合成 → silver | `spark_jobs/bars.py stream`（常驻） | Spark 容器 |
| ④ 质量问题入库 + 同步 ClickHouse | `pipelines/intraday_sync.py` 的 `wait_stream` / `sync_day` | Airflow |
| ⑤ 批处理核对 / 重建 | `spark_jobs/bars.py verify` / `rebuild` | Spark 容器 |

```
FMP 5 分钟线 ──① 拉取──▶ bronze: lakehouse/bars_5min（Delta，只追加）
                              │
                              ├──② 回放（按时间顺序，可调倍速，可注入重复 / 迟到）──▶ Kafka 主题 bars.5min（4 分区，键 = 代码）
                              │                                                        │
                              │                              ③ Spark Structured Streaming（每 5 秒一个微批）
                              │                                 foreachBatch：5 分钟线 MERGE → 受影响的（证券, 交易日）重算 → MERGE
                              │                                                        ▼
                              └──④ 批处理：同一个 aggregate() ──逐行比对──▶ silver: lakehouse/bars（5min / 15min / 1h / 1d）
                                                                                       │
                                                                     合成日线 vs golden 日线（lakehouse/price_daily）
```

---

## 1. 范围和数据源

- 股票池 787 只（证券主表的补数据名单），**2026-10-01 起的 5 分钟线**；有数据的 604 只（其余是早已退市的），12 个交易日，56.3 万根，约 15 MB
- 只是 demo，不拉长历史：FMP 套餐（Starter）带宽每 30 天 20 GB，5 分钟线从 2010 年拉要约 39 GB；套餐没有 1 分钟线
- **实时用回放**：盘中轮询要整夜开机（美股是北京时间晚上）、每次返回当天全部 K 线很费带宽；
  回放出来的消息在 Kafka / Spark 眼里和真实行情一样，而且可重复（开发、测试、演示都用它）

FMP 5 分钟线接口：常规时段 9:30–16:00、纽约当地时间、K 线开始时刻；单次只返回约 8 个交易日（多的静默截掉更早的）→ 按 7 天一段请求。

---

## 2. 每日流程（2026-10-18 起）

每日流程 `daily_eod`（北京时间周二到周六 10 点）里和事件、日线并行的一条线：

| 任务 | 做什么 |
|---|---|
| fetch_intraday | 前一交易日的 5 分钟线 → bronze（名单同日线；约 600 只、600 次请求、约 7 MB） |
| publish_intraday | 这一天的 5 分钟线推进 Kafka（不等待，几秒发完） |
| sync_intraday | 等 Spark 流任务处理完（Kafka 各分区末尾 = 检查点里已提交的位置，最多等 20 分钟）→ 质量问题 / 隔离写 meta → 同步 ClickHouse |

Kafka、Spark 常驻（`docker compose up -d` 就会启动；Spark 容器一启动就跑流任务，挂了自动重启，检查点保证接着上次读）。
2026-10-18 首次端到端（10-16）：603 只、46,921 根，推进 Kafka → Spark 1 个微批处理完（等了 90 秒）→ 同步 ClickHouse 67,386 行，校验全部正常。

**历史数据不回跑**：加清洗之前写进 silver 的（10-01 ~ 10-15）`quality_status` 为空 = 未校验，合成出来的是 `unchecked`；没有同步进 ClickHouse。

---

## 3. 清洗校验

规则即代码：`spark_jobs/bar_rules.py`（不依赖 pyspark，Spark 和 Airflow 都导入），同步时登记到 `meta.quality_rules`（接口 `price_5min`）。

| 规则 | 级别 | 检查 |
|---|---|---|
| IB001 | danger | 价格为空或不大于 0 |
| IB002 | danger | 最高价低于开盘 / 收盘价，或最低价高于开盘 / 收盘价 |
| IB003 | danger | 成交量为负 |
| IB004 | warning | K 线时间不在当天交易时段内（按交易日历，提前收盘到 13:00） |
| IB005 | warning | 相对上一根收盘跳 10% 以上、下一根又反向跳回 10% 以上（跳了又回来） |

- danger → 这根 5 分钟线 `quality_status = quarantined`：**不参与合成**（所在的 K 线根数不够 → `is_complete = false`），不同步进 ClickHouse
- warning → 照常合成，打标记；合成出来的 K 线取所含 5 分钟线里最差的状态（有未校验的 → unchecked，有 warning → warning，否则 health）
- IB005 要看前后两根：每个微批都对涉及的（证券, 交易日）当天**全部** 5 分钟线重新检查（新来的一根可能让前一根变成"跳了又回来"）

**问题入库**（`sync_day`，复用 `MetaStore.record_validation`，按批次整体替换，重跑不重复）：
- `meta.quality_issues`：按（批次, 代码, 交易日, 规则）一条，`detail` 里是违反的根数和哪几根。
  meta 的去重键是 批次 + 规则 + 代码 + 日期（按日线设计），一天几十根各记一条会被去重掉
- `meta.quarantine_records`：按（批次, 代码, 交易日）一条，`record` 里是这一天被隔离的全部 K 线
- Spark 不写数据库：检查结果（状态、违反的规则、批次号）存在 silver 里，由 Airflow 的同步任务写 meta。
  这样复用现有的"问题和隔离在一个事务里写、重新清洗时整体替换"，Spark 也不需要数据库的写权限

**Spark 端自检**：`docker compose exec spark /opt/spark/bin/spark-submit spark_jobs/selftest.py`（每条规则、隔离的不参与合成、状态传递、应有根数）

---

## 4. 表

**bronze `lakehouse/bars_5min`**（按 trade_date 分区，只追加）

| 字段 | 说明 |
|---|---|
| security_id, symbol | 证券主表编号、代码 |
| bar_start | K 线开始时刻（UTC；FMP 给的纽约当地时间换算来的） |
| trade_date | 纽约日期 |
| open, high, low, close, volume | FMP 原样 |
| batch_id, ingested_at | 对应 meta.ingestion 的批次；拉取时间 |

批次照旧登记在 `meta.ingestion`，写入记在 `meta.load_log`（layer = raw，target_table = `lakehouse.bars_5min`）；断点续跑按（接口, 参数）。

**Kafka 主题 `bars.5min`**：4 个分区，消息键 = 代码（同一只股票进同一个分区 → 分区内按时间有序），值 = 一根 K 线的 JSON（多一个 `sent_at` 发送时间，用来算延迟）。

**silver `lakehouse/bars`**（主键 security_id + interval + bar_start，按 interval、trade_date 分区）

| 字段 | 说明 |
|---|---|
| interval | `5min`（去重后的原始 K 线）/ `15min` / `1h` / `1d` |
| bar_start, trade_date | 开始时刻（UTC）、纽约日期 |
| open, high, low, close, volume | 开盘 = 最早一根的开盘，收盘 = 最晚一根的收盘，最高 / 最低 / 成交量合计 |
| n_bars, is_complete | 由几根 5 分钟线合成（隔离的不算）；根数够不够 |
| quality_status | 5min：health / warning / quarantined；合成的：health / warning / unchecked；空 = 加清洗之前的旧数据 |
| quality_flags | 5min：违反了哪些规则（如 `["IB005"]`） |
| batch_id | 5min：对应 meta.ingestion 的批次（问题入库用） |
| computed_at, source | 计算时间；`stream` / `batch` |

**ClickHouse `golden.bars_intraday`**（`sql/clickhouse/019`）：silver 的查询副本，字段同上（去掉 quality_flags、source，加 synced_at）；
只放通过校验的（5 分钟线去掉隔离的）；每天按交易日**先删再写**，重跑不会重复；湖是正本，同步出问题可以从湖重新同步。

- 1h 从 9:30 对齐：9:30、10:30 … 15:30，最后一根只有半小时（6 根）
- 1d 按纽约交易日；应有根数按交易日历（Postgres `ref.trading_calendar`，Spark 用只读账号 JDBC 读）：平常 78 根，提前收盘的日子 42 根
- 5 分钟线也存进 silver：之后各周期都从这里重算

---

## 5. 流式合成的设计

**foreachBatch + MERGE**，而不是 Spark 自带的"窗口聚合 + 水位线"：

| | 窗口聚合 + 水位线 | foreachBatch + MERGE（这里用的） |
|---|---|---|
| 多个周期 | 一个查询里多个流式聚合不能串联 / 合并输出 | 一个微批里想算几种算几种 |
| 迟到数据 | 晚于水位线的直接丢掉 | 从已存的 5 分钟线重算受影响的那一天，迟到多久都能改对 |
| 重复数据 | 要另外 dropDuplicatesWithinWatermark | 先按主键去重，再 MERGE |
| 失败重启 | 状态存在检查点里 | foreachBatch 是"至少一次"（会重放上一个微批），按主键 MERGE → 结果等价于"恰好一次" |
| 代价 | 只维护增量状态 | 每个微批要读一遍受影响那几天的 5 分钟线（一只股票一天 78 根，很便宜） |

每个微批：

1. 同一批里重复的消息去掉（按 security_id + bar_start）
2. 这批涉及的（证券, 交易日）当天的全部 5 分钟线 = silver 里已有的 + 新来的（同一根以新来的为准）
3. `check_bars()` 清洗校验 → 5 分钟线（带检查结果）MERGE 进 silver（值或状态变了才更新）
4. `aggregate()` 合成（隔离的不参与）→ MERGE 进 silver

**批流一体**：`check_bars()`、`aggregate()` 都是普通的 DataFrame 函数，流（每个微批）和批（`verify` / `rebuild` 从 bronze 读）调的是同一个，
所以两边结果必须逐行一致，`verify` 就是检查这一点。

Kafka 读到哪了记在检查点 `data/lakehouse/_checkpoints/bars_stream/`；重启后接着读。想从头重放：停掉流任务、删掉检查点和 silver，再启动。

---

## 6. 运行

```bash
docker compose up -d                                         # Kafka、Spark 常驻（Spark 容器自动跑流任务；网页 http://localhost:4040）
docker compose logs -f spark | grep "\[stream\]"            # 看每个微批
python -m pipelines.intraday fetch --from 2026-10-01 --to 2026-10-17     # 手动拉一段 5 分钟线（断点续跑）

# 回放演示（默认 600 倍速：一个交易日约 40 秒）
python -m streaming.replay_producer --duplicate 0.01 --late 0.005       # 注入 1% 重复、0.5% 迟到
python -m streaming.replay_producer --day 2026-10-16 --speedup 0         # 只回放一天，不等待

# ④ 核对（流 vs 批、合成日线 vs golden）；重建 silver
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g spark_jobs/bars.py verify
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g spark_jobs/bars.py rebuild

docker compose stop kafka spark                              # 暂时停掉省内存（每日流程的 sync_intraday 会等不到而失败）
```

手动补某一天（和每日流程一样）：

```python
from datetime import date
from pipelines.intraday import fetch_day
from pipelines.intraday_sync import publish_day, wait_stream, sync_day
d = date(2026, 10, 16)
fetch_day(d); publish_day(d); wait_stream(); sync_day(d)
```

查询（DuckDB）：

```python
from pipelines import lakehouse
con = lakehouse.connect()            # bars_5min、bars 也注册成视图
con.sql("SELECT * FROM bars WHERE symbol = 'AAPL' AND interval = '1h' AND trade_date = '2026-10-16' ORDER BY bar_start")
```

注意：`spark-submit` 时 JVM 已经启动，代码里的 `spark.driver.memory` 不生效，要用 `--driver-memory`。

---

## 7. 结果（2026-10-18 首次跑通，加清洗之前）

回放：568,954 条消息（含故意重复 5,723 条、推迟 3 个时刻再发 2,831 条），19 个微批消费完。

**流 vs 批**：

| 周期 | 行数（流 = 批） | 完整的 | 不一致 |
|---|---|---|---|
| 5min | 563,231 | 563,231 | 0 |
| 15min | 187,841 | 187,623 | 0 |
| 1h | 50,580 | 50,460 | 0 |
| 1d | 7,226 | 7,174 | 0 |

5 分钟线正好等于 bronze 去重后的根数：重复的都去掉了、迟到的都合进去了。

**合成日线 vs golden 日线**（7,174 个完整的交易日）：收盘价差异中位数 0.018%、99% 在 0.19% 以内（5 天超过 1%）；
成交量中位数是官方的 74%。原因：分钟线只有常规时段，最后一根 15:55 的收盘价不是收盘集合竞价价，成交量不含集合竞价、盘前盘后。
所以合成日线只能用来交叉核对，不能替代 golden 日线。

**踩过的坑**

- 比对一开始有 1 行"不一致"：某只股票成交量带小数，浮点数相加的结果和相加顺序有关（流是分批加、批是一次加），末几位不同 → 比对改成相对误差
- 600 倍速回放时每秒约 1,200 条，每个微批两次 MERGE 要 20 多秒，超过 5 秒的触发间隔（Spark 警告 `falling behind`），延迟最多约 70 秒；
  `maxOffsetsPerTrigger` 限制每批最多读 5 万条，积压时不会一口吃太多，回放结束后自然追平。真实行情 5 分钟才来一批，完全跟得上
- Kafka 关了自动建主题：流任务先启动时会因为主题不存在而失败（容器会自动重启），先建主题（回放程序 / `publish_day` 会建）
- Kafka 换了数据卷（主题重建、offset 从 0 开始）→ 删掉流任务的检查点再启动；`failOnDataLoss=false` 让 offset 变小时不整个停掉
- `docker-compose.yml` 里 airflow-init 的 `entrypoint: /bin/bash` 会被其他 Airflow 服务继承（同一个锚点），要显式写回官方入口

---

## 8. 面试要点

- **仓库 vs 湖仓**：日线先进数据库（存算绑定），分钟线存开放格式的湖（Spark 写、DuckDB 读、ClickHouse 只是加速副本）；质量规则和问题记录两边是同一套

- **Kafka 分区和顺序**：只保证分区内有序；用代码做消息键，同一只股票的 K 线进同一个分区。消费端用 min_by / max_by 按时间取开盘收盘，到达顺序乱了也不影响结果
- **至少一次 + 幂等写 = 效果上恰好一次**：生产者开了幂等（网络重试不会在 Kafka 里写重复），消费端按主键 MERGE（重放、重复消息都不会算重）
- **迟到数据**：窗口 + 水位线会丢掉太晚的数据；这里每次从已存的基础数据重算受影响的分区，代价是多读一点数据，换来结果永远正确
- **批流一体 / Lambda 架构**：同一个合成函数，流处理给低延迟结果，批处理从 bronze 重算用来核对和修数据（`rebuild`）；Kappa 架构则只保留流，靠重放 Kafka 修数据
- **背压**：`maxOffsetsPerTrigger` 控制每个微批的量；处理跟不上时延迟上升但不丢数据
- **开放表格式的互通**：bronze 由 Python（delta-rs）写、silver 由 Spark 写，DuckDB 都能直接读
