# Airflow：部署、DAG 说明、常用命令

> 返回 [数据字典](data-dictionary.md)

Airflow 只负责**调度**：每天 / 每周 / 每月按时、按顺序调用 `pipelines/` 里的函数，失败自动重试，在网页上看状态和日志。
业务逻辑都在 `pipelines/`，不经过 Airflow 也能手动跑（见文末"不经过 Airflow 手动跑"）。

---

## 1. 部署

| 项 | 值 |
|---|---|
| 版本 | Airflow **3.3.2**，Python 3.10（和本机 `backtrader-env` 一致） |
| 执行器 | **LocalExecutor**：单机，瓶颈在 FMP 限流不在算力，Celery 会多出 Redis + worker 两个故障点却没有收益；要扩展到多机只改 `AIRFLOW__CORE__EXECUTOR`，DAG 不用改（前提：中间态换成 S3 等共享存储，见 `INTERMEDIATE_URL`） |
| 网页 | **http://localhost:8081**（本机 8080 被 httpd 占用）；本地使用，不需要登录 |
| 镜像 | `data-engine-airflow:3.3.2` = 官方镜像 + 项目依赖，见 [`docker/airflow/Dockerfile`](../docker/airflow/Dockerfile)、[`requirements.txt`](../docker/airflow/requirements.txt)（版本和本机对齐；pip 走清华源） |
| 元数据库 | 项目 postgres 容器里的 **`airflow` 库**（用户 `airflow`），和 `data_engine` 完全分开 |
| 密钥 | `.env` 里的 `AIRFLOW_DB_PASSWORD`、`AIRFLOW_FERNET_KEY`、`AIRFLOW_JWT_SECRET`（`.env` 不进 git） |
| 资源池 | `fmp`，1 个位置：请求 FMP 的任务排队，同一时间只有一个在请求，避免限流。由 `airflow-init` 自动创建 |

[`docker-compose.yml`](../docker-compose.yml) 里的 4 个服务：

| 服务 | 作用 |
|---|---|
| `airflow-init` | 初始化 / 升级元数据库、确保资源池存在；跑完就退出 |
| `airflow-apiserver` | 网页和 API（容器内 8080 → 本机 8081）；任务的心跳也发给它 |
| `airflow-scheduler` | 调度器；LocalExecutor 下任务也在这个容器里运行 |
| `airflow-dag-processor` | 解析 `dags/` 目录（Airflow 3 单独拆出来的服务） |

容器里：项目目录挂到 `/opt/airflow/project`（`PYTHONPATH`），DAG 是项目的 `dags/`，任务日志在 `data/airflow/logs/`。
数据库地址用容器名（`POSTGRES_HOST=postgres:5432`、`CLICKHOUSE_HOST=clickhouse`），覆盖 `.env` 里给本机用的 `localhost:5433`。
中间态 parquet 的路径记成项目相对路径，Windows 本机和容器都能读（以前记的 Windows 绝对路径读取时自动换算）。

### 从零搭建

```bash
# 1. 镜像（镜像站拉不下来时：python tools/pull_image_via_proxy.py apache/airflow:3.3.2-python3.10，经本机代理下载）
docker pull docker.m.daocloud.io/apache/airflow:3.3.2-python3.10
docker tag docker.m.daocloud.io/apache/airflow:3.3.2-python3.10 apache/airflow:3.3.2-python3.10
docker compose build airflow-init

# 2. 元数据库（.env 里先有 AIRFLOW_DB_PASSWORD）
docker exec -i postgres psql -U <POSTGRES_USER> -d postgres -c "CREATE ROLE airflow LOGIN PASSWORD '<AIRFLOW_DB_PASSWORD>'"
docker exec -i postgres psql -U <POSTGRES_USER> -d postgres -c "CREATE DATABASE airflow OWNER airflow"

# 3. 启动（init 会建元数据表和资源池）
docker compose up -d

# 4. 连通性检查：网页上手动触发 healthcheck，或
docker compose exec airflow-scheduler airflow dags test healthcheck
```

---

## 2. DAG 说明

| DAG | 何时运行（北京时间） | 做什么 | 大约用时 |
|---|---|---|---|
| `daily_eod` | 周二到周六 10:00 | 事件 → 证券主表维护 → 回填 security_id，和日线最近 7 天并行 → golden → 汇总提醒 | 25–30 分钟 |
| `weekly_fundamentals` | 周六 10:00 | 三张报表（年报最近 3 期、季报最近 8 期）、比率、企业价值、市值（最近两周）、一致预期、评级 → raw → 清洗 | 约 1 小时 |
| `monthly_reference` | 每月 1 号 12:00 | 公司概况；证券主表 vs SEC 代码表核对；交易日历更新 + FMP 节假日核对 | 约 10 分钟 |
| `healthcheck` | 不定时，手动 | 容器里能否导入项目代码、连上 PostgreSQL / ClickHouse / FMP、读中间态 | 1 分钟 |

### 日期怎么定
- **日期一律由 Airflow 的执行时间换算**，代码里不用"今天"，所以补跑过去某一天和当天跑的结果一样
- `daily_eod`：执行时间换成**纽约时间的日期** = 要处理的美股交易日（北京时间早上 10 点 = 纽约前一天晚上 10 点（夏令时）/ 9 点（冬令时），已收盘）；不是交易日（周末、美股假日）→ 第一步返回 False，后面全部跳过
- 每周 / 每月：执行时间的北京日期
- 手动指定日期：触发时传 `{"trade_date": "2026-10-13"}`（每日）或 `{"run_date": "2026-10-17"}`（每周 / 每月）

### 幂等与重试
每个任务都可以安全地重跑：日期确定；拉取按（接口, 参数）断点续跑，已完成的股票跳过；事件按（来源, 事件键, 内容哈希）去重；
清洗按批次在一个事务里整体替换；golden 先比对、有变化才写新版本。所以：
- 任务失败**自动重试 4 次**，间隔逐次翻倍（10、20、40、60 分钟，`retry_exponential_backoff`），能扛过两个多小时的断网；重试只会补做没完成的部分
- 拉取时个别股票因网络 / 限流 / 服务端错误失败（`timeout` 等暂时性错误），任务也会失败、触发重试，断点续跑只补拉这些股票
- 每日流程分两条线并行：事件（load_events → maintain_securities → backfill_security_id）和日线（ingest_prices）；一条失败不挡另一条，load_golden 和汇总等两条线都结束再跑（`all_done`）。事件来源也逐个试，一个连不上不影响其他
- 汇总只统计本流程的接口（`DAILY_ENDPOINTS` / `WEEKLY_ENDPOINTS` / `MONTHLY_ENDPOINTS`），别的流程的失败批次不会把它标红
- `max_active_runs=1`：同一个 DAG 同一时间只跑一次，补跑时按日期顺序一天一天来
- 每周 / 每月的参数里带 `as_of`（运行日期）：只用来区分"哪一次拉取"，不出现在请求里——不加的话第二周会被当成"已经拉过"全部跳过
- 每周流程里各数据集互不依赖：一个失败不影响后面的（`trigger_rule=all_done`）

### 提醒（[`pipelines/alerts.py`](../pipelines/alerts.py)）

每个 DAG 最后一步 `summarize_and_alert` 汇总这次运行的批次、问题、隔离、事件修订，按规则提醒：

| 级别 | 情况 | 效果 |
|---|---|---|
| error | 有拉取批次失败；任何任务重试用完最终失败（`on_failure_callback`） | 记录；汇总任务**标红** |
| warning | 新增 danger 问题（数据被隔离）；事件修订 > 20 条（正常接近 0）；和 FMP 复权价核对的问题比上次多；月度核对发现主表和 SEC 不一致、交易日历和 FMP 不一致 | 只记录 |

记录位置：`data/alerts/alerts.jsonl`（每条一行 JSON）、调度器日志；每次的汇总在 `data/alerts/summaries.jsonl`（用来和上一次比较）。
以后接邮件 / Slack 只改 `notify()`。

---

## 3. 常用命令

```bash
docker compose up -d                                   # 启动全部（含 Airflow）
docker compose stop airflow-scheduler                  # 暂停调度（不会再自动跑；网页还能看）
docker compose logs -f airflow-scheduler               # 调度器日志（任务也在这个容器里跑）

# 以下在容器里执行：docker compose exec airflow-scheduler <命令>
airflow dags list                                      # 有哪些 DAG、是否暂停
airflow dags list-import-errors                        # DAG 文件有没有导入错误
airflow dags pause daily_eod / unpause daily_eod       # 暂停 / 恢复自动调度
airflow dags trigger daily_eod --conf '{"trade_date": "2026-10-13"}'     # 手动跑某一天
airflow dags list-runs daily_eod                       # 运行记录
airflow tasks states-for-dag-run daily_eod <run_id>    # 某次运行每个任务的状态
airflow dags test daily_eod 2026-10-18T00:00:00+00:00  # 不经调度器、在当前进程里跑一遍（调试用）

# 补跑一段日期（执行时间区间；非交易日自动跳过）
airflow backfill create --dag-id daily_eod --from-date 2026-10-06 --to-date 2026-10-10 --dry-run   # 先看会跑哪几天
airflow backfill create --dag-id daily_eod --from-date 2026-10-06 --to-date 2026-10-10

# 重跑某次运行里失败的任务（及下游）
airflow tasks clear daily_eod -t ingest_prices --start-date 2026-10-15 --end-date 2026-10-15 --downstream
```

任务日志：网页上点任务看；文件在 `data/airflow/logs/dag_id=<DAG>/run_id=<运行>/task_id=<任务>/attempt=<第几次>.log`。

### 不经过 Airflow 手动跑
```bash
python -m pipelines.daily --date 2026-10-13                          # 每日流程的某一天（全部步骤）
python -m pipelines.daily --date 2026-10-13 --steps load_golden      # 只跑某几步
```

---

## 4. 排障记录

| 现象 | 原因 | 处理 |
|---|---|---|
| 任务跑到一半被结束，日志没有报错；调度器日志 `Invalid auth token`、`Too many failed heartbeats` | **电脑睡眠**：心跳中断、认证令牌（600 秒）过期，醒来后被判定失联、强制结束 | 会自动重试，且只补做没完成的部分；根本解决：接通电源时不睡眠 `powercfg /change standby-timeout-ac 0` |
| 早上 10 点的任务没跑 | 电脑在睡眠，调度器也停了 | 醒来后会补跑最近一次错过的；同上，关闭睡眠 |
| 手动触发的运行一直 queued | `max_active_runs=1`，前面还有一次在跑（比如取消暂停时自动补的那次） | 正常，等前一次跑完 |
| 取消暂停后马上自动跑了一次 | 即使 `catchup=False`，也会补跑最近一个错过的时间点 | 正常 |
| 2026-10-17 三个流程都失败 | 电脑醒得晚，几个流程同时开跑时正赶上断网（约 1 小时）：重试 2 次很快用完；断点续跑碰到空结果崩溃；拉取失败的股票没让任务失败；事件失败挡住了日线；月度汇总把每周的失败批次也算了进去 | 全部修复（见上面"幂等与重试"）；断网恢复后 `airflow tasks clear <dag> -s <日期> -e <次日> --only-failed --downstream --yes` 重跑失败的任务 |
| 拉镜像 502 / 连接中断 | 镜像站问题 | `python tools/pull_image_via_proxy.py <镜像>`（经本机代理直连 Docker Hub） |
| 配置 CSV 读不了（不是 UTF-8） | 在 Excel 里打开后被保存过（按 GBK 读、存回去中文变 `?`、日期变 `28/04/2011`） | 程序会直接报出原因；用 VS Code 编辑，或 Excel 里另存为「CSV UTF-8」 |
