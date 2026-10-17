# Superset 看板

> 返回 [数据字典](data-dictionary.md)

## 访问
- 网页：**http://localhost:8088**，管理员 `admin`，密码见 `.env` 的 `SUPERSET_ADMIN_PASSWORD`；界面默认中文
- 数据质量看板：http://localhost:8088/superset/dashboard/data-quality/
- 管线运行看板：http://localhost:8088/superset/dashboard/pipeline-runs/

## 部署

| 项 | 值 |
|---|---|
| 版本 | Superset **6.1.0** |
| 镜像 | `data-engine-superset:6.1.0` = 官方镜像 + PostgreSQL / ClickHouse 驱动，见 [`docker/superset/Dockerfile`](../docker/superset/Dockerfile) |
| 配置 | [`docker/superset/superset_config.py`](../docker/superset/superset_config.py)（挂进容器）；启动脚本 [`bootstrap.sh`](../docker/superset/bootstrap.sh)：升级元数据库 → 确保管理员存在 → 初始化 → 启动网页，都可以重复执行 |
| 元数据库 | postgres 容器里的 **`superset` 库**（用户 `superset`），存看板和图表定义 |
| 连数据用的账号 | **只读账号 `superset_ro`**（Superset 的 SQL Lab 里可以随手写 SQL，不能给它写权限）：PostgreSQL 的 `meta` / `ref` / `events`（以后新建的表自动有查询权限）、`airflow` 库的 `dag` / `dag_run` / `task_instance`；ClickHouse 的 `raw` / `golden`（`readonly = 1`） |
| 密钥 | `.env` 的 `SUPERSET_SECRET_KEY`、`SUPERSET_DB_PASSWORD`、`SUPERSET_ADMIN_PASSWORD`、`SUPERSET_RO_PASSWORD` |

## 看板即代码

数据库连接、数据集、图表、看板都由 [`tools/superset_dashboards.py`](../tools/superset_dashboards.py) 用 Superset 的 API 创建：
定义在 git 里，换环境（如上云）重跑一遍就能重建；可以重复执行（按名称查找，已存在就更新）；
每张图表建好后实际查询一次，确认能出数据。

```bash
python tools/superset_dashboards.py
```

在网页上手动改过的图表，下次运行脚本会被脚本里的定义覆盖——要长期保留的改动，改脚本。

### 数据集（虚拟数据集：一段 SQL，不建新表）

| 数据集 | 来源 | 内容 |
|---|---|---|
| 质量问题 | PostgreSQL `meta` | 每条问题 + 规则描述 + 接口 |
| 隔离区 | PostgreSQL `meta` | 隔离数据及状态 |
| 与 FMP 复权价不一致 | PostgreSQL `meta` | GD001 的明细：我们的 / FMP 的 / 不复权的收益率 |
| 拉取批次 | PostgreSQL `meta` | 每个批次的状态、用时、股票数、行数 |
| 写入记录 | PostgreSQL `meta` | 每次写 raw / 隔离区 / 中间态 / golden 的行数 |
| DAG 运行、任务运行 | Airflow 元数据库 | 每次流程运行和每个任务的状态、用时 |
| golden 数据新鲜度 | ClickHouse | 仍在交易的证券，golden 日线最后一天和最新交易日差几天 |
| golden 每日覆盖 | ClickHouse | 近 180 天每天有日线的证券数（突然下降说明缺数据） |

### 看板

| 看板 | 图表 |
|---|---|
| 数据质量 | 待处理的隔离数据、质量问题总数、与 FMP 不一致的天数；问题数（按规则、按级别堆叠）；隔离数据（按接口）；每天有日线的证券数；与 FMP 不一致明细；日线落后的证券 |
| 管线运行 | 批次数、失败 / 部分成功的批次；每天拉取行数（按接口）；每天批次数（按状态）；流程运行时长；每日流程各步骤平均用时；最近的流程运行；最近失败 / 部分成功的批次 |
