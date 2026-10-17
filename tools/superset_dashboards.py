"""
用代码建 Superset 的数据库连接、数据集、图表、看板（看板即代码：定义在 git 里，换环境重跑一遍就能重建）

可以重复执行：按名称查找，已存在就更新，不会重复创建。
每张图表建好后调用 Superset 的数据接口实际查询一次，确认能出数据。

用法（Superset 在 http://localhost:8088 运行时）：
    python tools/superset_dashboards.py
"""
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE = os.getenv("SUPERSET_URL", "http://localhost:8088")
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
ENV = os.environ

# =========================
# 数据库连接（只读账号；容器之间用容器名）
# =========================
DATABASES = {
    "数据仓库 PostgreSQL": f"postgresql+psycopg2://superset_ro:{ENV['SUPERSET_RO_PASSWORD']}@postgres:5432/{ENV.get('POSTGRES_DB', 'data_engine')}",
    "Airflow 元数据库": f"postgresql+psycopg2://superset_ro:{ENV['SUPERSET_RO_PASSWORD']}@postgres:5432/airflow",
    "ClickHouse": f"clickhousedb://superset_ro:{ENV['SUPERSET_RO_PASSWORD']}@clickhouse:8123/golden",
}

# =========================
# 数据集（虚拟数据集：一段 SQL；不建新表）
# =========================
DATASETS = {
    "质量问题": ("数据仓库 PostgreSQL", """
        SELECT q.rule_id, q.severity, coalesce(r.description, q.rule_id) AS rule_description, i.endpoint,
               q.symbol, q.date, q.detected_at, i.started_at AS run_started_at
        FROM meta.quality_issues q
        JOIN meta.ingestion i USING (batch_id)
        LEFT JOIN meta.quality_rules r ON r.rule_id = q.rule_id AND r.rule_version = q.rule_version"""),
    "隔离区": ("数据仓库 PostgreSQL", """
        SELECT endpoint, status, symbol, date, quarantined_at, resolved_at FROM meta.quarantine_records"""),
    "与 FMP 复权价不一致": ("数据仓库 PostgreSQL", """
        SELECT symbol, date,
               round(((detail->>'r_ours')::numeric) * 100, 2)        AS "我们的收益率 %",
               round(((detail->>'r_fmp')::numeric) * 100, 2)         AS "FMP 收益率 %",
               round(((detail->>'r_unadjusted')::numeric) * 100, 2)  AS "不复权收益率 %",
               round(abs((detail->>'r_ours')::numeric - (detail->>'r_fmp')::numeric) * 100, 2) AS "相差 %"
        FROM meta.quality_issues WHERE rule_id = 'GD001'"""),
    "拉取批次": ("数据仓库 PostgreSQL", """
        SELECT batch_id, endpoint, status, started_at, finished_at,
               round(extract(epoch FROM finished_at - started_at)::numeric / 60, 1) AS minutes,
               n_symbols, n_success, n_missing, n_failed, rows_fetched
        FROM meta.ingestion"""),
    "写入记录": ("数据仓库 PostgreSQL", """
        SELECT l.batch_id, i.endpoint, l.layer, l.target_table, l.rows_written, l.status, l.loaded_at
        FROM meta.load_log l JOIN meta.ingestion i USING (batch_id)"""),
    "DAG 运行": ("Airflow 元数据库", """
        SELECT dag_id, run_id, run_type, state, start_date, end_date,
               round(extract(epoch FROM end_date - start_date)::numeric / 60, 1) AS minutes
        FROM dag_run WHERE start_date IS NOT NULL"""),
    "任务运行": ("Airflow 元数据库", """
        SELECT dag_id, task_id, run_id, state, try_number, start_date, end_date,
               round((duration / 60)::numeric, 1) AS minutes
        FROM task_instance WHERE start_date IS NOT NULL"""),
    "golden 数据新鲜度": ("ClickHouse", """
        SELECT s.symbol AS symbol, s.name AS name, s.sector AS sector, p.last_date AS last_date, p.n AS rows,
               dateDiff('day', p.last_date, (SELECT max(date) FROM golden.v_price_daily)) AS days_behind
        FROM golden.securities AS s
        LEFT JOIN (SELECT security_id, max(date) AS last_date, count() AS n FROM golden.v_price_daily GROUP BY security_id) AS p
            ON s.security_id = p.security_id
        WHERE s.status = 'active'"""),
    "golden 每日覆盖": ("ClickHouse", """
        SELECT toDateTime(date) AS trade_date, count() AS securities
        FROM golden.v_price_daily WHERE date >= today() - 180 GROUP BY date"""),
}

COUNT = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "数量"}


def sql_metric(expr: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


def where(expr: str) -> list[dict]:
    return [{"expressionType": "SQL", "sqlExpression": expr, "clause": "WHERE"}]


# =========================
# 图表：名称 → (数据集, 图表类型, 参数)；查询验证用的列 / 指标另外写
# =========================
CHARTS = {
    # ---- 数据质量 ----
    "待处理的隔离数据": ("隔离区", "big_number_total",
                  {"metric": COUNT, "adhoc_filters": where("status = 'pending'"), "subheader": "行（danger 规则拦下、等人工核对）"}),
    "质量问题总数": ("质量问题", "big_number_total", {"metric": COUNT, "subheader": "条（warning + danger）"}),
    "与 FMP 复权价不一致": ("与 FMP 复权价不一致", "big_number_total",
                     {"metric": COUNT, "subheader": "天（每日收益率相差 > 2%，多为分拆 / 特殊事件）"}),
    "问题数（按规则）": ("质量问题", "echarts_timeseries_bar",
                  {"x_axis": "rule_id", "metrics": [COUNT], "groupby": ["severity"], "stack": "Stack",
                   "x_axis_sort": "数量", "x_axis_sort_asc": False, "row_limit": 1000, "show_legend": True}),
    "隔离数据（按接口）": ("隔离区", "pie",
                   {"groupby": ["endpoint"], "metric": COUNT, "adhoc_filters": where("status = 'pending'"),
                    "show_labels": True, "label_type": "key_value", "show_legend": True}),
    "每天有日线的证券数（近 180 天）": ("golden 每日覆盖", "echarts_timeseries_line",
                            {"x_axis": "trade_date", "time_grain_sqla": "P1D",
                             "metrics": [sql_metric("MAX(securities)", "证券数")], "row_limit": 10000}),
    "与 FMP 不一致明细": ("与 FMP 复权价不一致", "table",
                   {"query_mode": "raw", "all_columns": ["symbol", "date", "我们的收益率 %", "FMP 收益率 %", "不复权收益率 %", "相差 %"],
                    "order_by_cols": [json.dumps(["相差 %", False])], "row_limit": 200}),
    "日线落后的证券": ("golden 数据新鲜度", "table",
                {"query_mode": "raw", "all_columns": ["symbol", "name", "sector", "last_date", "days_behind"],
                 "adhoc_filters": where("days_behind > 0 OR last_date IS NULL"),
                 "order_by_cols": [json.dumps(["days_behind", False])], "row_limit": 200}),
    # ---- 管线运行 ----
    "拉取批次数": ("拉取批次", "big_number_total", {"metric": COUNT, "subheader": "个批次（全部）"}),
    "失败 / 部分成功的批次": ("拉取批次", "big_number_total",
                     {"metric": COUNT, "adhoc_filters": where("status <> 'success'"), "subheader": "个批次"}),
    "每天拉取行数（按接口）": ("拉取批次", "echarts_timeseries_bar",
                     {"x_axis": "started_at", "time_grain_sqla": "P1D", "metrics": [sql_metric("SUM(rows_fetched)", "行数")],
                      "groupby": ["endpoint"], "stack": "Stack", "row_limit": 10000, "show_legend": True}),
    "每天批次数（按状态）": ("拉取批次", "echarts_timeseries_bar",
                    {"x_axis": "started_at", "time_grain_sqla": "P1D", "metrics": [COUNT], "groupby": ["status"],
                     "stack": "Stack", "row_limit": 10000, "show_legend": True}),
    "流程运行时长（分钟）": ("DAG 运行", "echarts_timeseries_bar",
                    {"x_axis": "start_date", "time_grain_sqla": "P1D", "metrics": [sql_metric("MAX(minutes)", "分钟")],
                     "groupby": ["dag_id"], "adhoc_filters": where("state = 'success'"), "row_limit": 10000, "show_legend": True}),
    "每日流程各步骤平均用时（分钟）": ("任务运行", "echarts_timeseries_bar",
                         {"x_axis": "task_id", "metrics": [sql_metric("AVG(minutes)", "平均分钟")],
                          "adhoc_filters": where("dag_id = 'daily_eod' AND state = 'success'"),
                          "x_axis_sort": "平均分钟", "x_axis_sort_asc": False, "row_limit": 100}),
    "最近的流程运行": ("DAG 运行", "table",
                {"query_mode": "raw", "all_columns": ["dag_id", "run_type", "state", "start_date", "minutes"],
                 "order_by_cols": [json.dumps(["start_date", False])], "row_limit": 30}),
    "最近失败 / 部分成功的批次": ("拉取批次", "table",
                       {"query_mode": "raw",
                        "all_columns": ["started_at", "endpoint", "status", "n_symbols", "n_failed", "n_missing", "rows_fetched"],
                        "adhoc_filters": where("status <> 'success'"),
                        "order_by_cols": [json.dumps(["started_at", False])], "row_limit": 50}),
}

DASHBOARDS = {
    "数据质量看板": ("data-quality", [
        ["待处理的隔离数据", "质量问题总数", "与 FMP 复权价不一致"],
        ["问题数（按规则）", "隔离数据（按接口）"],
        ["每天有日线的证券数（近 180 天）"],
        ["与 FMP 不一致明细", "日线落后的证券"],
    ]),
    "管线运行看板": ("pipeline-runs", [
        ["拉取批次数", "失败 / 部分成功的批次"],
        ["每天拉取行数（按接口）", "每天批次数（按状态）"],
        ["流程运行时长（分钟）", "每日流程各步骤平均用时（分钟）"],
        ["最近的流程运行", "最近失败 / 部分成功的批次"],
    ]),
}


# =========================
# Superset API
# =========================
class Superset:
    def __init__(self, base: str, user: str, password: str):
        self.base = base.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(f"{self.base}/api/v1/security/login",
                        json={"username": user, "password": password, "provider": "db", "refresh": True}, timeout=30)
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{self.base}/api/v1/security/csrf_token/", timeout=30).json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": self.base})

    def _call(self, method, path, **kw):
        r = self.s.request(method, f"{self.base}{path}", timeout=120, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:500]}")
        return r.json() if r.text else {}

    def find(self, resource: str, field: str, value: str) -> int | None:
        page = 0
        while True:
            res = self._call("GET", f"/api/v1/{resource}/", params={"q": f"(page:{page},page_size:100)"})["result"]
            for item in res:
                if item.get(field) == value:
                    return item["id"]
            if len(res) < 100:
                return None
            page += 1

    def upsert(self, resource: str, field: str, value: str, body: dict) -> int:
        oid = self.find(resource, field, value)
        if oid is None:
            return self._call("POST", f"/api/v1/{resource}/", json=body)["id"]
        self._call("PUT", f"/api/v1/{resource}/{oid}", json=body)
        return oid


def query_object(viz: str, fd: dict) -> dict:
    """验证用的查询（和图表实际渲染的查询大体一致）"""
    q = {"filters": [], "extras": {}, "row_limit": fd.get("row_limit", 1000)}
    wheres = [f["sqlExpression"] for f in fd.get("adhoc_filters", [])]
    if wheres:
        q["extras"]["where"] = " AND ".join(f"({w})" for w in wheres)
    if viz in ("big_number_total", "pie"):
        q["metrics"] = [fd["metric"]]
        q["columns"] = fd.get("groupby", [])
    elif viz == "table":
        q["columns"] = fd["all_columns"]
        q["orderby"] = [json.loads(o) for o in fd.get("order_by_cols", [])]
    else:
        q["metrics"] = fd["metrics"]
        q["columns"] = [fd["x_axis"], *fd.get("groupby", [])]
    return q


def main():
    ss = Superset(BASE, "admin", ENV["SUPERSET_ADMIN_PASSWORD"])
    db_ids = {name: ss.upsert("database", "database_name", name,
                              {"database_name": name, "sqlalchemy_uri": uri, "expose_in_sqllab": True})
              for name, uri in DATABASES.items()}
    print("数据库连接：", db_ids)

    ds_ids = {}
    for name, (db, sql) in DATASETS.items():
        body = {"database": db_ids[db], "table_name": name, "sql": sql.strip()}
        oid = ss.find("dataset", "table_name", name)
        if oid is None:
            oid = ss._call("POST", "/api/v1/dataset/", json=body)["id"]
        else:
            ss._call("PUT", f"/api/v1/dataset/{oid}", json={"sql": sql.strip()})
        ss._call("PUT", f"/api/v1/dataset/{oid}/refresh")                 # 重新读取列（含时间列识别）
        ds_ids[name] = oid
    print("数据集：", len(ds_ids), "个")

    dash_ids = {title: ss.upsert("dashboard", "dashboard_title", title,
                                 {"dashboard_title": title, "slug": slug, "published": True})
                for title, (slug, _) in DASHBOARDS.items()}
    dash_of = {c: dash_ids[t] for t, (_, rows) in DASHBOARDS.items() for row in rows for c in row}

    chart_ids, failures = {}, []
    for name, (ds, viz, fd) in CHARTS.items():
        form = {"viz_type": viz, "datasource": f"{ds_ids[ds]}__table", **fd}
        chart_ids[name] = ss.upsert("chart", "slice_name", name, {
            "slice_name": name, "viz_type": viz, "datasource_id": ds_ids[ds], "datasource_type": "table",
            "params": json.dumps(form, ensure_ascii=False), "dashboards": [dash_of[name]]})
        try:                                                              # 实际查询一次
            data = ss._call("POST", "/api/v1/chart/data", json={
                "datasource": {"id": ds_ids[ds], "type": "table"}, "force": True,
                "queries": [query_object(viz, fd)], "result_format": "json", "result_type": "full"})
            n = data["result"][0]["rowcount"]
            print(f"  ✅ {name}：{n} 行")
        except RuntimeError as e:
            failures.append(name)
            print(f"  ❌ {name}：{str(e)[:300]}")

    for title, (slug, rows) in DASHBOARDS.items():
        pos = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
               "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
               "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}}}
        for i, row in enumerate(rows):
            rid = f"ROW-{i}"
            pos["GRID_ID"]["children"].append(rid)
            width = 12 // len(row)
            height = 25 if all(CHARTS[c][1] == "big_number_total" for c in row) else 60
            pos[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                        "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            for c in row:
                cid = f"CHART-{chart_ids[c]}"
                pos[rid]["children"].append(cid)
                pos[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                            "meta": {"chartId": chart_ids[c], "width": width, "height": height, "sliceName": c}}
        ss._call("PUT", f"/api/v1/dashboard/{dash_ids[title]}", json={
            "position_json": json.dumps(pos, ensure_ascii=False),
            "json_metadata": json.dumps({"refresh_frequency": 0, "color_scheme": "supersetColors"})})
        print(f"看板「{title}」：{BASE}/superset/dashboard/{slug}/")
    if failures:
        sys.exit(f"{len(failures)} 张图表查询失败：{failures}")


if __name__ == "__main__":
    main()
