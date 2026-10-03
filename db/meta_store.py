"""
MetaStore：数据治理工具表（PostgreSQL meta schema）的读写

按"业务动作"提供方法，而不是按表提供增删改查；需要改多张表的动作在方法内部用一个事务完成。

用法：
    store = MetaStore()

    # 主要用法：with 管理一次拉取的生命周期
    with store.ingestion_run("price_volume", symbols, params) as run:
        df, summary, succeed, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))

    # 简便写法：装饰器
    @track_ingestion("price_volume")
    def fetch_price_volume(symbols, from_date, to_date): ...
"""
import functools
import inspect
import json
import logging
import secrets
import subprocess
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from config.paths import BASE_DIR
from db.connections import get_pg

log = logging.getLogger(__name__)


# =========================
# 工具函数
# =========================
def _json_default(obj):
    """让 numpy / pandas / 日期类型可以被 json 序列化"""
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if isinstance(obj, np.generic):
        return obj.item()
    if obj is pd.NaT:
        return None
    return str(obj)


def _clean_nan(obj):
    """NaN 不是合法 JSON，转成 None"""
    if isinstance(obj, dict):
        return {k: _clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nan(v) for v in obj]
    if isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


def _jsonb(obj) -> Jsonb:
    return Jsonb(_clean_nan(obj), dumps=lambda o: json.dumps(o, default=_json_default, ensure_ascii=False))


def _to_date(value) -> date | None:
    if value is None or (isinstance(value, float) and np.isnan(value)) or value is pd.NaT:
        return None
    return pd.Timestamp(value).date()


def new_batch_id(endpoint: str) -> str:
    """可读批次号：{endpoint}_{YYYYmmdd_HHMMSS}_{4位随机码}，时间为 UTC"""
    return f"{endpoint}_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{secrets.token_hex(2)}"


@functools.lru_cache(maxsize=1)
def current_git_version() -> str | None:
    """当前代码的 git commit；有未提交的改动时加 -dirty"""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=BASE_DIR,
            capture_output=True, text=True, check=True, timeout=5,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], cwd=BASE_DIR,
            capture_output=True, text=True, check=True, timeout=5,
        ).stdout.strip()
        return f"{commit}-dirty" if dirty else commit
    except (OSError, subprocess.SubprocessError):
        return None


# =========================
# 一次拉取的生命周期
# =========================
class IngestionRun:
    """
    进入 with：插入一行 status='running'
    run.finish(...)：回填计数、列表，并按失败数判定 success / partial / failed
    退出 with：
        - 正常退出：写入 finish() 的结果
        - 出异常：记为 failed，异常继续向外抛出
        - 没调用 finish()：记为 failed（视为流程不完整）
    """

    def __init__(self, store: "MetaStore", endpoint: str, symbols: list[str], params: dict | None = None):
        self.store = store
        self.endpoint = endpoint
        self.symbols = list(symbols)
        self.params = params or {}
        self.batch_id = new_batch_id(endpoint)
        self._result: dict | None = None
        self.report: str | None = None

    def __enter__(self) -> "IngestionRun":
        self.store.conn.execute(
            """
            INSERT INTO meta.ingestion (batch_id, endpoint, params, n_ticker, git_version)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (self.batch_id, self.endpoint, _jsonb(self.params), len(self.symbols), current_git_version()),
        )
        log.info(f"[{self.endpoint}] 批次开始 {self.batch_id}，{len(self.symbols)} 只股票")
        return self

    def finish(self, summary: dict, failed_list: list[dict], rows_fetched: int = 0, report: str | None = None):
        """
        参数直接用 FetchData.fetch_fmp_batch() 的返回值：
            summary      -> success / missing / failed / missing_symbols
            failed_list  -> 每项至少有 symbol
        """
        n_failed = summary.get("failed", len(failed_list))
        if n_failed == 0:
            status = "success"
        elif n_failed >= len(self.symbols):
            status = "failed"
        else:
            status = "partial"

        self._result = {
            "n_success": summary.get("success", 0),
            "n_missing": summary.get("missing", 0),
            "n_failed": n_failed,
            "failed_tickers": [f["symbol"] for f in failed_list],
            "missing_tickers": list(summary.get("missing_symbols", [])),
            "failure_detail": failed_list,
            "rows_fetched": rows_fetched,
            "status": status,
        }
        if report is not None:
            self.report = report

    def set_report(self, report_path):
        """错误报告生成后回填路径（可在 finish 之后调用）"""
        self.report = str(report_path) if report_path is not None else None

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            result = {
                "n_success": 0, "n_missing": 0, "n_failed": 0,
                "failed_tickers": [], "missing_tickers": [], "rows_fetched": 0,
                **(self._result or {}),
                "status": "failed",
            }
            result["failure_detail"] = list(result.get("failure_detail", [])) + [
                {"error_type": "exception", "error": f"{exc_type.__name__}: {exc}"}
            ]
            log.error(f"[{self.endpoint}] 批次 {self.batch_id} 中途报错：{exc_type.__name__}: {exc}")
        elif self._result is None:
            result = {
                "n_success": 0, "n_missing": 0, "n_failed": 0,
                "failed_tickers": [], "missing_tickers": [], "rows_fetched": 0,
                "status": "failed",
                "failure_detail": [{"error_type": "not_finished", "error": "退出 with 前没有调用 run.finish()"}],
            }
            log.error(f"[{self.endpoint}] 批次 {self.batch_id} 没有调用 finish()，记为 failed")
        else:
            result = self._result

        self.store.conn.execute(
            """
            UPDATE meta.ingestion
            SET n_success = %(n_success)s, n_missing = %(n_missing)s, n_failed = %(n_failed)s,
                failed_tickers = %(failed_tickers)s, missing_tickers = %(missing_tickers)s,
                failure_detail = %(failure_detail)s, rows_fetched = %(rows_fetched)s,
                status = %(status)s, finished_at = now(), report = %(report)s
            WHERE batch_id = %(batch_id)s
            """,
            {**result, "failure_detail": _jsonb(result["failure_detail"]),
             "report": self.report, "batch_id": self.batch_id},
        )
        log.info(f"[{self.endpoint}] 批次结束 {self.batch_id}：{result['status']}")
        return False  # 不吞异常


# =========================
# MetaStore
# =========================
class MetaStore:

    def __init__(self, conn: psycopg.Connection | None = None):
        self.conn = conn or get_pg()

    # ---------- ingestion ----------
    def ingestion_run(self, endpoint: str, symbols: list[str], params: dict | None = None) -> IngestionRun:
        return IngestionRun(self, endpoint, symbols, params)

    def get_ingestion(self, batch_id: str) -> dict | None:
        with self.conn.cursor(row_factory=dict_row) as cur:
            return cur.execute("SELECT * FROM meta.ingestion WHERE batch_id = %s", (batch_id,)).fetchone()

    # ---------- quality_rules ----------
    def active_rules(self, endpoint: str) -> dict[str, dict]:
        """当前生效的规则：{rule_id: {rule_version, severity, params, description}}"""
        with self.conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                """
                SELECT rule_id, rule_version, severity, params, description
                FROM meta.quality_rules
                WHERE endpoint = %s AND is_active
                ORDER BY rule_id
                """,
                (endpoint,),
            ).fetchall()
        return {r.pop("rule_id"): r for r in rows}

    # ---------- quality_issues ----------
    def record_issues(self, batch_id: str, issues: list[dict]) -> int:
        """
        批量写入质量问题，同一批次重复的问题自动跳过。返回实际新增的行数。
        每项需要：rule_id, rule_version, severity, ticker, date（可为空）, detail（可选）
        """
        if not issues:
            return 0
        rows = [
            (batch_id, i["rule_id"], i["rule_version"], i["severity"], i["ticker"],
             _to_date(i.get("date")), _jsonb(i.get("detail", {})))
            for i in issues
        ]
        with self.conn.transaction(), self.conn.cursor() as cur:
            before = cur.execute("SELECT count(*) FROM meta.quality_issues WHERE batch_id = %s", (batch_id,)).fetchone()[0]
            cur.executemany(
                """
                INSERT INTO meta.quality_issues (batch_id, rule_id, rule_version, severity, ticker, date, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            after = cur.execute("SELECT count(*) FROM meta.quality_issues WHERE batch_id = %s", (batch_id,)).fetchone()[0]
        return after - before

    # ---------- quarantine_records ----------
    def quarantine(self, batch_id: str, endpoint: str, records: list[dict]) -> int:
        """
        把 danger 数据放进隔离区，同一条数据重复隔离自动跳过。返回实际新增的行数。
        每项需要：ticker, date, record（整行原始数据 dict）
        """
        if not records:
            return 0
        rows = [(batch_id, endpoint, r["ticker"], _to_date(r["date"]), _jsonb(r["record"])) for r in records]
        with self.conn.transaction(), self.conn.cursor() as cur:
            before = cur.execute("SELECT count(*) FROM meta.quarantine_records WHERE batch_id = %s", (batch_id,)).fetchone()[0]
            cur.executemany(
                """
                INSERT INTO meta.quarantine_records (batch_id, endpoint, ticker, date, record)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            after = cur.execute("SELECT count(*) FROM meta.quarantine_records WHERE batch_id = %s", (batch_id,)).fetchone()[0]
        return after - before

    def release(self, quarantine_id: int, reason: str, decided_by: str) -> int:
        """放行一条隔离数据：一个事务里改隔离状态 + 写处理记录。返回 correction_id。"""
        return self._resolve(quarantine_id, "released", "release", reason, decided_by)

    def discard(self, quarantine_id: int, reason: str, decided_by: str) -> int:
        """丢弃一条隔离数据：一个事务里改隔离状态 + 写处理记录。返回 correction_id。"""
        return self._resolve(quarantine_id, "discarded", "discard", reason, decided_by)

    def _resolve(self, quarantine_id: int, new_status: str, action: str, reason: str, decided_by: str) -> int:
        with self.conn.transaction(), self.conn.cursor() as cur:
            # 先改状态：只有 pending 能被处理，避免重复放行 / 放行已丢弃的数据
            updated = cur.execute(
                """
                UPDATE meta.quarantine_records
                SET status = %s, resolved_at = now()
                WHERE quarantine_id = %s AND status = 'pending'
                """,
                (new_status, quarantine_id),
            ).rowcount
            if updated == 0:
                raise ValueError(f"quarantine_id={quarantine_id} 不存在或不是 pending 状态，无法 {action}")
            correction_id = cur.execute(
                """
                INSERT INTO meta.data_corrections (quarantine_id, action, reason, decided_by)
                VALUES (%s, %s, %s, %s)
                RETURNING correction_id
                """,
                (quarantine_id, action, reason, decided_by),
            ).fetchone()[0]
        log.info(f"quarantine_id={quarantine_id} 已 {action}（correction_id={correction_id}）")
        return correction_id

    # ---------- outbox ----------
    def pending_outbox(self, endpoint: str | None = None, limit: int = 1000) -> list[dict]:
        """已放行、但还没写入 golden 的隔离数据"""
        sql = """
            SELECT quarantine_id, batch_id, endpoint, ticker, date, record
            FROM meta.quarantine_records
            WHERE status = 'released' AND golden_written_at IS NULL
        """
        params: list = []
        if endpoint is not None:
            sql += " AND endpoint = %s"
            params.append(endpoint)
        sql += " ORDER BY resolved_at LIMIT %s"
        params.append(limit)
        with self.conn.cursor(row_factory=dict_row) as cur:
            return cur.execute(sql, params).fetchall()

    def mark_written(self, quarantine_ids: list[int]) -> int:
        """写入 golden 成功后回填 golden_written_at。返回更新行数。"""
        if not quarantine_ids:
            return 0
        return self.conn.execute(
            """
            UPDATE meta.quarantine_records
            SET golden_written_at = now()
            WHERE quarantine_id = ANY(%s) AND status = 'released' AND golden_written_at IS NULL
            """,
            (list(quarantine_ids),),
        ).rowcount


# =========================
# 装饰器：ingestion_run 的简便写法
# =========================
def track_ingestion(endpoint: str, store: MetaStore | None = None):
    """
    被装饰的函数要求：
        - 有一个名为 symbols 的参数（股票列表）
        - 返回值与 fetch_fmp_batch() 相同：(df, summary, succeed_list, failed_list)
        - 如果函数有名为 run 的参数，会自动传入 IngestionRun（可用 run.batch_id）
    其余参数作为 params 记入 meta.ingestion。
    """
    def decorator(fn):
        sig = inspect.signature(fn)
        if "symbols" not in sig.parameters:
            raise TypeError(f"{fn.__name__} 必须有名为 symbols 的参数才能使用 @track_ingestion")

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind_partial(*args, **kwargs)
            bound.apply_defaults()
            params = {k: v for k, v in bound.arguments.items() if k not in ("symbols", "run")}
            meta = store or MetaStore()

            with meta.ingestion_run(endpoint, bound.arguments["symbols"], params) as run:
                if "run" in sig.parameters:
                    bound.arguments["run"] = run
                df, summary, succeed, failed = fn(*bound.args, **bound.kwargs)
                run.finish(summary, failed, rows_fetched=len(df))
            return df, summary, succeed, failed

        return wrapper
    return decorator
