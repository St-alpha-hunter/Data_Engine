"""
交易日历：生成 + 落库 + FMP 交叉核对

    1. 用 exchange_calendars 生成 1990-01-01 到"今年 + 2 年"年底的每个自然日（XNYS、XNAS 分开）
    2. 写入 ref.trading_calendar：有变化才更新，没变化的行不动
    3. 从 FMP holidays-by-exchange 拉同一时间段的节假日（记一个 ingestion 批次）
    4. 逐日比对，差异作为 warning 记入 meta.quality_issues（规则 CAL001–CAL003），不自动覆盖日历

定期运行（如每周一次）即可：日历始终覆盖到未来 2 年；FMP 有新公布的临时休市会作为差异提示出来。

命令行：
    python -m pipelines.trading_calendar            # 生成 + 落库 + FMP 核对
    python -m pipelines.trading_calendar --no-fmp   # 只生成 + 落库
"""
import argparse
import logging
from datetime import date, timedelta
from io import StringIO

import exchange_calendars as xcals
import numpy as np
import pandas as pd
import psycopg

from db import MetaStore
from fetch_data.fetch import FetchData

log = logging.getLogger(__name__)

START = date(1990, 1, 1)
HORIZON_YEARS = 2
SOURCE = "exchange_calendars"

# 交易所 MIC 代码 -> FMP 里的交易所名
EXCHANGES = {"XNYS": "NYSE", "XNAS": "NASDAQ"}
FMP_TO_MIC = {v: k for k, v in EXCHANGES.items()}

COLUMNS = ["exchange", "date", "is_trading_day", "session_type", "open_time", "close_time", "holiday_name",
           "trading_day_seq", "prev_trading_day", "next_trading_day", "source", "source_version"]

# 交叉核对规则（以代码为准，运行时同步到 meta.quality_rules）
CALENDAR_ENDPOINT = "holidays_by_exchange"
CALENDAR_RULES = [
    {"rule_id": "CAL001", "rule_version": 1, "endpoint": CALENDAR_ENDPOINT, "severity": "warning",
     "description": "FMP 标为休市，但 exchange_calendars 为交易日", "params": {},
     "impl": "pipelines.trading_calendar.cross_check"},
    {"rule_id": "CAL002", "rule_version": 1, "endpoint": CALENDAR_ENDPOINT, "severity": "warning",
     "description": "exchange_calendars 为休市（非周末），但 FMP 没有标为休市", "params": {},
     "impl": "pipelines.trading_calendar.cross_check"},
    # FMP 2012 年以前没有提前收盘数据，只核对 early_close_from 之后
    {"rule_id": "CAL003", "rule_version": 1, "endpoint": CALENDAR_ENDPOINT, "severity": "warning",
     "description": "提前收盘不一致：一方有一方没有，或收盘时间不同（只核对 early_close_from 之后）",
     "params": {"early_close_from": "2012-01-01"}, "impl": "pipelines.trading_calendar.cross_check"},
]


def default_end(today: date | None = None) -> date:
    today = today or date.today()
    return date(today.year + HORIZON_YEARS, 12, 31)


# =========================
# 1. 生成
# =========================
def build_calendar(exchange: str, start: date = START, end: date | None = None) -> pd.DataFrame:
    """用 exchange_calendars 生成 [start, end] 每个自然日一行"""
    end = end or default_end()
    cal = xcals.get_calendar(exchange, start=str(start), end=str(end))
    tz = cal.tz

    days = pd.date_range(start, end, freq="D")
    schedule = cal.schedule.loc[str(start):str(end)]
    opens = schedule["open"].dt.tz_convert(tz)
    closes = schedule["close"].dt.tz_convert(tz)
    early = {d.normalize().tz_localize(None) for d in cal.early_closes}

    # 节假日名称：常规节假日带名字，临时休市统一叫 Special closure
    names = {}
    regular = cal.regular_holidays.holidays(start=start, end=end, return_name=True)
    for d, name in regular.items():
        names.setdefault(pd.Timestamp(d).normalize(), name)
    for d in cal.adhoc_holidays:
        names.setdefault(pd.Timestamp(d).tz_localize(None).normalize(), "Special closure")

    df = pd.DataFrame({"date": days})
    df["exchange"] = exchange
    df["is_trading_day"] = df["date"].isin(schedule.index)
    df["session_type"] = np.where(~df["is_trading_day"], "closed",
                                  np.where(df["date"].isin(early), "early_close", "full"))
    open_map = dict(zip(schedule.index, opens.dt.time))
    close_map = dict(zip(schedule.index, closes.dt.time))
    df["open_time"] = df["date"].map(open_map)
    df["close_time"] = df["date"].map(close_map)

    weekday = df["date"].dt.dayofweek < 5
    df["holiday_name"] = df["date"].map(names)
    df.loc[df["is_trading_day"] | ~weekday, "holiday_name"] = None     # 周末、交易日不写节假日名
    df.loc[df["session_type"] == "early_close", "holiday_name"] = "Early close"

    seq = df["is_trading_day"].cumsum()
    df["trading_day_seq"] = seq.where(df["is_trading_day"])

    trading = df["date"].where(df["is_trading_day"])
    df["prev_trading_day"] = trading.shift(1).ffill()           # 严格早于当天
    df["next_trading_day"] = trading.shift(-1).bfill()          # 严格晚于当天

    df["source"] = SOURCE
    df["source_version"] = xcals.__version__
    for col in ("date", "prev_trading_day", "next_trading_day"):
        df[col] = df[col].dt.date
    df["trading_day_seq"] = df["trading_day_seq"].astype("Int64")
    df = df.astype(object).where(df.notna(), None)
    return df[COLUMNS]


# =========================
# 2. 落库
# =========================
def upsert_calendar(conn: psycopg.Connection, df: pd.DataFrame) -> dict:
    """写入 ref.trading_calendar：新日期插入，有变化的更新，没变化的不动。返回 {inserted, updated, unchanged}"""
    compare = [c for c in COLUMNS if c not in ("exchange", "date")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in compare)
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in compare)

    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_calendar (LIKE ref.trading_calendar INCLUDING DEFAULTS) ON COMMIT DROP")
        with cur.copy(f"COPY tmp_calendar ({', '.join(COLUMNS)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as copy:
            copy.write(buf.read())
        rows = cur.execute(f"""
            INSERT INTO ref.trading_calendar AS t ({', '.join(COLUMNS)})
            SELECT {', '.join(COLUMNS)} FROM tmp_calendar
            ON CONFLICT (exchange, date) DO UPDATE SET {set_clause}, updated_at = now()
            WHERE {changed}
            RETURNING (xmax = 0) AS inserted
        """).fetchall()
    inserted = sum(1 for (i,) in rows if i)
    updated = len(rows) - inserted
    return {"inserted": inserted, "updated": updated, "unchanged": len(df) - len(rows)}


# =========================
# 3. FMP 交叉核对
# =========================
def fetch_fmp_holidays(store: MetaStore, start: date, end: date, fetcher_cls=FetchData) -> tuple[str, pd.DataFrame]:
    """拉 FMP 节假日，记一个 ingestion 批次。返回 (batch_id, df)"""
    # FMP 这个接口的 from 不含当天，往前放一天
    params = {"from_date": start - timedelta(days=1), "to_date": end}
    symbols = list(EXCHANGES.values())
    with store.ingestion_run(CALENDAR_ENDPOINT, symbols, params) as run:
        with fetcher_cls(symbols, CALENDAR_ENDPOINT, params) as fetcher:
            df, summary, succeed, failed = fetcher.fetch_fmp_batch()
        run.finish(summary, failed, rows_fetched=len(df))
    return run.batch_id, df


def cross_check(cal: pd.DataFrame, fmp: pd.DataFrame, exchange: str, early_close_from: str = "2012-01-01") -> list[dict]:
    """
    逐日比对 exchange_calendars 生成的日历和 FMP 节假日，返回问题列表（可直接写 meta.quality_issues）
        CAL001 FMP 休市、日历交易
        CAL002 日历休市（非周末）、FMP 没标休市
        CAL003 提前收盘不一致（只核对 early_close_from 之后）
    """
    rules = {r["rule_id"]: r for r in CALENDAR_RULES}
    cal = cal.set_index("date")
    fmp = fmp[fmp["symbol"] == EXCHANGES[exchange]].copy() if "symbol" in fmp.columns else fmp.iloc[0:0]
    fmp["date"] = pd.to_datetime(fmp["date"]).dt.date
    fmp = fmp[(fmp["date"] >= cal.index.min()) & (fmp["date"] <= cal.index.max())].drop_duplicates("date").set_index("date")

    fmp_closed = {d for d, r in fmp.iterrows() if r.get("isClosed") is True}
    fmp_early = {d: r.get("adjCloseTime") for d, r in fmp.iterrows()
                 if r.get("isClosed") is not True and r.get("adjCloseTime")}

    issues, flagged = [], set()

    def add(rule_id, d, detail):
        r = rules[rule_id]
        issues.append({"rule_id": rule_id, "rule_version": r["rule_version"], "severity": r["severity"],
                       "symbol": exchange, "date": d, "detail": detail})
        flagged.add(d)

    def cal_info(d):
        row = cal.loc[d]
        return {"session_type": row["session_type"], "close_time": str(row["close_time"]) if row["close_time"] else None,
                "holiday_name": row["holiday_name"]}

    def fmp_info(d):
        if d not in fmp.index:
            return None
        r = fmp.loc[d]
        return {"name": r.get("name"), "isClosed": r.get("isClosed"), "adjCloseTime": r.get("adjCloseTime")}

    for d in sorted(fmp_closed):
        if d in cal.index and cal.loc[d, "is_trading_day"]:
            add("CAL001", d, {"fmp": fmp_info(d), "calendar": cal_info(d)})

    holidays = cal[(~cal["is_trading_day"]) & (pd.to_datetime(cal.index).dayofweek < 5)].index
    for d in holidays:
        if d not in fmp_closed:
            add("CAL002", d, {"fmp": fmp_info(d), "calendar": cal_info(d)})

    since = pd.Timestamp(early_close_from).date()
    cal_early = {d: str(row["close_time"])[:5] for d, row in cal[cal["session_type"] == "early_close"].iterrows()}
    for d in sorted(set(cal_early) | set(fmp_early)):
        if d < since or d in flagged:
            continue
        if cal_early.get(d) != fmp_early.get(d):
            add("CAL003", d, {"fmp": fmp_info(d), "calendar": cal_info(d) if d in cal.index else None})
    return issues


# =========================
# 一键运行
# =========================
def run(*, store: MetaStore | None = None, start: date = START, end: date | None = None,
        with_fmp: bool = True, fetcher_cls=FetchData) -> dict:
    store = store or MetaStore()
    end = end or default_end()
    result = {"range": (str(start), str(end)), "exchanges": {}}

    calendars = {}
    for exchange in EXCHANGES:
        cal = build_calendar(exchange, start, end)
        calendars[exchange] = cal
        stats = upsert_calendar(store.conn, cal)
        result["exchanges"][exchange] = {"days": len(cal), "trading_days": int(sum(cal["is_trading_day"])), **stats}
        log.info(f"[{exchange}] {start} ~ {end}：{len(cal)} 天，写入 {stats}")

    if with_fmp:
        store.sync_rules(CALENDAR_RULES)
        batch_id, fmp = fetch_fmp_holidays(store, start, end, fetcher_cls)
        params = {r["rule_id"]: r["params"] for r in CALENDAR_RULES}
        issues = []
        for exchange, cal in calendars.items():
            issues += cross_check(cal, fmp, exchange, **params["CAL003"])
        store.record_issues(batch_id, issues)
        result["fmp_batch_id"] = batch_id
        result["issues"] = issues
        log.info(f"FMP 交叉核对：批次 {batch_id}，差异 {len(issues)} 条")
    return result


def main():
    parser = argparse.ArgumentParser(description="生成交易日历并落库，可选 FMP 交叉核对")
    parser.add_argument("--no-fmp", action="store_true", help="只生成 + 落库，不做 FMP 核对")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    r = run(with_fmp=not args.no_fmp)
    print(f"范围：{r['range'][0]} ~ {r['range'][1]}")
    for ex, s in r["exchanges"].items():
        print(f"  {ex}：{s['days']} 天，交易日 {s['trading_days']}；新增 {s['inserted']}，更新 {s['updated']}，未变 {s['unchanged']}")
    if "issues" in r:
        print(f"FMP 核对（批次 {r['fmp_batch_id']}）：差异 {len(r['issues'])} 条")
        for i in r["issues"]:
            fmp = i["detail"]["fmp"] or {}
            cal = i["detail"]["calendar"] or {}
            print(f"  {i['rule_id']} {i['symbol']} {i['date']}  FMP: {fmp.get('name')} closed={fmp.get('isClosed')} "
                  f"close={fmp.get('adjCloseTime')}  |  日历: {cal.get('session_type')} {cal.get('close_time')} {cal.get('holiday_name')}")


if __name__ == "__main__":
    main()
