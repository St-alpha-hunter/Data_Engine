"""
连通性检查：容器里的任务能不能导入项目代码、连上 PostgreSQL / ClickHouse / FMP

不定时运行，需要时在网页上手动触发；新环境搭好、改了网络或密钥之后先跑它
"""
import pendulum
from airflow.sdk import dag, task


@dag(schedule=None, start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"), catchup=False,
     tags=["ops"], doc_md=__doc__)
def healthcheck():

    @task
    def postgres() -> dict:
        from db.meta_store import MetaStore
        conn = MetaStore().conn
        return {"securities": conn.execute("SELECT count(*) FROM ref.securities").fetchone()[0],
                "trading_days": conn.execute("SELECT count(*) FROM ref.trading_calendar WHERE is_trading_day").fetchone()[0]}

    @task
    def clickhouse() -> dict:
        from db.ch_store import ClickHouseStore
        r = ClickHouseStore().query_df("SELECT count() AS n, max(date) AS last FROM golden.v_price_daily")
        return {"golden_rows": int(r["n"][0]), "last_date": str(r["last"][0])}

    @task
    def fmp() -> dict:
        from fetch_data.fetch import FetchData
        with FetchData(["AAPL"], "profile", None) as f:
            df, summary, _, failed = f.fetch_fmp_batch()
        if failed or df.empty:
            raise RuntimeError(f"FMP 请求失败：{failed}")
        return {"aapl_price": float(df["price"][0])}

    @task
    def intermediate() -> dict:
        """中间态 parquet 能不能读（Windows 上记下的路径在容器里要能换算过来）"""
        from db.meta_store import MetaStore
        from db.object_store import IntermediateStore
        loc = MetaStore().conn.execute("SELECT location FROM meta.load_log WHERE layer = 'intermediate' "
                                       "AND status = 'success' ORDER BY 1 DESC LIMIT 1").fetchone()[0]
        return {"location": loc, "rows": len(IntermediateStore().read(loc))}

    postgres() >> clickhouse() >> fmp() >> intermediate()


healthcheck()
