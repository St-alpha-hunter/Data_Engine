"""
ClickHouse 读写：DataFrame 进、DataFrame 出
"""
import pandas as pd

from db.connections import get_ch


class ClickHouseStore:

    def __init__(self, client=None):
        self.client = client or get_ch()

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        """把 DataFrame 写入表，列名需与表字段一致。返回写入行数。"""
        if df.empty:
            return 0
        self.client.insert_df(table, df)
        return len(df)

    def query_df(self, sql: str, parameters: dict | None = None) -> pd.DataFrame:
        return self.client.query_df(sql, parameters=parameters)

    def command(self, sql: str, parameters: dict | None = None):
        """执行 DDL 等不返回结果集的语句"""
        return self.client.command(sql, parameters=parameters)
