import pandas as pd

class CleanBasic:
    """
    初次清洗的基类
    drop_columns删除缺失值
    date_column统一转化日期
    drop_duplicate需要去重的列
    """
    endpoint_name = None
    config = {
        "required_columns": [],
        "numeric_columns": [],
        "date_columns": [],
        "drop_columns": [],
        "duplicate_columns": []
    }

    def __init__(self, df: pd.DataFrame):
        if df is None:
            raise ValueError("收到空表，请检查错误")

        self.endpoint_name = self.endpoint_name
        self.raw_df = df.copy()
        self.config = self.config
        self.errors = []
        self.clean_df = None
        self.stats_df = None

    def clean(self):
        self._check_required_columns()
        self._convert_types()
        self._drop_invalid_rows()
        self._drop_duplicates()
        self._custom_validate()
        #return self.df, self.errors
        return self.clean_df


    def _check_required_columns(self):
        missing = set(self.config["required_columns"]) - set(self.raw_df.columns)
        if missing:
            raise KeyError(f"缺少字段: {list(missing)}")

    def _convert_types(self):
        for col in self.config["date_columns"]:
            self.raw_df[col] = pd.to_datetime(self.raw_df[col], errors="coerce")

        for col in self.config["numeric_columns"]:
            self.raw_df[col] = pd.to_numeric(self.raw_df[col], errors="coerce")

    def _drop_invalid_rows(self):
        if self.config["drop_columns"]:
            self.raw_df = self.raw_df.dropna(subset=self.config["drop_columns"])

    def _drop_duplicates(self):
        if self.config["duplicate_columns"]:
            self.raw_df = self.raw_df.drop_duplicates(subset=self.config["duplicate_columns"])

    ##子类把这些方法装进feed
    def _add_error(self, mask, error_name):
        #self.raw_df[error_name] = mask.astype(int)
        error_df = self.raw_df.loc[mask].copy()

        if not error_df.empty:
            self.errors.append({
                "error_name": error_name,
                "data错误地址": error_df
            })

    def _custom_validate(self):
         pass


    def execute(self):
        pass