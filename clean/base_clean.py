from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Rule:
    """
    一条质量校验规则（以代码为准，运行时同步到 meta.quality_rules）
    check：子类里的检查方法名，签名为 (self, df, **params) -> 布尔 Series，True 表示这一行违反规则
    改了 severity 或 params 必须把 version + 1，否则同步时会报错
    """
    rule_id: str
    version: int
    severity: str                      # warning / danger
    description: str
    check: str
    params: dict = field(default_factory=dict)
    detail_columns: tuple = ()         # 记进 quality_issues.detail 的列；为空则用 config["numeric_columns"]


@dataclass
class CleanResult:
    """
    validate() 的输出，pipeline 拿到后分发：
        clean_df      -> 中间态 parquet / golden（health + warning，带 quality_status，只有原始字段）
        quarantine_df -> meta.quarantine_records（触发 danger 规则的行）
        issues        -> meta.quality_issues（每条记录触发的每条规则一项）
    """
    clean_df: pd.DataFrame
    quarantine_df: pd.DataFrame
    issues: list[dict]
    stats: dict


class CleanBasic:
    """
    初次清洗的基类
    drop_columns删除缺失值
    date_column统一转化日期
    drop_duplicate需要去重的列

    两种用法：
        clean()     旧流程：错误记在 self.errors，clean_df 里仍含错误行（尚未定义 rules 的子类继续用）
        validate()  新流程：按 rules 校验，返回 CleanResult（定义了 rules 的子类用）
    """
    endpoint_name = None
    config = {
        "required_columns": [],
        "numeric_columns": [],
        "date_columns": [],
        "drop_columns": [],
        "duplicate_columns": [],
        "output_columns": [],          # validate() 输出的原始字段
    }
    rules: list[Rule] = []

    # 自动注册：endpoint_name -> 清洗类。子类写上 endpoint_name 就会被登记，pipeline 按接口名查找
    registry: dict[str, type] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("endpoint_name")
        if not name:
            return
        existing = CleanBasic.registry.get(name)
        # 同一个模块被重复导入（如 __main__ 和 clean.xxx）时类名相同，不算冲突
        if existing is not None and existing.__qualname__ != cls.__qualname__:
            raise TypeError(f"endpoint {name} 已经有清洗类 {existing.__name__}，不能再注册 {cls.__name__}")
        CleanBasic.registry[name] = cls

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
        # 定义了 rules 的子类走新流程，保证两种调用方式结果一致
        if self.rules:
            return self.validate().clean_df

        self._check_required_columns()
        self._convert_types()
        self._drop_invalid_rows()
        self._drop_duplicates()
        self._custom_validate()
        #return self.df, self.errors
        return self.clean_df

    # =========================
    # 新流程：按规则校验
    # =========================
    def validate(self) -> CleanResult:
        if not self.rules:
            raise NotImplementedError(f"{type(self).__name__} 还没有定义 rules，请用 clean()")

        n_in = len(self.raw_df)
        self._check_required_columns()
        self._convert_types()
        # 不在这里删缺失值：缺失值交给规则判定（进隔离区），保证 raw 行数 = clean + quarantine + 去重行数
        n_before_dedup = len(self.raw_df)
        self._drop_duplicates()
        n_duplicates = n_before_dedup - len(self.raw_df)

        # 子类可在 _prepare 里排序、算辅助列（如收益率）；辅助列不会进入输出
        self.raw_df = self._prepare(self.raw_df).reset_index(drop=True)
        df = self.raw_df
        self.errors = []

        issues: list[dict] = []
        danger = pd.Series(False, index=df.index)
        warning = pd.Series(False, index=df.index)

        for rule in self.rules:
            mask = getattr(self, rule.check)(df, **rule.params)
            mask = mask.reindex(df.index).fillna(False).astype(bool)
            if mask.any():
                self._add_error(mask, rule.rule_id)        # 兼容 FeedErrorDeputy
                issues.extend(self._issues_for(rule, df.loc[mask]))
            if rule.severity == "danger":
                danger |= mask
            else:
                warning |= mask

        out_cols = self.config.get("output_columns") or list(df.columns)
        clean_df = df.loc[~danger, out_cols].copy()
        clean_df["quality_status"] = np.where(warning[~danger], "warning", "health")
        quarantine_df = df.loc[danger, out_cols].copy()

        self.clean_df = clean_df
        stats = {
            "rows_in": n_in,
            "duplicates_dropped": n_duplicates,
            "health": int((clean_df["quality_status"] == "health").sum()),
            "warning": int((clean_df["quality_status"] == "warning").sum()),
            "danger": len(quarantine_df),
            "issues": len(issues),
        }
        return CleanResult(clean_df.reset_index(drop=True), quarantine_df.reset_index(drop=True), issues, stats)

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _issues_for(self, rule: Rule, bad: pd.DataFrame) -> list[dict]:
        cols = [c for c in (rule.detail_columns or self.config["numeric_columns"]) if c in bad.columns]
        details = bad[cols].astype(object).where(bad[cols].notna(), None).to_dict(orient="records")
        return [
            {"rule_id": rule.rule_id, "rule_version": rule.version, "severity": rule.severity,
             "symbol": symbol, "date": date, "detail": detail}
            for symbol, date, detail in zip(bad["symbol"], bad["date"], details)
        ]

    @classmethod
    def rule_definitions(cls) -> list[dict]:
        """给 MetaStore.sync_rules() 用的规则清单"""
        return [
            {"rule_id": r.rule_id, "rule_version": r.version, "endpoint": cls.endpoint_name,
             "description": r.description, "severity": r.severity, "params": dict(r.params),
             "impl": f"{cls.__name__}.{r.check}"}
            for r in cls.rules
        ]

    # =========================
    # 公共步骤
    # =========================
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
