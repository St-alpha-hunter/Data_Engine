"""
事件整理函数：API 原始行 → 标准化事件（纯函数，不碰数据库）

    from fetch_data.events import normalize_dividends
    result = normalize_dividends(rows)
    result.events      # 可写库：每个 dict 的键和 events.record_dividend() 的参数一致（去掉 p_ 前缀）
    result.rejected    # 数据有问题，写库阶段记为质量问题
    result.skipped     # 按约定不记录（如私有公司被收购）
    result.summary()
"""
from fetch_data.events.acquisitions import normalize_acquisitions
from fetch_data.events.common import NormalizeResult
from fetch_data.events.dividends import normalize_dividends
from fetch_data.events.sp500_index_changes import normalize_sp500_index_changes
from fetch_data.events.splits import normalize_splits
from fetch_data.events.symbol_changes import normalize_symbol_changes

__all__ = [
    "NormalizeResult",
    "normalize_dividends", "normalize_splits", "normalize_symbol_changes",
    "normalize_acquisitions", "normalize_sp500_index_changes",
]
