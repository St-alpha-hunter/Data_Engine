"""
拆股整理：FMP splits?symbol= / splits-calendar 的原始行 → 标准化拆股事件
输出的每个事件可直接作为 events.record_split() 的参数

FMP 字段：symbol, date(生效日), numerator, denominator, splitType（stock-split / stock-dividend / 空）

类型判断：
    stock-dividend 且比例 > 1 → stock_dividend（送股）
    stock-dividend 且比例 < 1 → adjustment（价格调整：FMP 把合并、分拆、特别分配记成这种形式，
                                如 BK 2007 合并 4717:5000、VTR 2015 分拆 CCP 8757:10000）
    stock-split 或空          → 按比例：分子 < 分母为 reverse（合股），否则 forward（拆股）
"""
from datetime import date

from fetch_data.events.common import NormalizeResult, clean_text, payload_of, status_for, to_date, to_decimal, to_symbol

EVENT_TYPE = "split"


def split_type_of(numerator, denominator, fmp_split_type: str | None) -> str:
    if fmp_split_type == "stock-dividend":
        return "stock_dividend" if numerator > denominator else "adjustment"
    return "reverse" if numerator < denominator else "forward"


def normalize_splits(rows: list[dict], source: str = "fmp", today: date | None = None) -> NormalizeResult:
    result = NormalizeResult(EVENT_TYPE)
    seen = set()
    for row in rows:
        symbol = to_symbol(row.get("symbol"))
        effective_date = to_date(row.get("date"))
        numerator = to_decimal(row.get("numerator"))
        denominator = to_decimal(row.get("denominator"))
        if symbol is None:
            result.reject("invalid_symbol", row); continue
        if effective_date is None:
            result.reject("invalid_date", row); continue
        if numerator is None or denominator is None or numerator <= 0 or denominator <= 0:
            result.reject("invalid_ratio", row); continue
        if numerator == denominator:
            result.reject("ratio_one_to_one", row); continue

        fmp_type = clean_text(row.get("splitType"))
        split_type = split_type_of(numerator, denominator, fmp_type)

        key = f"{symbol}|{effective_date}|split"
        if key in seen:
            result.skip("duplicate_in_batch", row); continue
        seen.add(key)

        result.events.append({
            "symbol": symbol,
            "effective_date": effective_date,
            "numerator": numerator,
            "denominator": denominator,
            "split_type": split_type,
            "status": status_for(effective_date, today),
            "source": source,
            "source_event_key": key,
            "payload": payload_of(row),
        })
    return result
