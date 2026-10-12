"""
代码变更整理：FMP symbol-change 的原始行 → 标准化代码变更事件
输出的每个事件可直接作为 events.record_symbol_change() 的参数

FMP 字段：date(变更生效日), companyName, oldSymbol, newSymbol
已知问题：有日期为 1969-12-31 的占位数据 → rejected（invalid_date），写库阶段记为质量问题（只拦截占位日期，1970 年以前的真实日期正常接收）
"""
from datetime import date

from fetch_data.events.common import NormalizeResult, clean_text, payload_of, status_for, to_date, to_symbol

EVENT_TYPE = "symbol_change"


def normalize_symbol_changes(rows: list[dict], source: str = "fmp", today: date | None = None) -> NormalizeResult:
    result = NormalizeResult(EVENT_TYPE)
    seen = set()
    for row in rows:
        old_symbol = to_symbol(row.get("oldSymbol"))
        new_symbol = to_symbol(row.get("newSymbol"))
        change_date = to_date(row.get("date"))
        if old_symbol is None or new_symbol is None:
            result.reject("invalid_symbol", row); continue
        if change_date is None:
            result.reject("invalid_date", row); continue
        if old_symbol == new_symbol:
            result.reject("same_symbol", row); continue

        key = f"{old_symbol}|{new_symbol}|{change_date}"
        if key in seen:
            result.skip("duplicate_in_batch", row); continue
        seen.add(key)

        result.events.append({
            "change_date": change_date,
            "old_symbol": old_symbol,
            "new_symbol": new_symbol,
            "company_name": clean_text(row.get("companyName")),
            "status": status_for(change_date, today),
            "source": source,
            "source_event_key": key,
            "payload": payload_of(row),
        })
    return result
