"""
S&P 500 成分股变更整理：GitHub fja05680/sp500 的 sp500_ticker_start_end.csv → 标准化调入 / 调出事件
输出的每个事件可直接作为 events.record_sp500_index_change() 的参数

CSV 字段：ticker, start_date, end_date（仍在成分里的 end_date 为空）
每段区间拆成两个事件：start_date 调入（add）、end_date 调出（remove）

注意：该数据集的代码有时是公司"后来的代码"（如 2006 年的谷歌记成 GOOGL），
这里只按原样整理，security_id 留空，等证券主表核对后再回填
"""
from fetch_data.events.common import NormalizeResult, payload_of, to_date, to_symbol

EVENT_TYPE = "index_change"
INDEX_CODE = "SP500"


def normalize_sp500_index_changes(rows: list[dict], source: str = "sp500_github") -> NormalizeResult:
    result = NormalizeResult(EVENT_TYPE)
    seen = set()

    def add_event(symbol, d, action, row):
        key = f"{INDEX_CODE}|{symbol}|{action}|{d}"
        if key in seen:
            result.skip("duplicate_in_batch", row); return
        seen.add(key)
        result.events.append({
            "symbol": symbol,
            "effective_date": d,
            "action": action,
            "status": "confirmed",
            "source": source,
            "source_event_key": key,
            "payload": payload_of(row),
        })

    for row in rows:
        symbol = to_symbol(row.get("ticker"))
        start = to_date(row.get("start_date"))
        end_raw = row.get("end_date")
        end = to_date(end_raw)
        if symbol is None:
            result.reject("invalid_symbol", row); continue
        if start is None:
            result.reject("invalid_date", row); continue
        if end_raw is not None and str(end_raw).strip() not in ("", "nan", "NaN", "None") and end is None:
            result.reject("invalid_date", row); continue
        if end is not None and end <= start:
            result.reject("end_before_start", row); continue

        add_event(symbol, start, "add", row)
        if end is not None:
            add_event(symbol, end, "remove", row)
    return result
