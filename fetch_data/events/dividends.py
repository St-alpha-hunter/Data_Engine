"""
分红整理：FMP dividends?symbol= / dividends-calendar 的原始行 → 标准化分红事件
输出的每个事件可直接作为 events.record_dividend() 的参数

FMP 字段：symbol, date(除息日), recordDate, paymentDate, declarationDate, adjDividend, dividend, yield, frequency
"""
from datetime import date

from fetch_data.events.common import (
    NormalizeResult, clean_text, payload_of, status_for, to_date, to_decimal, to_symbol,
)

EVENT_TYPE = "dividend"
FREQUENCIES = {"Weekly", "Bi-Weekly", "Monthly", "Quarterly", "Semi-Annual", "Annual", "Irregular", "Special"}
SPECIAL_FREQUENCIES = {"Special", "Irregular"}


VOLATILE_FIELDS = {"yield"}          # 随行情变化、不属于这笔分红本身的字段


def normalize_dividends(rows: list[dict], source: str = "fmp", today: date | None = None) -> NormalizeResult:
    result = NormalizeResult(EVENT_TYPE)
    seen = set()
    for row in rows:
        symbol = to_symbol(row.get("symbol"))
        ex_date = to_date(row.get("date"))
        amount = to_decimal(row.get("dividend"))
        if symbol is None:
            result.reject("invalid_symbol", row); continue
        if ex_date is None:
            result.reject("invalid_date", row); continue
        if amount is None or amount <= 0:
            result.reject("invalid_amount", row); continue

        frequency = clean_text(row.get("frequency"))
        if frequency is not None and frequency not in FREQUENCIES:
            result.reject("unknown_frequency", row); continue

        adj_amount = to_decimal(row.get("adjDividend"))
        if adj_amount is not None and adj_amount <= 0:
            adj_amount = None                          # 调整后金额只是参考，无效就不填，不拒绝整条

        key = f"{symbol}|{ex_date}|dividend"
        if key in seen:                                # 同一批里重复出现（如日历和历史接口重叠）
            result.skip("duplicate_in_batch", row); continue
        seen.add(key)

        result.events.append({
            "symbol": symbol,
            "ex_date": ex_date,
            "amount": amount,
            "adj_amount": adj_amount,
            "record_date": to_date(row.get("recordDate")),
            "payment_date": to_date(row.get("paymentDate")),
            "declaration_date": to_date(row.get("declarationDate")),
            "frequency": frequency,
            "dividend_type": "special" if frequency in SPECIAL_FREQUENCIES else "regular",
            "currency": "USD",
            "status": status_for(ex_date, today),
            "source": source,
            "source_event_key": key,
            # yield（股息率）不算进内容：FMP 按当前股价算，每天都变，算进去每拉一次就多一个假修订（实测每日 47 条）
            "payload": payload_of({k: v for k, v in row.items() if k not in VOLATILE_FIELDS}),
        })
    return result
