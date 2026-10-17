"""
收购整理：FMP mergers-acquisitions-latest 的原始行 → 标准化收购事件（一笔交易一条）
输出的每个事件可直接作为 events.record_acquisition() 的参数

FMP 字段：symbol, companyName, cik（收购方）, targetedSymbol, targetedCompanyName, targetedCik（被收购方）,
          transactionDate（SEC 文件日期）, acceptedDate, link

FMP 的原始数据有大量重复，先按交易合并：
    · 收购方有多个代码（普通股、优先股、SPAC 单位 / 认股权证），每个代码一行
    · 同一笔交易提交了多份 SEC 文件，每份一行
合并规则：
    deal_key = 被收购方 CIK | 收购方 CIK（CIK 缺失时用代码代替）
    filing_date = 最早的 transactionDate；filing_url / accepted_at 取最早那份文件
    代码选"普通股"：不带 '-' 的代码里取最短的（MITT 而不是 MITT-PA，CCXI 而不是 CCXIU / CCXIW）
    所有原始行都保存在 payload 里
被收购方没有代码（私有公司）的交易 → skipped（private_target）
完成日（completion_date）这里不填：要用被收购方的最后交易日推断，在写库阶段处理
"""
import json
from collections import defaultdict
from datetime import date
from zoneinfo import ZoneInfo

from fetch_data.events.common import (
    NormalizeResult, clean_text, payload_of, to_cik, to_date, to_datetime, to_symbol,
)

EVENT_TYPE = "acquisition"
SEC_TZ = ZoneInfo("America/New_York")       # FMP 的 acceptedDate 是 SEC 的接收时间，美东当地时间，不带时区


def accepted_time(value):
    dt = to_datetime(value)
    return dt.replace(tzinfo=SEC_TZ) if dt is not None and dt.tzinfo is None else dt


def pick_common_symbol(symbols) -> str | None:
    """从同一家公司的多个代码里挑普通股：优先不带 '-' 的，再取最短的，长度相同按字母序"""
    valid = sorted({s for s in symbols if s})
    if not valid:
        return None
    plain = [s for s in valid if "-" not in s] or valid
    return min(plain, key=lambda s: (len(s), s))


def normalize_acquisitions(rows: list[dict], source: str = "fmp") -> NormalizeResult:
    result = NormalizeResult(EVENT_TYPE)

    deals = defaultdict(list)
    for row in rows:
        target_symbol = to_symbol(row.get("targetedSymbol"))
        if target_symbol is None:
            if clean_text(row.get("targetedSymbol")) is None:
                result.skip("private_target", row)             # 私有公司：按约定不记
            else:
                result.reject("invalid_symbol", row)
            continue
        if to_date(row.get("transactionDate")) is None:
            result.reject("invalid_date", row); continue
        target_cik = to_cik(row.get("targetedCik"))
        acquirer_cik = to_cik(row.get("cik"))
        deal_key = f"{target_cik or target_symbol}|{acquirer_cik or to_symbol(row.get('symbol')) or 'UNKNOWN'}"
        deals[deal_key].append(row)

    for deal_key, group in deals.items():
        # 排序要完全确定：同一份文件 FMP 可能按不同股票类别各给一行（日期、受理时间都相同），
        # 返回顺序每次不一样，不排到底的话合并后的 payload 会变，产生假修订（实测每日运行 34 条）
        group = sorted(group, key=lambda r: (to_date(r.get("transactionDate")), str(r.get("acceptedDate") or ""),
                                             str(r.get("link") or ""), str(r.get("symbol") or ""),
                                             str(r.get("targetedSymbol") or ""), json.dumps(r, sort_keys=True, default=str)))
        first = group[0]
        target_symbol = pick_common_symbol(to_symbol(r.get("targetedSymbol")) for r in group)
        acquirer_symbol = pick_common_symbol(to_symbol(r.get("symbol")) for r in group)

        result.events.append({
            "deal_key": deal_key,
            "target_symbol": target_symbol,
            "target_name": clean_text(first.get("targetedCompanyName")),
            "target_cik": to_cik(first.get("targetedCik")),
            "acquirer_symbol": acquirer_symbol,
            "acquirer_name": clean_text(first.get("companyName")),
            "acquirer_cik": to_cik(first.get("cik")),
            "filing_date": to_date(first.get("transactionDate")),
            "completion_date": None,
            "filing_url": clean_text(first.get("link")),
            "accepted_at": accepted_time(first.get("acceptedDate")),
            "source": source,
            "source_event_key": deal_key,
            "payload": {
                "deal_key": deal_key,
                "target_symbols": sorted({to_symbol(r.get("targetedSymbol")) for r in group} - {None}),
                "acquirer_symbols": sorted({to_symbol(r.get("symbol")) for r in group} - {None}),
                "filing_dates": sorted({str(to_date(r.get("transactionDate"))) for r in group}),
                "rows": [payload_of(r) for r in group],
            },
        })
    return result
