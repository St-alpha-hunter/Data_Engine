"""
事件整理的公共部分：统一的返回结构 + 日期 / 代码 / 金额的清洗

整理函数是纯函数：输入 API 返回的原始行，输出标准化事件，不碰数据库。
标准化事件是一个 dict，键名和 events.record_xxx() 的参数一致（去掉 p_ 前缀），
写库时可以直接把它展开成参数。

每个整理函数返回 NormalizeResult：
    events    整理好的事件，可以写库
    rejected  数据本身有问题，不能写库（如日期无效、金额 <= 0）；写库阶段记为质量问题
    skipped   数据没问题，但按约定不记录（如被收购方是私有公司）
"""
import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from config.symbol_map import from_fmp_symbol

# FMP 的占位日期（时间戳为 0 换算出来的日期），只拦截这两个；更早的真实日期（如 IBM 1968 年拆股）正常接收
SENTINEL_DATES = {date(1969, 12, 31), date(1970, 1, 1)}

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]*$")


@dataclass
class NormalizeResult:
    event_type: str
    events: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)      # {"reason": ..., "row": ...}
    skipped: list[dict] = field(default_factory=list)       # {"reason": ..., "row": ...}

    def reject(self, reason: str, row) -> None:
        self.rejected.append({"reason": reason, "row": jsonable(row)})

    def skip(self, reason: str, row) -> None:
        self.skipped.append({"reason": reason, "row": jsonable(row)})

    def summary(self) -> dict:
        return {
            "event_type": self.event_type,
            "events": len(self.events),
            "rejected": dict(Counter(r["reason"] for r in self.rejected)),
            "skipped": dict(Counter(r["reason"] for r in self.skipped)),
        }


# =========================
# 清洗函数：无效输入一律返回 None，由调用方决定 reject 还是允许为空
# =========================
def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() in ("", "None", "null", "NaN", "nan"):
        return True
    return False


def to_symbol(value) -> str | None:
    """代码统一大写、去空格，FMP 写法转成项目内部写法（BRK-B → BRK.B）；格式不对返回 None"""
    if is_missing(value):
        return None
    s = from_fmp_symbol(str(value).strip().upper())
    return s if SYMBOL_PATTERN.match(s) else None


def to_date(value) -> date | None:
    """解析日期（YYYY-MM-DD 或带时间的字符串 / date / datetime）；无效或是占位日期（SENTINEL_DATES）返回 None"""
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        d = value.date()
    elif isinstance(value, date):
        d = value
    else:
        try:
            d = date.fromisoformat(str(value).strip()[:10])
        except ValueError:
            return None
    return None if d in SENTINEL_DATES else d


def to_datetime(value) -> datetime | None:
    if is_missing(value):
        return None
    try:
        return datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def to_decimal(value) -> Decimal | None:
    """转成 Decimal（金额、比例必须精确）；非数字返回 None。用 str() 中转避免 float 的二进制误差"""
    if is_missing(value):
        return None
    try:
        d = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def to_cik(value) -> str | None:
    """CIK 统一补齐成 10 位数字；不是数字返回 None"""
    if is_missing(value):
        return None
    s = str(value).strip()
    return s.zfill(10) if s.isdigit() and len(s) <= 10 else None


def clean_text(value) -> str | None:
    return None if is_missing(value) else str(value).strip()


def status_for(event_date: date, today: date | None = None) -> str:
    """生效日在未来 → announced（已公告）；否则 confirmed（已生效）"""
    return "announced" if event_date > (today or date.today()) else "confirmed"


def jsonable(obj):
    """把原始行变成可以存 jsonb 的对象：NaN → None，日期 → 字符串，Decimal → 字符串"""
    if isinstance(obj, dict):
        return {str(k): jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(v) for v in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if hasattr(obj, "item"):                       # numpy 标量
        return jsonable(obj.item())
    return obj


def payload_of(row) -> dict:
    """原始内容：整理前的那一行，原样保存（只做 JSON 化）"""
    payload = jsonable(row)
    json.dumps(payload)                            # 存不进 jsonb 的在这里就报错
    return payload
