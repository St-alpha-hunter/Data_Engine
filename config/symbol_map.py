"""
股票代码写法映射：项目内部统一用点（BRK.B），FMP 用横杠（BRK-B）

    请求 FMP 时：to_fmp_symbol("BRK.B")  → "BRK-B"     （FetchData 按股票请求时自动转换）
    读回数据时：from_fmp_symbol("BRK-B") → "BRK.B"     （整理函数的 to_symbol 自动转换）

只对登记在这里的代码做映射；以后遇到新的类别股，加到 CANONICAL_TO_FMP 里即可
"""

# 项目内部写法 → FMP 写法
CANONICAL_TO_FMP = {
    "BRK.B": "BRK-B",       # 伯克希尔·哈撒韦 B 类股
    "BF.B": "BF-B",         # 百富门 B 类股
}

FMP_TO_CANONICAL = {fmp: canonical for canonical, fmp in CANONICAL_TO_FMP.items()}


def to_fmp_symbol(symbol: str) -> str:
    return CANONICAL_TO_FMP.get(symbol, symbol)


def from_fmp_symbol(symbol: str) -> str:
    return FMP_TO_CANONICAL.get(symbol, symbol)
