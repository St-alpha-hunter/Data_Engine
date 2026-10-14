import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FMP_API_KEY")

# config/endpoints.py

SYMBOL_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/refs/heads/main/data/constituents.csv"

# 每个 endpoint 的配置：
#   url                   URL 模板
#   required_params       调用时必须传入的参数
#   source_logic_version  FMP 计算口径版本；FMP 改了口径就 +1，写入 raw 时带上
#   raw_table             原始数据落库的 ClickHouse 表（已建表的接口才有）
#   iterate               请求方式：symbol（默认，每只股票一个请求）/ none（一次请求全市场）/ page（按页翻，遇空页停止）
#   max_rows              单次请求最多返回的行数；返回行数达到它视为可能被截断
#   chunk_days            按 from_date / to_date 分段请求，每段天数（配合 max_rows：顶到上限的段自动对半拆开）
FMP_ENDPOINTS = {
    "symbol": {
        "url":"https://raw.githubusercontent.com/datasets/s-and-p-500-companies/refs/heads/main/data/constituents.csv",
        "required_params":[],
        "source_logic_version": 1
    },
    "price_volume": {
        "url": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted?symbol={symbol}&from={from_date}&to={to_date}&apikey={apikey}",
        "required_params": ["from_date", "to_date"],
        "source_logic_version": 1,
        "raw_table": "raw.price_volume_daily",
        # FMP 单次最多返回 5000 行，超过时只给最近的 5000 行、不报错（实测 KO 全量只回到 2006-11）
        # 所以按 10 年一段请求（约 2520 行）；某段仍顶到上限会自动对半拆开重拉
        "max_rows": 5000,
        "chunk_days": 3650
    },
    # 不复权日线：字段名也叫 adjOpen / adjClose，但实际是当时真实成交的价格（golden 以它为主）
    # 同样有单次 5000 行上限，按 10 年一段请求
    "price_volume_unadjusted": {
        "url": "https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted?symbol={symbol}&from={from_date}&to={to_date}&apikey={apikey}",
        "required_params": ["from_date", "to_date"],
        "source_logic_version": 1,
        "raw_table": "raw.price_volume_unadjusted_daily",
        "max_rows": 5000,
        "chunk_days": 3650
    },
    # 历史市值（每天一行）；原来的 market-capitalization 只返回今天一行，字段相同
    # 和日线一样有单次 5000 行上限，按 10 年一段请求
    "market_cap": {
        "url": "https://financialmodelingprep.com/stable/historical-market-capitalization?symbol={symbol}&from={from_date}&to={to_date}&limit=5000&apikey={apikey}",
        "required_params": ["from_date", "to_date"],
        "source_logic_version": 1,
        "raw_table": "raw.market_cap",
        "max_rows": 5000,
        "chunk_days": 3650
    },
    "cash_flow": {
        "url": "https://financialmodelingprep.com/stable/cash-flow-statement?symbol={symbol}&period={period}&limit={limit}&apikey={apikey}",
        "required_params": ["period", "limit"],
        "source_logic_version": 1,
        "raw_table": "raw.cash_flow"
    },
    "income_statement": {
        "url": "https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&period={period}&limit={limit}&apikey={apikey}",
        "required_params": ["period", "limit"],
        "source_logic_version": 1,
        "raw_table": "raw.income_statement"
    },
    "balance_sheet": {
        "url": "https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={symbol}&period={period}&limit={limit}&apikey={apikey}",
        "required_params": ["period", "limit"],
        "source_logic_version": 1,
        "raw_table": "raw.balance_sheet"
    },
    # 当前套餐只能拉年报（period=quarter 返回 402）
    "financial_ratio":{
        "url": "https://financialmodelingprep.com/stable/ratios?symbol={symbol}&period=annual&limit={limit}&apikey={apikey}",
        "required_params":["limit"],
        "source_logic_version": 1,
        "raw_table": "raw.financial_ratio"
    },
    # 只拉年度：季度也能拉，但返回数据里没有 period 字段，和年度的混在一起分不开
    "enterprise_values":{
        "url":"https://financialmodelingprep.com/stable/enterprise-values?symbol={symbol}&period=annual&limit={limit}&apikey={apikey}",
        "required_params":["limit"],
        "source_logic_version": 1,
        "raw_table": "raw.enterprise_values"
    },
    "executive_compensation":{
        "url":"https://financialmodelingprep.com/stable/governance-executive-compensation?symbol={symbol}&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1,
        "raw_table": "raw.executive_compensation"
    },
    "all_shares_float":{
        "url":"https://financialmodelingprep.com/stable/shares-float-all?page=0&limit=1000&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1
    },
    "company_profile":{
        "url":"https://financialmodelingprep.com/stable/profile-cik?cik={cik}&apikey={apikey}",
        "required_params":["cik"],
        "source_logic_version": 1,
        "raw_table": "raw.profile"
    },
    # 只拉年度预测（返回数据里没有 period 字段，年度和季度混在一起分不开）；limit 最多 10
    "financial_estimate":{
        "url":"https://financialmodelingprep.com/stable/analyst-estimates?symbol={symbol}&period=annual&page={page}&limit={limit}&apikey={apikey}",
        "required_params":["page","limit"],
        "source_logic_version": 1,
        "raw_table": "raw.financial_estimate"
    },
    "analyst_grades":{
        "url":"https://financialmodelingprep.com/stable/grades-historical?symbol={symbol}&limit=1000&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1,
        "raw_table": "raw.analyst_grades"
    },
    # ===== 证券主表 =====
    # 公司概况（按股票查）：CIK、ISIN、CUSIP、上市日期、交易所、是否 ADR / ETF / 仍在交易
    # 注意：只有当前状态；代码被重复使用时返回的是现在用这个代码的公司（如 FB 现在是一只 ETF）
    "profile":{
        "url":"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1,
        "raw_table": "raw.profile"
    },
    # ===== 公司事件 =====
    # 按股票查全部历史（补历史用）
    "dividends":{
        "url":"https://financialmodelingprep.com/stable/dividends?symbol={symbol}&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1
    },
    "splits":{
        "url":"https://financialmodelingprep.com/stable/splits?symbol={symbol}&apikey={apikey}",
        "required_params":[],
        "source_logic_version": 1
    },
    # 日历（每天增量用）：单次最多返回 4000 条，每次只查 1 天；当前套餐只能查最近约一年和未来
    "dividends_calendar":{
        "url":"https://financialmodelingprep.com/stable/dividends-calendar?from={from_date}&to={to_date}&apikey={apikey}",
        "required_params":["from_date","to_date"],
        "iterate": "none",
        "source_logic_version": 1
    },
    "splits_calendar":{
        "url":"https://financialmodelingprep.com/stable/splits-calendar?from={from_date}&to={to_date}&apikey={apikey}",
        "required_params":["from_date","to_date"],
        "iterate": "none",
        "source_logic_version": 1
    },
    # 全市场代码变更：一次返回全部历史
    "symbol_change":{
        "url":"https://financialmodelingprep.com/stable/symbol-change?limit=100000&apikey={apikey}",
        "required_params":[],
        "iterate": "none",
        "source_logic_version": 1
    },
    # 并购：按页往前翻（每页 100 条），翻到空页停止
    "mergers_acquisitions":{
        "url":"https://financialmodelingprep.com/stable/mergers-acquisitions-latest?page={page}&limit=100&apikey={apikey}",
        "required_params":[],
        "iterate": "page",
        "source_logic_version": 1
    },
    # 交易所节假日：这里的 symbol 传交易所名（NYSE / NASDAQ）
    # 注意 FMP 这个接口的 from 不含当天（from=2015-01-01 会漏掉元旦），调用时 from 要往前放一天
    "holidays_by_exchange":{
        "url":"https://financialmodelingprep.com/stable/holidays-by-exchange?exchange={symbol}&from={from_date}&to={to_date}&apikey={apikey}",
        "required_params":["from_date","to_date"],
        "source_logic_version": 1
    }
}