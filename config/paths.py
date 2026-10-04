from pathlib import Path

# from yfinance.scrapers.fundamentals import Financials

BASE_DIR = Path(__file__).resolve().parent.parent
#把路径字符串变成 pathlib.Path 对象，并转成绝对路径
DATA_DIR = BASE_DIR / "data"

##报错存储
ERRORS_REPORT = BASE_DIR/"error_report"

##结构检查不通过、没能进 raw 的批次，原始返回内容存这里排查
RAW_REJECTS = DATA_DIR / "raw_rejects"

##临时股票池
TEM_SYMBOL = DATA_DIR / "stocks_pools/sp500_symbols.txt"

STOCKS_POOL_PATH = DATA_DIR / "stocks_pools/sp500_symbols.parquet"

VOLUME_DATA_PATH = DATA_DIR / "volume_data_1y"

MARKET_CAP_PATH = DATA_DIR / "market_cap_data_1y_fmp"

CASH_FLOW_PATH = DATA_DIR / "cash_flow_statement"

BALANCE_SHEET_PATH = DATA_DIR / "balance_sheet"

INCOME_STAT_PATH = DATA_DIR /"income_statement"

FINANCIALS_PATH = DATA_DIR /"financial_ratio"

ENTERPRISE_PATH = DATA_DIR /"enterprise_value"

EXECUTE_PATH = DATA_DIR /"executive_compensation"

FLOAT_SHARES = DATA_DIR /"all_data_shares"

COMPANY_PROFILE = DATA_DIR /"company_profile"

FINANCIAL_ESTIMATE = DATA_DIR /"financial_estimate"

ANALYST_GRADES = DATA_DIR /"analyst_grades"

##每个 endpoint 的本地数据目录：清洗后的中间态 parquet 按批次存到这里（{目录}/{batch_id}.parquet）
##没在这里登记的 endpoint 存到 data/{endpoint}/
ENDPOINT_DATA_DIRS = {
    "price_volume": VOLUME_DATA_PATH,
    "market_cap": MARKET_CAP_PATH,
    "cash_flow": CASH_FLOW_PATH,
    "balance_sheet": BALANCE_SHEET_PATH,
    "income_statement": INCOME_STAT_PATH,
    "financial_ratio": FINANCIALS_PATH,
    "enterprise_values": ENTERPRISE_PATH,
    "executive_compensation": EXECUTE_PATH,
    "all_shares_float": FLOAT_SHARES,
    "company_profile": COMPANY_PROFILE,
    "financial_estimate": FINANCIAL_ESTIMATE,
    "analyst_grades": ANALYST_GRADES,
}


