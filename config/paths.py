from pathlib import Path

# from yfinance.scrapers.fundamentals import Financials

BASE_DIR = Path(__file__).resolve().parent.parent
#把路径字符串变成 pathlib.Path 对象，并转成绝对路径
DATA_DIR = BASE_DIR / "data"

##报错存储
ERRORS_REPORT = BASE_DIR/"error_report"

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


