import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FMP_API_KEY")

# config/endpoints.py

SYMBOL_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/refs/heads/main/data/constituents.csv"

FMP_ENDPOINTS = {
    "symbol": {
        "url":"https://raw.githubusercontent.com/datasets/s-and-p-500-companies/refs/heads/main/data/constituents.csv",
        "required_params":[]
    },
    "price_volume": {
        "url": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted?symbol={symbol}&from_date={from_date}&to_date={to_date}&apikey={apikey}",
        "required_params": ["from_date", "to_date"],
    },
    "market_cap": {
        "url": "https://financialmodelingprep.com/stable/market-capitalization?symbol={symbol}&apikey={apikey}",
        "required_params": [],
    },
    "cash_flow": {
        "url": "https://financialmodelingprep.com/stable/cash-flow-statement?symbol={symbol}&apikey={apikey}",
        "required_params": [],
    },
    "income_statement": {
        "url": "https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&apikey={apikey}",
        "required_params": [],
    },
    "balance_sheet": {
        "url": "https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={symbol}&apikey={apikey}",
        "required_params": [],
    },
    "financial_ratio":{
        "url": "https://financialmodelingprep.com/stable/ratios?symbol={symbol}&apikey={apikey}",
        "required_params":[]
    },
    "enterprise_values":{
        "url":"https://financialmodelingprep.com/stable/enterprise-values?symbol={symbol}&apikey={apikey}",
        "required_params":[]
    },
    "executive_compensation":{
        "url":"https://financialmodelingprep.com/stable/governance-executive-compensation?symbol={symbol}&apikey={apikey}",
        "required_params":[]
    },
    "all_shares_float":{
        "url":"https://financialmodelingprep.com/stable/shares-float-all?page=0&limit=1000&apikey={apikey}",
        "required_params":[]
    },
    "company_profile":{
        "url":"https://financialmodelingprep.com/stable/profile-cik?cik={cik}&apikey={apikey}",
        "required_params":["cik"]
    },
    "financial_estimate":{
        "url":"https://financialmodelingprep.com/stable/analyst-estimates?symbol={symbol}&period={period}&page={page}&limit={limit}&apikey={apikey}",
        "required_params":["period","page","limit"]
    },
    "analyst_grades":{
        "url":"https://financialmodelingprep.com/stable/grades-historical?symbol={symbol}&apikey={apikey}",
        "required_params":[]
    }
}