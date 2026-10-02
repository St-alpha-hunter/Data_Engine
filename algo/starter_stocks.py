import logging
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from config.endpoints import SYMBOL_URL, API_KEY
from config.paths import STOCKS_POOL_PATH

load_dotenv()
log = logging.getLogger(__name__)

if not API_KEY:
    raise ValueError("FMP_API_KEY NOT SET IN")

class StockPoolBuilder:
    """
    构建 S&P 500代表性股票池
    """
    def __init__(self, symbol_url=SYMBOL_URL, save_path=STOCKS_POOL_PATH):
        self.symbol_url = symbol_url
        self.save_path = save_path

    def fetch_symbols(self):
        print("Fetching symbol list from CSV...")
        df = pd.read_parquet(self.symbol_url)


