import time
import numpy as np
import pandas as pd
from datetime import datetime

from config.paths import TEM_SYMBOL
from fetch_data.fetch import FetchData
from monitoring.feed_error_deputy import FeedErrorDeputy
from clean.base_clean import CleanBasic

"""
(1)每天搞一个新的Company Profile，然后去验证
(2)验证changePercentage和change, 每天去做交叉验证
(3)验证range
"""

class CompanyProfile(CleanBasic):

    endpoint_name = "company_profile"

    config = {
        "required_columns": [
            "symbol","price","marketCap",
            "beta","lastDividend","range",
            "change","changePercentage","volume",
            "averageVolume","companyName","currency",
            "cik","isin","cusip","exchangeFullName","exchange",
            "industry","website","description","ceo",
            "sector","country","fullTimeEmployees",
            "phone","address","city",
            "state","zip","image",
            "ipoDate","defaultImage",
            "isEtf","isActivelyTrading","isAdr","isFund"
        ],

        "numeric_columns":[
            "price",
            "beta",
            "lastDividend",
            "change",
            "changePercentage",
            "volume",
            "averageVolume"
        ],

        "date_columns":[
            "ipoDate"
        ],

    "duplicate_columns": [
            "cik", "symbol"
        ]
    }

    def _custom_validate(self):
        self._mark_date()
        self._check_positive()

##每天进来之后就mark一下
    def _mark_date(self):
        self.raw_df["date"] = time.strftime('%Y-%m-%d', time.localtime())

    def _check_positive(self):
        check_positive_items = [i for i in self.config["numeric_columns"]]
        mask = ~(self.raw_df[check_positive_items] < 0).any(axis=1)
        self._add_error(mask, "abnormal")




"""
[
	{
		"symbol": "AAPL",
		"price": 262.82,
		"marketCap": 3900351299800,
		"beta": 1.109,
		"lastDividend": 1.04,
		"range": "169.21-265.29",
		"change": 3.24,
		"changePercentage": 1.24817,
		"volume": 36725325,
		"averageVolume": 47424558,
		"companyName": "Apple Inc.",
		"currency": "USD",
		"cik": "0000320193",
		"isin": "US0378331005",
		"cusip": "037833100",
		"exchangeFullName": "NASDAQ Global Select",
		"exchange": "NASDAQ",
		"industry": "Consumer Electronics",
		"website": "https://www.apple.com",
		"description": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. The company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, and HomePod. It also provides AppleCare support and cloud services; and operates various platforms, including the App Store that allow customers to discov...",
		"ceo": "Timothy D. Cook",
		"sector": "Technology",
		"country": "US",
		"fullTimeEmployees": "164000",
		"phone": "(408) 996-1010",
		"address": "One Apple Park Way",
		"city": "Cupertino",
		"state": "CA",
		"zip": "95014",
		"image": "https://images.financialmodelingprep.com/symbol/AAPL.png",
		"ipoDate": "1980-12-12",
		"defaultImage": false,
		"isEtf": false,
		"isActivelyTrading": true,
		"isAdr": false,
		"isFund": false
	}
]

"""