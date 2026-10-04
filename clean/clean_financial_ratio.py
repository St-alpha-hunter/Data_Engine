import numpy as np
import pandas as pd
from datetime import datetime

from config.paths import TEM_SYMBOL
from fetch_data.fetch import FetchData
from monitoring.feed_error_deputy import FeedErrorDeputy
from clean.base_clean import CleanBasic

class FinancialRatio(CleanBasic):

    endpoint_name = "financial_ratio"

    config = {
        "required_columns":[
            'symbol', 
            'date', 
            'fiscalYear', 
            'period', 
            'reportedCurrency', 
            'grossProfitMargin', 
            'ebitMargin',
            'ebitdaMargin',
            'operatingProfitMargin',
            'pretaxProfitMargin',
            'continuousOperationsProfitMargin',
            'netProfitMargin',
            'bottomLineProfitMargin',
            'receivablesTurnover',
            'payablesTurnover',
            'inventoryTurnover',
            'fixedAssetTurnover',
            'assetTurnover',
            'currentRatio',
            'quickRatio',
            'solvencyRatio',
            'cashRatio',
            'priceToEarningsRatio',
            'priceToEarningsGrowthRatio',
            'forwardPriceToEarningsGrowthRatio',
            'priceToBookRatio',
            'priceToSalesRatio',
            'priceToFreeCashFlowRatio',
            'priceToOperatingCashFlowRatio',
            'debtToAssetsRatio',
            'debtToEquityRatio',
            'debtToCapitalRatio',
            'longTermDebtToCapitalRatio',
            'financialLeverageRatio',
            'workingCapitalTurnoverRatio',
            'operatingCashFlowRatio',
            'operatingCashFlowSalesRatio',
            'freeCashFlowOperatingCashFlowRatio',
            'debtServiceCoverageRatio',
            'interestCoverageRatio',
            'shortTermOperatingCashFlowCoverageRatio',
            'operatingCashFlowCoverageRatio',
            'capitalExpenditureCoverageRatio',
            'dividendPaidAndCapexCoverageRatio',
            'dividendPayoutRatio',
            'dividendYield',
            'dividendYieldPercentage',
            'revenuePerShare',
            'netIncomePerShare',
            'interestDebtPerShare',
            'cashPerShare',
            'bookValuePerShare',
            'tangibleBookValuePerShare',
            'shareholdersEquityPerShare',
            'operatingCashFlowPerShare',
            'capexPerShare',
            'freeCashFlowPerShare',
            'netIncomePerEBT',
            'ebtPerEbit',
            'priceToFairValue',
            'debtToMarketCap',
            'effectiveTaxRate',
            'enterpriseValueMultiple'
            ],

        "numeric_columns":[
            'grossProfitMargin', 
            'ebitMargin', 'ebitdaMargin', 
            'operatingProfitMargin', 'pretaxProfitMargin', 
            'continuousOperationsProfitMargin', 'netProfitMargin', 
            'bottomLineProfitMargin', 'receivablesTurnover', 'payablesTurnover', 
            'inventoryTurnover', 'fixedAssetTurnover', 'assetTurnover', 'currentRatio', 
            'quickRatio', 'solvencyRatio', 'cashRatio', 'priceToEarningsRatio',
              'priceToEarningsGrowthRatio', 'forwardPriceToEarningsGrowthRatio', 
              'priceToBookRatio', 'priceToSalesRatio', 'priceToFreeCashFlowRatio', 
              'priceToOperatingCashFlowRatio', 'debtToAssetsRatio', 'debtToEquityRatio', 
              'debtToCapitalRatio', 'longTermDebtToCapitalRatio', 'financialLeverageRatio', 
              'workingCapitalTurnoverRatio', 'operatingCashFlowRatio', 'operatingCashFlowSalesRatio',
                'freeCashFlowOperatingCashFlowRatio', 'debtServiceCoverageRatio',
                  'interestCoverageRatio', 'shortTermOperatingCashFlowCoverageRatio', 
                  'operatingCashFlowCoverageRatio', 'capitalExpenditureCoverageRatio', 
                  'dividendPaidAndCapexCoverageRatio', 'dividendPayoutRatio', 'dividendYield',
                    'dividendYieldPercentage', 'revenuePerShare', 'netIncomePerShare', 
                    'interestDebtPerShare', 'cashPerShare', 'bookValuePerShare', 
                    'tangibleBookValuePerShare', 'shareholdersEquityPerShare', 
                    'operatingCashFlowPerShare', 'capexPerShare', 
                    'freeCashFlowPerShare', 'netIncomePerEBT', 
                    'ebtPerEbit', 'priceToFairValue', 
                    'debtToMarketCap', 'effectiveTaxRate', 'enterpriseValueMultiple'
        ],
        "date_columns":[
            "date","fiscalYear"
        ],
        "drop_columns":[
            "symbol","date"
        ],
        "duplicate_columns":[
            "symbol","date"
        ]

    }

    def _custom_validate(self):
        pass


    def _margin_check(self):
        pass

    def _check_positive_value(self):
        check_positive_item = ['receivablesTurnover',
                'payablesTurnover',
                'inventoryTurnover',
                'fixedAssetTurnover',
                'assetTurnover']
        mask = ~(self.raw_df[check_positive_item]<0).any(axis = 1)
        self._add_error(mask, "abnormal")
        
    def _check_logic_relation(self):
        pass

    def _compute_navigate_rate(self):
        pass

    def build_stats(self):
        pass


"""
[
	{
		"symbol": "AAPL",
		"date": "2024-09-28",
		"fiscalYear": "2024",
		"period": "FY",
		"reportedCurrency": "USD",
		"grossProfitMargin": 0.4620634981523393,
		"ebitMargin": 0.31510222870075566,
		"ebitdaMargin": 0.3443707085043538,
		"operatingProfitMargin": 0.31510222870075566,
		"pretaxProfitMargin": 0.3157901466620635,
		"continuousOperationsProfitMargin": 0.23971255769943867,
		"netProfitMargin": 0.23971255769943867,
		"bottomLineProfitMargin": 0.23971255769943867,
		"receivablesTurnover": 5.903038811648023,
		"payablesTurnover": 3.0503480278422272,
		"inventoryTurnover": 28.870710952511665,
		"fixedAssetTurnover": 8.560310858143607,
		"assetTurnover": 1.0713874732862074,
		"currentRatio": 0.8673125765340832,
		"quickRatio": 0.8260068483831466,
		"solvencyRatio": 0.3414634938155374,
		"cashRatio": 0.16975259648963673,
		"priceToEarningsRatio": 37.287278415656736,
		"priceToEarningsGrowthRatio": -45.93792700808932,
		"forwardPriceToEarningsGrowthRatio": -45.93792700808932,
		"priceToBookRatio": 61.37243774486391,
		"priceToSalesRatio": 8.93822887866815,
		"priceToFreeCashFlowRatio": 32.12256867269569,
		"priceToOperatingCashFlowRatio": 29.55638142954995,
		"debtToAssetsRatio": 0.29215025480848267,
		"debtToEquityRatio": 1.872326602282704,
		"debtToCapitalRatio": 0.6518501763673821,
		"longTermDebtToCapitalRatio": 0.6009110021023125,
		"financialLeverageRatio": 6.408779631255487,
		"workingCapitalTurnoverRatio": -31.099932397502684,
		"operatingCashFlowRatio": 0.6704045534944896,
		"operatingCashFlowSalesRatio": 0.3024128274962599,
		"freeCashFlowOperatingCashFlowRatio": 0.9201126388959359,
		"debtServiceCoverageRatio": 5.024761722304708,
		"interestCoverageRatio": 0,
		"shortTermOperatingCashFlowCoverageRatio": 5.663777000814215,
		"operatingCashFlowCoverageRatio": 1.109022873702276,
		"capitalExpenditureCoverageRatio": 12.517624642743728,
		"dividendPaidAndCapexCoverageRatio": 4.7912969490701345,
		"dividendPayoutRatio": 0.16252026969360758,
		"dividendYield": 0.0043585983369965175,
		"dividendYieldPercentage": 0.43585983369965176,
		"revenuePerShare": 25.484914639368924,
		"netIncomePerShare": 6.109054070954992,
		"interestDebtPerShare": 6.949329249507765,
		"cashPerShare": 4.247388013764271,
		"bookValuePerShare": 3.711600978715614,
		"tangibleBookValuePerShare": 3.711600978715614,
		"shareholdersEquityPerShare": 3.711600978715614,
		"operatingCashFlowPerShare": 7.706965094592383,
		"capexPerShare": 0.6156891035281195,
		"freeCashFlowPerShare": 7.091275991064264,
		"netIncomePerEBT": 0.7590881483581001,
		"ebtPerEbit": 1.0021831580314244,
		"priceToFairValue": 61.37243774486391,
		"debtToMarketCap": 0.03050761336980449,
		"effectiveTaxRate": 0.24091185164189982,
		"enterpriseValueMultiple": 26.524727497716487
	}
]

"""