import numpy as np
from fetch_data.basic_clean import CleanBasic

class BalanceSheetYear(CleanBasic):

    endpoint_name = "balance_sheet_yearly"

    config = {
        "required_columns":[

        ],
        "numeric_columns":[

        ],
        
        "date_columns": [
            "date", "filingDate", "acceptedDate", "fiscalYear"
        ],
        "drop_columns": [
            "date", "symbol"
        ],
        "duplicate_columns": [
            "date", "symbol"
        ]
    }



"""

[
	{
		"date": "2024-09-28",
		"symbol": "AAPL",
		"reportedCurrency": "USD",
		"cik": "0000320193",
		"filingDate": "2024-11-01",
		"acceptedDate": "2024-11-01 06:01:36",
		"fiscalYear": "2024",
		"period": "FY",
		"cashAndCashEquivalents": 29943000000,
		"shortTermInvestments": 35228000000,
		"cashAndShortTermInvestments": 65171000000,
		"netReceivables": 66243000000,
		"accountsReceivables": 33410000000,
		"otherReceivables": 32833000000,
		"inventory": 7286000000,
		"prepaids": 0,
		"otherCurrentAssets": 14287000000,
		"totalCurrentAssets": 152987000000,
		"propertyPlantEquipmentNet": 45680000000,
		"goodwill": 0,
		"intangibleAssets": 0,
		"goodwillAndIntangibleAssets": 0,
		"longTermInvestments": 91479000000,
		"taxAssets": 19499000000,
		"otherNonCurrentAssets": 55335000000,
		"totalNonCurrentAssets": 211993000000,
		"otherAssets": 0,
		"totalAssets": 364980000000,
		"totalPayables": 95561000000,
		"accountPayables": 68960000000,
		"otherPayables": 26601000000,
		"accruedExpenses": 0,
		"shortTermDebt": 20879000000,
		"capitalLeaseObligationsCurrent": 1632000000,
		"taxPayables": 26601000000,
		"deferredRevenue": 8249000000,
		"otherCurrentLiabilities": 50071000000,
		"totalCurrentLiabilities": 176392000000,
		"longTermDebt": 85750000000,
		"deferredRevenueNonCurrent": 10798000000,
		"deferredTaxLiabilitiesNonCurrent": 0,
		"otherNonCurrentLiabilities": 35090000000,
		"totalNonCurrentLiabilities": 131638000000,
		"otherLiabilities": 0,
		"capitalLeaseObligations": 12430000000,
		"totalLiabilities": 308030000000,
		"treasuryStock": 0,
		"preferredStock": 0,
		"commonStock": 83276000000,
		"retainedEarnings": -19154000000,
		"additionalPaidInCapital": 0,
		"accumulatedOtherComprehensiveIncomeLoss": -7172000000,
		"otherTotalStockholdersEquity": 0,
		"totalStockholdersEquity": 56950000000,
		"totalEquity": 56950000000,
		"minorityInterest": 0,
		"totalLiabilitiesAndTotalEquity": 364980000000,
		"totalInvestments": 126707000000,
		"totalDebt": 106629000000,
		"netDebt": 76686000000
	}
]

"""