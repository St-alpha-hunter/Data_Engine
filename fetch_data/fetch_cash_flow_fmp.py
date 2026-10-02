import numpy as np
from fetch_data.basic_clean import CleanBasic


class CashFlowYear(CleanBasic):

    endpoint_name = "cash_flow_yearly"

    config = {
        "required_columns":[
            "date",
            "symbol",
            "reportedCurrency",
            "cik",
            "filingDate",
            "acceptedDate",
            "fiscalYear",
            "period",
            "netIncome",
            "depreciationAndAmortization",
            "deferredIncomeTax",
            "stockBasedCompensation",
            "changeInWorkingCapital",
            "accountsReceivables",
            "inventory",
            "accountsPayables",
            "otherWorkingCapital",
            "otherNonCashItems",
            "netCashProvidedByOperatingActivities",
            "investmentsInPropertyPlantAndEquipment",
            "acquisitionsNet",
            "purchasesOfInvestments",
            "salesMaturitiesOfInvestments",
            "otherInvestingActivities",
            "netCashProvidedByInvestingActivities",
            "netDebtIssuance",
            "longTermNetDebtIssuance",
            "shortTermNetDebtIssuance",
            "netStockIssuance",
            "netCommonStockIssuance",
            "commonStockIssuance",
            "commonStockRepurchased",
            "netPreferredStockIssuance",
            "netDividendsPaid",
            "commonDividendsPaid",
            "preferredDividendsPaid",
            "otherFinancingActivities",
            "netCashProvidedByFinancingActivities",
            "effectOfForexChangesOnCash",
            "netChangeInCash",
            "cashAtEndOfPeriod",
            "cashAtBeginningOfPeriod",
            "operatingCashFlow",
            "capitalExpenditure",
            "freeCashFlow",
            "incomeTaxesPaid",
            "interestPaid"
        ],
        "numeric_columns":[
            "netIncome",
            "depreciationAndAmortization",
            "deferredIncomeTax",
            "stockBasedCompensation",
            "changeInWorkingCapital",
            "accountsReceivables",
            "inventory",
            "accountsPayables",
            "otherWorkingCapital",
            "otherNonCashItems",
            "netCashProvidedByOperatingActivities",
            "investmentsInPropertyPlantAndEquipment",
            "acquisitionsNet",
            "purchasesOfInvestments",
            "salesMaturitiesOfInvestments",
            "otherInvestingActivities",
            "netCashProvidedByInvestingActivities",
            "netDebtIssuance",
            "longTermNetDebtIssuance",
            "shortTermNetDebtIssuance",
            "netStockIssuance",
            "netCommonStockIssuance",
            "commonStockIssuance",
            "commonStockRepurchased",
            "netPreferredStockIssuance",
            "netDividendsPaid",
            "commonDividendsPaid",
            "preferredDividendsPaid",
            "otherFinancingActivities",
            "netCashProvidedByFinancingActivities",
            "effectOfForexChangesOnCash",
            "netChangeInCash",
            "cashAtEndOfPeriod",
            "cashAtBeginningOfPeriod",
            "operatingCashFlow",
            "capitalExpenditure",
            "freeCashFlow",
            "incomeTaxesPaid",
            "interestPaid"
        ],
        "date_columns":[
            "date", "filingDate", "acceptedDate", "fiscalYear"
        ],
        "drop_columns":[

        ],
        "duplicate_columns":[
            "date", "symbol","cik"
        ]
    }

    def _custom_validate(self):
        self._check_positive_price()
        self._check_negative_price()
        self._check_logic_relation()
        self._check_navigate_rate()

    def _check_positive_price(self):
        self.positive_columns = [
            "depreciationAndAmortization",
            "stockBasedCompensation",
            "salesMaturitiesOfInvestments",
            "commonStockIssuance",
            "cashAtEndOfPeriod",
            "cashAtBeginningOfPeriod",
            "incomeTaxesPaid",
            "interestPaid",
        ]
        mask = ~(self.raw_df[self.positive_columns] < 0).any(axis=1)
        self._add_error(mask, "abnormal")

    def _check_negative_price(self):
        self.negative_columns = [
                "investmentsInPropertyPlantAndEquipment",
                "capitalExpenditure",
                "acquisitionsNet",
                "purchasesOfInvestments",
                "commonStockRepurchased",
                "commonDividendsPaid",
                "preferredDividendsPaid",
                "netDividendsPaid",
            ]
        mask =~(self.raw_df[self.negative_columns] > 0).any(axis=1)
        self._add_error(mask, "abnormal")

    def _check_logic_relation(self):
        tol = 2

        mask_operating = ~np.isclose(
            self.raw_df["netCashProvidedByOperatingActivities"],
            self.raw_df["netIncome"]
            + self.raw_df["depreciationAndAmortization"]
            + self.raw_df["deferredIncomeTax"]
            + self.raw_df["stockBasedCompensation"]
            + self.raw_df["changeInWorkingCapital"]
            + self.raw_df["otherNonCashItems"],
            atol=tol
        )

        mask_investing = ~np.isclose(
            self.raw_df["netCashProvidedByInvestingActivities"],
            self.raw_df["investmentsInPropertyPlantAndEquipment"]
            + self.raw_df["acquisitionsNet"]
            + self.raw_df["purchasesOfInvestments"]
            + self.raw_df["salesMaturitiesOfInvestments"]
            + self.raw_df["otherInvestingActivities"],
            atol=tol
        )

        mask_financing = ~np.isclose(
            self.raw_df["netCashProvidedByFinancingActivities"],
            self.raw_df["netDebtIssuance"]
            + self.raw_df["netStockIssuance"]
            + self.raw_df["netDividendsPaid"]
            + self.raw_df["otherFinancingActivities"],
            atol=tol
        )

        mask_cash_change = ~np.isclose(
            self.raw_df["netChangeInCash"],
            self.raw_df["netCashProvidedByOperatingActivities"]
            + self.raw_df["netCashProvidedByInvestingActivities"]
            + self.raw_df["netCashProvidedByFinancingActivities"]
            + self.raw_df["effectOfForexChangesOnCash"],
            atol=tol
        )

        mask_cash_end = ~np.isclose(
            self.raw_df["cashAtEndOfPeriod"],
            self.raw_df["cashAtBeginningOfPeriod"]
            + self.raw_df["netChangeInCash"],
            atol=tol
        )

        mask_fcf = ~np.isclose(
            self.raw_df["freeCashFlow"],
            self.raw_df["operatingCashFlow"]
            + self.raw_df["capitalExpenditure"],
            atol=tol
        )

        mask = (
                mask_operating
                | mask_investing
                | mask_financing
                | mask_cash_change
                | mask_cash_end
                | mask_fcf
        )

        self._add_error(mask, "cashflow_logic_relation_error")

    def _check_navigate_rate(self):
        pass



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
                "netIncome": 93736000000,
                "depreciationAndAmortization": 11445000000,
                "deferredIncomeTax": 0,
                "stockBasedCompensation": 11688000000,
                "changeInWorkingCapital": 3651000000,
                "accountsReceivables": -5144000000,
                "inventory": -1046000000,
                "accountsPayables": 6020000000,
                "otherWorkingCapital": 3821000000,
                "otherNonCashItems": -2266000000,
                "netCashProvidedByOperatingActivities": 118254000000,
                "investmentsInPropertyPlantAndEquipment": -9447000000,
                "acquisitionsNet": 0,
                "purchasesOfInvestments": -48656000000,
                "salesMaturitiesOfInvestments": 62346000000,
                "otherInvestingActivities": -1308000000,
                "netCashProvidedByInvestingActivities": 2935000000,
                "netDebtIssuance": -5998000000,
                "longTermNetDebtIssuance": -9958000000,
                "shortTermNetDebtIssuance": 3960000000,
                "netStockIssuance": -94949000000,
                "netCommonStockIssuance": -94949000000,
                "commonStockIssuance": 0,
                "commonStockRepurchased": -94949000000,
                "netPreferredStockIssuance": 0,
                "netDividendsPaid": -15234000000,
                "commonDividendsPaid": -15234000000,
                "preferredDividendsPaid": 0,
                "otherFinancingActivities": -5802000000,
                "netCashProvidedByFinancingActivities": -121983000000,
                "effectOfForexChangesOnCash": 0,
                "netChangeInCash": -794000000,
                "cashAtEndOfPeriod": 29943000000,
                "cashAtBeginningOfPeriod": 30737000000,
                "operatingCashFlow": 118254000000,
                "capitalExpenditure": -9447000000,
                "freeCashFlow": 108807000000,
                "incomeTaxesPaid": 26102000000,
                "interestPaid": 0
            }
        ]
    """