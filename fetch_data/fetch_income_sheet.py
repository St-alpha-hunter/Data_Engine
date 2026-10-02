import numpy as np
from fetch_data.basic_clean import CleanBasic

class IncomeSheetYear(CleanBasic):

    endpoint_name = "income_sheet_yearly"

    config = {
            "required_columns" :[
                "date","symbol","reportedCurrency","cik",
                "filingDate","acceptedDate","fiscalYear","period",
                "revenue","costOfRevenue","grossProfit",
                "researchAndDevelopmentExpenses","generalAndAdministrativeExpenses",
                "sellingAndMarketingExpenses","sellingGeneralAndAdministrativeExpenses",
                "otherExpenses","operatingExpenses","costAndExpenses",
                "netInterestIncome","interestIncome","interestExpense",
                "depreciationAndAmortization","ebitda","ebit",
                "nonOperatingIncomeExcludingInterest","operatingIncome",
                "totalOtherIncomeExpensesNet","incomeBeforeTax",
                "incomeTaxExpense","netIncomeFromContinuingOperations",
                "netIncomeFromDiscontinuedOperations","otherAdjustmentsToNetIncome",
                "netIncome","netIncomeDeductions","bottomLineNetIncome",
                "eps","epsDiluted","weightedAverageShsOut","weightedAverageShsOutDil"
            ],

        "numeric_columns":[
            "revenue",
            "costOfRevenue",
            "grossProfit",
            "researchAndDevelopmentExpenses",
            "generalAndAdministrativeExpenses",
            "sellingAndMarketingExpenses",
            "sellingGeneralAndAdministrativeExpenses",
            "otherExpenses",
            "operatingExpenses",
            "costAndExpenses",
            "netInterestIncome",
            "interestIncome",
            "interestExpense",
            "depreciationAndAmortization",
            "ebitda",
            "ebit",
            "nonOperatingIncomeExcludingInterest",
            "operatingIncome",
            "totalOtherIncomeExpensesNet",
            "incomeBeforeTax",
            "incomeTaxExpense",
            "netIncomeFromContinuingOperations",
            "netIncomeFromDiscontinuedOperations",
            "otherAdjustmentsToNetIncome",
            "netIncome",
            "netIncomeDeductions",
            "bottomLineNetIncome",
            "eps",
            "epsDiluted",
            "weightedAverageShsOut",
            "weightedAverageShsOutDil"
        ],
        "date_columns":[
            "date","filingDate","acceptedDate","fiscalYear"
        ],
        "drop_columns":[
            "date","symbol"
        ],
        "duplicate_columns":[
            "date","symbol"
        ]
    }


    def _custom_validate(self):
        self._check_positive_value()
        self._check_logic_relation()
        self._compute_navigate_rate()

    def _check_positive_value(self):
        check_positive_item = [ i for i in self.config["numeric_columns"]]
        mask = (self.raw_df[check_positive_item] < 0).any(axis=1)
        self._add_error(mask, "abnormal")

    def _check_logic_relation(self):
        mask = ~(
            np.isclose(
                self.raw_df["grossProfit"],
                self.raw_df["revenue"]-self.raw_df["costOfRevenue"],
                atol=2)|
            np.isclose(
                self.raw_df["operatingIncome"],
                self.raw_df["grossProfit"]-self.raw_df["operatingExpenses"],
                atol=2)|
            np.isclose(
                       self.raw_df["incomeBeforeTax"],
                       self.raw_df["operatingIncome"] + self.raw_df["totalOtherIncomeExpensesNet"],
                       atol=2)|
            np.isclose(
                       self.raw_df["netIncome"],
                       self.raw_df["incomeBeforeTax"]-self.raw_df["incomeTaxExpense"],
                       atol=2)|
            np.isclose(
                self.raw_df["operatingExpenses"],
                self.raw_df["researchAndDevelopmentExpenses"]+self.raw_df["sellingGeneralAndAdministrativeExpenses"]+self.raw_df["sellingGeneralAndAdministrativeExpenses"],
                atol=2)
        )

        self._add_error(mask,"logic_error")

    # checks = {
    #     "gross_profit": grossProfit == revenue - costOfRevenue,
    #     "operating_income": operatingIncome == grossProfit - operatingExpenses,
    #     "income_before_tax": incomeBeforeTax == operatingIncome + totalOtherIncomeExpensesNet,
    #     "net_income": netIncome == incomeBeforeTax - incomeTaxExpense,
    # }

    # operatingExpenses
    # ≈ researchAndDevelopmentExpenses
    # + sellingGeneralAndAdministrativeExpenses
    # + otherExpenses
    # #

    ######先写到这里


    # costAndExpenses
    # ≈ costOfRevenue + operatingExpenses
    #
    # ebitda
    # ≈ ebit + depreciationAndAmortization
    #
    # eps
    # ≈ netIncome / weightedAverageShsOut
    #
    # epsDiluted
    # ≈ netIncome / weightedAverageShsOutDil
    def _compute_navigate_rate(self):
        pass

    def build_stats(self):
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
		"revenue": 391035000000,
		"costOfRevenue": 210352000000,
		"grossProfit": 180683000000,
		"researchAndDevelopmentExpenses": 31370000000,
		"generalAndAdministrativeExpenses": 0,
		"sellingAndMarketingExpenses": 0,
		"sellingGeneralAndAdministrativeExpenses": 26097000000,
		"otherExpenses": 0,
		"operatingExpenses": 57467000000,
		"costAndExpenses": 267819000000,
		"netInterestIncome": 0,
		"interestIncome": 0,
		"interestExpense": 0,
		"depreciationAndAmortization": 11445000000,
		"ebitda": 134661000000,
		"ebit": 123216000000,
		"nonOperatingIncomeExcludingInterest": 0,
		"operatingIncome": 123216000000,
		"totalOtherIncomeExpensesNet": 269000000,
		"incomeBeforeTax": 123485000000,
		"incomeTaxExpense": 29749000000,
		"netIncomeFromContinuingOperations": 93736000000,
		"netIncomeFromDiscontinuedOperations": 0,
		"otherAdjustmentsToNetIncome": 0,
		"netIncome": 93736000000,
		"netIncomeDeductions": 0,
		"bottomLineNetIncome": 93736000000,
		"eps": 6.11,
		"epsDiluted": 6.08,
		"weightedAverageShsOut": 15343783000,
		"weightedAverageShsOutDil": 15408095000
	}
]


"""
