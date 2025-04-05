import os
import pandas as pd
import argparse
from data_utils import (
    get_income_statement,
    get_balance_sheet,
    get_cash_flow,
    get_ratio,
    get_companies
)
from dotenv import load_dotenv

load_dotenv()

# Environment variables
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_INCOME_STATEMENT_FILE = os.getenv("COMPANY_INCOME_STATEMENT_FILE")
COMPANY_BALANCE_SHEET_FILE = os.getenv("COMPANY_BALANCE_SHEET_FILE")
COMPANY_CASH_FLOW_FILE = os.getenv("COMPANY_CASH_FLOW_FILE")
COMPANY_RATIO_FILE = os.getenv("COMPANY_RATIO_FILE")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")

def main(is_test):
    if not os.path.exists(COMPANIES_FILE):
        companies_df = get_companies(COMPANIES_FILE, ERROR_LOG_FILE)
    else:
        companies_df = pd.read_csv(COMPANIES_FILE, encoding="utf-8")
    
    # Execute each financial data fetching function
    get_income_statement(companies_df, COMPANY_INCOME_STATEMENT_FILE, ERROR_LOG_FILE, quarter=True, is_test=is_test)
    get_balance_sheet(companies_df, COMPANY_BALANCE_SHEET_FILE, ERROR_LOG_FILE, quarter=True, is_test=is_test)
    get_cash_flow(companies_df, COMPANY_CASH_FLOW_FILE, ERROR_LOG_FILE, quarter=True, is_test=is_test)
    get_ratio(companies_df, COMPANY_RATIO_FILE, ERROR_LOG_FILE, quarter=True, is_test=is_test)
    
    print("Financial data fetched and stored.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Financial Data Service pipeline.")
    parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
                        help="Run in test mode (default: True).")
    args = parser.parse_args()
    main(is_test=args.test)
