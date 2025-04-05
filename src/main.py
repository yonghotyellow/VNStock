import os
import pandas as pd
import argparse
from data_utils import (
    get_companies,
    get_company_info,
    get_officers,
    get_shareholders,
    get_dividends,
    get_stock_quote_history,
    get_income_statement  # Import the new method
)
from dotenv import load_dotenv
load_dotenv()

# Get environment variables
DATA_DIR = os.getenv("DATA_DIR")
LOG_DIR = os.getenv("LOG_DIR")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_INFO_FILE = os.getenv("COMPANY_INFO_FILE")
COMPANY_OFFICERS_FILE = os.getenv("COMPANY_OFFICER_FILE")
COMPANY_SHAREHOLDERS_FILE = os.getenv("COMPANY_SHAREHOLDER_FILE")
COMPANY_DIVIDENDS_FILE = os.getenv("COMPANY_DIVIDENDS_FILE")
COMPANY_STOCK_QUOTE_FILE = os.getenv("COMPANY_STOCK_QUOTE_FILE")
COMPANY_INCOME_STATEMENT_FILE = os.getenv("COMPANY_INCOME_STATEMENT_FILE")  # New variable

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def main(is_test):
    # Fetch and save companies
    if not os.path.exists(COMPANIES_FILE):
        companies_df = get_companies(COMPANIES_FILE, ERROR_LOG_FILE)
    else:
        companies_df = pd.read_csv(COMPANIES_FILE, encoding='utf-8')

    # Fetch and save company info
    get_company_info(companies_df, COMPANY_INFO_FILE, ERROR_LOG_FILE, is_test)

    # Fetch and save officers data
    get_officers(companies_df, COMPANY_OFFICERS_FILE, ERROR_LOG_FILE, is_test)

    # Fetch and save shareholders data
    get_shareholders(companies_df, COMPANY_SHAREHOLDERS_FILE, ERROR_LOG_FILE, is_test)

    # Fetch and save dividends data
    get_dividends(companies_df, COMPANY_DIVIDENDS_FILE, ERROR_LOG_FILE, is_test)

    # Fetch and save stock quote history
    get_stock_quote_history(companies_df, COMPANY_STOCK_QUOTE_FILE, ERROR_LOG_FILE, start_date="2020-01-01", is_test=is_test)

    # Fetch and save income statement data
    get_income_statement(companies_df, COMPANY_INCOME_STATEMENT_FILE, ERROR_LOG_FILE, quarter=True, is_test=is_test)

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Run the DEOps_Project pipeline.")
    parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True, help="Run in test mode (default: True).")
    args = parser.parse_args()

    # Execute the main function with the test mode argument
    main(is_test=args.test)