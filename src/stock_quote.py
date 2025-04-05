import os
import pandas as pd
import argparse
from datetime import datetime
from data_utils import get_stock_quote_history, get_companies
from dotenv import load_dotenv

load_dotenv()

# Environment variables
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_STOCK_QUOTE_FILE = os.getenv("COMPANY_STOCK_QUOTE_FILE")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")

def main(is_test):
    if not os.path.exists(COMPANIES_FILE):
        companies_df = get_companies(COMPANIES_FILE, ERROR_LOG_FILE)
    else:
        companies_df = pd.read_csv(COMPANIES_FILE, encoding="utf-8")
    
    end_date = datetime.today().strftime("%Y-%m-%d")
    get_stock_quote_history(companies_df, COMPANY_STOCK_QUOTE_FILE, ERROR_LOG_FILE, start_date="2020-01-01", end_date=end_date, is_test=is_test)
    print("Stock quote history fetched and stored.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Stock Quote Service pipeline.")
    parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
                        help="Run in test mode (default: True).")
    args = parser.parse_args()
    main(is_test=args.test)
