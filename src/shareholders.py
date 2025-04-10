import os
import pandas as pd
import argparse
from data_utils import get_shareholders, get_companies
from dotenv import load_dotenv

load_dotenv()

# Environment variables
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_SHAREHOLDERS_FILE = os.getenv("COMPANY_SHAREHOLDER_FILE")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")
def main(is_test):
    if not os.path.exists(COMPANIES_FILE):
        companies_df = get_companies(COMPANIES_FILE, ERROR_LOG_FILE)
    else:
        companies_df = pd.read_csv(COMPANIES_FILE, encoding="utf-8")
    
    get_shareholders(companies_df, COMPANY_SHAREHOLDERS_FILE, ERROR_LOG_FILE, is_test)
    print("Shareholders data fetched and stored.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Shareholders Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
