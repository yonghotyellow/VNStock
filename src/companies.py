import os
import pandas as pd
from data_utils import get_companies
from dotenv import load_dotenv

load_dotenv()

# Environment variables
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")

def main():
    companies_df = get_companies(COMPANIES_FILE, ERROR_LOG_FILE)
    print("Companies fetched and stored.")

if __name__ == "__main__":
    main()
