import os
import pandas as pd
from data_utils import get_companies, get_company_info, get_officers

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Get environment variables
DATA_DIR = os.getenv("DATA_DIR")
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_INFO_FILE = os.getenv("COMPANY_INFO_FILE")
COMPANY_OFFICERS_FILE = os.getenv("COMPANY_OFFICER_FILE")

os.makedirs(DATA_DIR, exist_ok=True)

print(f"Data Directory: {DATA_DIR}")

if __name__ == "__main__":
    # Fetch and save companies
    if not os.path.exists(COMPANIES_FILE):
        companies_df = get_companies(COMPANIES_FILE)
    else:
        companies_df = pd.read_csv(COMPANIES_FILE, encoding='utf-8')

    # Fetch and save company info
    get_company_info(companies_df, COMPANY_INFO_FILE)

    # Fetch and save officers data
    get_officers(companies_df, COMPANY_OFFICERS_FILE)