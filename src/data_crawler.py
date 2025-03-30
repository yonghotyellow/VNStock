import pandas as pd
from vnstock import Vnstock, Listing, Quote, Company, Finance, Trading, Screener 
import os
import time
import json

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get environment variables
DATA_DIR = os.getenv("DATA_DIR")
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
COMPANY_INFO_FILE = os.getenv("COMPANY_INFO_FILE")

os.makedirs(DATA_DIR, exist_ok=True)

print(f"Data Directory: {DATA_DIR}")

stock = Vnstock().stock(symbol='ACB', source='VCI')
companies = pd.DataFrame(stock.listing.symbols_by_exchange())

companies_df = companies[(companies['exchange'] == 'HSX') & (companies['type'] == 'STOCK')]
companies_df = companies_df.drop(columns=['organ_short_name', 'organ_name'], axis=1)
#saving to csv
print(companies_df.head(5))
companies_df.to_csv(COMPANIES_FILE, index=False, encoding='utf-8')
print('Successfully saved companies.csv file')
time.sleep(5)

# Read companies from CSV
companies_df = pd.read_csv(COMPANIES_FILE, encoding='utf-8')

# Start collecting company info
print('Start collecting company info')

# Open the JSON file in write mode and start the JSON array
with open(COMPANY_INFO_FILE, "w", encoding="utf-8") as f:
    f.write("[")

for idx, symbol in enumerate(companies_df['symbol']):
    company = Company(symbol=symbol)
    try:
        # Fetch company data
        overview = company.overview()
        profile = company.profile()
        info = {
            "symbol": symbol,
            "exchange": overview.get("exchange")[0],
            "industry": overview.get("industry")[0],
            "company_type": overview.get("company_type")[0],
            "established_year": overview.get("established_year")[0],
            "stock_rating": overview.get("stock_rating")[0],
            "short_name": overview.get("short_name")[0],
            "website": overview.get("website")[0],
            "company_name": profile.get("company_name")[0],
            "company_profile": profile.get("company_profile")[0],
            "history_dev": profile.get("history_dev")[0],
            "company_promise": profile.get("company_promise")[0],
            "business_risk": profile.get("business_risk")[0],
            "key_developments": profile.get("key_developments")[0],
            "business_strategies": profile.get("business_strategies")[0],
        }

        # Write the current company's info to the JSON file
        with open(COMPANY_INFO_FILE, "a", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=4)
            if idx < len(companies_df['symbol']) - 1:
                f.write(",\n")  # Add a comma except for the last item

        print(f"Successfully fetched data for {symbol}")
        time.sleep(3)  # Rate limiting

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

# Close the JSON array
with open(COMPANY_INFO_FILE, "a", encoding="utf-8") as f:
    f.write("]")