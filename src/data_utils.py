import os
import time
import json
import pandas as pd
from vnstock import Vnstock, Company
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_companies(file_path):
    """Fetch and save the list of companies."""
    stock = Vnstock().stock(symbol='ACB', source='VCI')
    companies = pd.DataFrame(stock.listing.symbols_by_exchange())
    companies_df = companies[(companies['exchange'] == 'HSX') & (companies['type'] == 'STOCK')]
    companies_df = companies_df.drop(columns=['organ_short_name', 'organ_name'], axis=1)
    companies_df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Successfully saved companies.csv file to {file_path}")
    return companies_df

def get_company_info(companies_df, file_path):
    """Fetch and save detailed company information."""
    print('Start collecting company info')
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("[")
    for idx, symbol in enumerate(companies_df['symbol']):
        company = Company(symbol=symbol)
        try:
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
            with open(file_path, "a", encoding="utf-8") as f:
                json.dump(info, f, ensure_ascii=False, indent=4)
                if idx < len(companies_df['symbol']) - 1:
                    f.write(",\n")
            print(f"Company info for {symbol} successfully written")
            time.sleep(3)
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    with open(file_path, "a", encoding="utf-8") as f:
        f.write("]")

def get_officers(companies_df, file_path):
    """Fetch and save officers' data."""
    print('Start collecting officers data')
    for symbol in companies_df['symbol']:
        company = Company(symbol=symbol)
        try:
            officers = company.officers()
            # if not officers:
            #     print(f"No officers data found for {symbol}")
            #     continue
            
            officers_df = pd.DataFrame(officers)
            officers_df['symbol'] = symbol
            columns = ['symbol'] + [col for col in officers_df.columns if col != 'symbol']
            officers_df = officers_df[columns]
            if not os.path.exists(file_path):
                officers_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                officers_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            print(f"Officers data for {symbol} successfully written")
            time.sleep(3)
        except Exception as e:
            print(f"Error fetching officers data for {symbol}: {e}")