import pandas as pd
from vnstock import Vnstock, Listing, Quote, Company, Finance, Trading, Screener 
import os
import time

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Saving companies on HOSE
stock = Vnstock().stock(symbol='ACB', source='VCI')
companies = pd.DataFrame(stock.listing.symbols_by_exchange())

companies_df = companies[(companies['exchange'] == 'HSX') & (companies['type'] == 'STOCK')]
companies_df = companies_df.drop(columns=['organ_short_name', 'organ_name'], axis=1)
#saving to csv
print(companies_df.head(5))
companies_file = os.path.join(DATA_DIR, "companies.csv")
companies_df.to_csv(companies_file, index=False, encoding='utf-8')
print('Successfully saved companies.csv file')
time.sleep(15)
company_info_list = []
for symbol in companies_df['symbol']:
    try:
        company = Company(symbol=symbol)
        overview = company.overview()
        profile = company.profile()

        info = {
            "symbol": symbol,
                "exchange": overview.get("exchange"),
                "industry": overview.get("industry"),
                "company_type": overview.get("company_type"),
                "established_year": overview.get("established_year"),
                "stock_rating": overview.get("stock_rating"),
                "short_name": overview.get("short_name"),
                "website": overview.get("website"),
                "company_name": profile.get("company_name"),
                "company_profile": profile.get("company_profile"),
                "history_dev": profile.get("history_dev"),
                "company_promise": profile.get("company_promise"),
                "business_risk": profile.get("business_risk"),
                "key_developments": profile.get("key_developments"),
                "business_strategies": profile.get("business_strategies"),
        }
        # print(info)
        # break
        company_info_list.append(info)
        print(f'Successfully fetching data from {symbol}')
        time.sleep(5)
    except Exception as e:
        print(f'Error fetching data from {symbol}: {e}')

company_info_file = os.path.join(DATA_DIR, "company_info.csv")
pd.DataFrame(company_info_list).to_csv(company_info_file, index=False, encoding='utf-8')