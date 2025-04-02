import os
import time
import json
import pandas as pd
from vnstock import Vnstock, Company
from dotenv import load_dotenv
from datetime import datetime

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

def get_company_info(companies_df, file_path, is_test=True):
    """Fetch and save detailed company information."""
    print('Start collecting company info')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode
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

def get_officers(companies_df, file_path, is_test=True):
    """Fetch and save officers' data."""
    print('Start collecting officers data')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode
    for symbol in companies_df['symbol']:
        company = Company(symbol=symbol)
        try:
            officers = company.officers()
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

def get_shareholders(companies_df, file_path, is_test=True):
    """Fetch and save shareholders' data."""
    print('Start collecting shareholders data')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode
    for symbol in companies_df['symbol']:
        company = Company(symbol=symbol)
        try:
            # Fetch shareholders data
            shareholders = company.shareholders()
            
            # Convert shareholders to a DataFrame
            shareholders_df = pd.DataFrame(shareholders)
            
            # Add the symbol column to the DataFrame
            shareholders_df['symbol'] = symbol
            
            # Reorder columns to make 'symbol' the first column
            columns = ['symbol'] + [col for col in shareholders_df.columns if col != 'symbol']
            shareholders_df = shareholders_df[columns]
            
            # Save to CSV
            if not os.path.exists(file_path):
                shareholders_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                shareholders_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Shareholders data for {symbol} successfully written to {file_path}")
            time.sleep(3)
        except Exception as e:
            print(f"Error fetching shareholders data for {symbol}: {e}")

def get_dividends(companies_df, file_path, is_test=True):
    """Fetch and save dividends data."""
    print('Start collecting dividends data')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode
    for symbol in companies_df['symbol']:
        company = Company(symbol=symbol)
        try:
            # Fetch dividends data
            dividends = company.dividends()
            
            # Convert dividends to a DataFrame
            dividends_df = pd.DataFrame(dividends)
            
            # Add the symbol column to the DataFrame
            dividends_df['symbol'] = symbol
            
            # Reorder columns to make 'symbol' the first column
            columns = ['symbol'] + [col for col in dividends_df.columns if col != 'symbol']
            dividends_df = dividends_df[columns]
            
            # Save to CSV
            if not os.path.exists(file_path):
                dividends_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                dividends_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Dividends data for {symbol} successfully written to {file_path}")
            time.sleep(3)
        except Exception as e:
            print(f"Error fetching dividends data for {symbol}: {e}")

def get_stock_quote_history(companies_df, file_path, start_date="2020-01-01", end_date=None, is_test=True):
    """Fetch and save stock quote history data."""
    print('Start collecting stock quote history data')
    
    # Set default end_date to today's date if not provided
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    
    # Limit to the first 3 companies if in test mode
    if is_test:
        companies_df = companies_df.head(3)
    
    for symbol in companies_df['symbol']:
        try:
            # Fetch stock quote history
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            quote_history = stock.quote.history(start=start_date, end=end_date)
            
            # Convert to DataFrame
            quote_history_df = pd.DataFrame(quote_history)
            
            # Add the symbol column to the DataFrame
            quote_history_df['symbol'] = symbol
            
            # Reorder columns to make 'symbol' the first column
            columns = ['symbol'] + [col for col in quote_history_df.columns if col != 'symbol']
            quote_history_df = quote_history_df[columns]
            
            # Save to CSV
            if not os.path.exists(file_path):
                quote_history_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                quote_history_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Stock quote history for {symbol} successfully written to {file_path}")
            
            # Sleep for 30 seconds before fetching the next company
            time.sleep(30)
        except Exception as e:
            print(f"Error fetching stock quote history for {symbol}: {e}")