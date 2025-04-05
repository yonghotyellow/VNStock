import os
import time
import json
import pandas as pd
from vnstock import Vnstock, Company
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

def log_error(err_file_path, message):
    """Log errors with timestamp to the specified error file."""
    with open(err_file_path, "a", encoding="utf-8") as err_file:
        err_file.write(f"{datetime.now()} - {message}\n")

def get_companies(file_path, err_file_path):
    """Fetch and save the list of companies."""
    try:
        stock = Vnstock().stock(symbol='ACB', source='VCI')
        companies = pd.DataFrame(stock.listing.symbols_by_exchange())
        companies_df = companies[(companies['exchange'] == 'HSX') & (companies['type'] == 'STOCK')]
        companies_df = companies_df.drop(columns=['organ_short_name', 'organ_name'], axis=1)
        companies_df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"Successfully saved companies.csv file to {file_path}")
        return companies_df
    except Exception as e:
        error_message = f"Error fetching companies: {e}"
        log_error(err_file_path, error_message)
        print(error_message)
        return pd.DataFrame()  # Return an empty DataFrame in case of failure

def get_company_info(companies_df, file_path, err_file_path, is_test=True):
    """Fetch and save detailed company information."""
    print('Start collecting company info')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode

    # Check if the file exists and remove the closing "]"
    if os.path.exists(file_path):
        with open(file_path, "rb+") as f:
            f.seek(-1, os.SEEK_END)  # Move to the last character
            last_char = f.read(1)
            if last_char == b"]":  # Check if the file ends with "]"
                f.seek(-1, os.SEEK_END)  # Move back one character
                f.truncate()  # Remove the "]"
                f.write(b",\n")  # Add a comma to prepare for appending

    else:
        # If the file doesn't exist, create it and write the opening "["
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
            error_message = f"Error fetching data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

    # Add the closing "]" to the JSON file
    with open(file_path, "a", encoding="utf-8") as f:
        f.write("]")

def get_officers(companies_df, file_path, err_file_path, is_test=True):
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
            error_message = f"Error fetching officers data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_shareholders(companies_df, file_path, err_file_path, is_test=True):
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
            error_message = f"Error fetching shareholders data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_dividends(companies_df, file_path, err_file_path, is_test=True):
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
            error_message = f"Error fetching dividends data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_stock_quote_history(companies_df, file_path, err_file_path, start_date="2020-01-01", end_date=None, is_test=True):
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
            
            print(f"Stock quote history for {symbol} successfully written")
            
            # Sleep for 3 seconds before fetching the next company
            time.sleep(3)
        except Exception as e:
            error_message = f"Error fetching stock quote history for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_income_statement(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save income statement data."""
    print('Start collecting income statement data')
    
    # Determine the period based on the quarter parameter
    period = 'quarter' if quarter else 'year'
    
    # Limit to the first 10 companies if in test mode
    if is_test:
        companies_df = companies_df.head(10)
    
    for symbol in companies_df['symbol']:
        try:
            # Fetch income statement data
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            income_statement = stock.finance.income_statement(period=period, lang='vi')
            
            # Convert to DataFrame
            income_statement_df = pd.DataFrame(income_statement)
            
            # Save to CSV
            if not os.path.exists(file_path):
                income_statement_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                income_statement_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Income statement for {symbol} successfully written")
            
            # Sleep for 3 seconds before fetching the next company
            time.sleep(3)
        except Exception as e:
            error_message = f"Error fetching income statement for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_balance_sheet(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save balance sheet data."""
    print('Start collecting balance sheet data')
    
    # Determine the period based on the quarter parameter
    period = 'quarter' if quarter else 'year'
    
    # Limit to the first 10 companies if in test mode
    if is_test:
        companies_df = companies_df.head(10)
    
    for symbol in companies_df['symbol']:
        try:
            # Fetch balance sheet data
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            balance_sheet = stock.finance.balance_sheet(period=period, lang='vi')
            
            # Convert to DataFrame
            balance_sheet_df = pd.DataFrame(balance_sheet)
            
            # Save to CSV
            if not os.path.exists(file_path):
                balance_sheet_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                balance_sheet_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Balance sheet for {symbol} successfully written to {file_path}")
            
            # Sleep for 3 seconds before fetching the next company
            time.sleep(3)
        except Exception as e:
            error_message = f"Error fetching balance sheet for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_cash_flow(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save cash flow data."""
    print('Start collecting cash flow data')
    
    # Determine the period based on the quarter parameter
    period = 'quarter' if quarter else 'year'
    
    # Limit to the first 10 companies if in test mode
    if is_test:
        companies_df = companies_df.head(10)
    
    for symbol in companies_df['symbol']:
        try:
            # Fetch cash flow data
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            cash_flow = stock.finance.cash_flow(period=period, lang='vi')
            
            # Convert to DataFrame
            cash_flow_df = pd.DataFrame(cash_flow)
            
            # Save to CSV
            if not os.path.exists(file_path):
                cash_flow_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                cash_flow_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Cash flow for {symbol} successfully written to {file_path}")
            
            # Sleep for 3 seconds before fetching the next company
            time.sleep(3)
        except Exception as e:
            error_message = f"Error fetching cash flow for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_ratio(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save financial ratio data."""
    print('Start collecting financial ratio data')
    
    # Determine the period based on the quarter parameter
    period = 'quarter' if quarter else 'year'
    
    # Limit to the first 10 companies if in test mode
    if is_test:
        companies_df = companies_df.head(10)
    
    for symbol in companies_df['symbol']:
        try:
            # Fetch financial ratio data
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            ratio = stock.finance.ratio(period=period, lang='vi')
            
            # Convert to DataFrame
            ratio_df = pd.DataFrame(ratio)
            
            # Save to CSV
            if not os.path.exists(file_path):
                ratio_df.to_csv(file_path, index=False, encoding="utf-8")
            else:
                ratio_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
            
            print(f"Financial ratio for {symbol} successfully written to {file_path}")
            
            # Sleep for 3 seconds before fetching the next company
            time.sleep(3)
        except Exception as e:
            error_message = f"Error fetching financial ratio for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)