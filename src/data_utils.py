import os
import time
import json
import pandas as pd
from vnstock import Vnstock, Company
from datetime import datetime
from functools import wraps

def log_error(err_file_path, message):
    """Log errors with timestamp to the specified error file."""
    with open(err_file_path, "a", encoding="utf-8") as err_file:
        err_file.write(f"{datetime.now()} - {message}\n")

def write_or_append_csv(df, file_path):
    """Write a DataFrame to a CSV file; create or append based on file existence."""
    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False, encoding='utf-8')
    else:
        df.to_csv(file_path, mode="a", header=False, index=False, encoding='utf-8')

def init_json_file(file_path):
    """Initialize a JSON file for appending objects."""
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("[")

def append_json(file_path, data, is_last=False):
    """Append a JSON object to a file and close the JSON array if needed."""
    # If file exists, remove the trailing "]" if present.
    if os.path.exists(file_path):
        with open(file_path, "rb+") as f:
            f.seek(-1, os.SEEK_END)
            last_char = f.read(1)
            if last_char == b"]":
                f.seek(-1, os.SEEK_END)
                f.truncate()
                f.write(b",\n")
    else:
        init_json_file(file_path)

    with open(file_path, "a", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        if is_last:
            f.write("]")
        else:
            f.write(",\n")

def retry_on_error(func):
    """
    Decorator to retry a function up to 3 times with delays of 10, 30, and 60 seconds
    if an exception is raised.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        delays = [10, 30, 60]
        for attempt, delay in enumerate([0] + delays):
            try:
                if attempt > 0:
                    # Delay before retrying (if not the first attempt)
                    time.sleep(delay)
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < len(delays):
                    # Log the retry attempt
                    error_message = f"Retry {attempt + 1} for {func.__name__} due to error: {e}"
                    print(error_message)
                    log_error(args[-1], error_message)
                    # log_error(, error_message)
                else:
                    raise e
    return wrapper

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

@retry_on_error
def fetch_company_info(symbol, err_file_path):
    """Fetch company overview and profile data for a symbol."""
    company = Company(symbol=symbol)
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
    return info

def get_company_info(companies_df, file_path, err_file_path, is_test=True):
    """Fetch and save detailed company information."""
    print('Start collecting company info')
    if is_test:
        companies_df = companies_df.head(10)  # Limit to the first 10 rows if in test mode

    init_json_file(file_path)
    num_symbols = len(companies_df['symbol'])
    for idx, symbol in enumerate(companies_df['symbol']):
        try:
            info = fetch_company_info(symbol, err_file_path)
            # Check if it's the last item to properly close the JSON array
            is_last = (idx == num_symbols - 1)
            append_json(file_path, info, is_last=is_last)
            print(f"Company info for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)
    # If file wasn't properly closed, ensure the JSON array is closed.
    with open(file_path, "a", encoding="utf-8") as f:
        f.write("]")

@retry_on_error
def fetch_officers(symbol, err_file_path):
    """Fetch officers data for a symbol."""
    company = Company(symbol=symbol)
    return company.officers()

def get_officers(companies_df, file_path, err_file_path, is_test=True):
    """Fetch and save officers' data."""
    print('Start collecting officers data')
    if is_test:
        companies_df = companies_df.head(10)
    for symbol in companies_df['symbol']:
        try:
            officers = fetch_officers(symbol, err_file_path)
            officers_df = pd.DataFrame(officers)
            officers_df['symbol'] = symbol
            cols = ['symbol'] + [col for col in officers_df.columns if col != 'symbol']
            officers_df = officers_df[cols]
            write_or_append_csv(officers_df, file_path)
            print(f"Officers data for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching officers data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

@retry_on_error
def fetch_shareholders(symbol, err_file_path):
    """Fetch shareholders data for a symbol."""
    company = Company(symbol=symbol)
    return company.shareholders()

def get_shareholders(companies_df, file_path, err_file_path, is_test=True):
    """Fetch and save shareholders' data."""
    print('Start collecting shareholders data')
    if is_test:
        companies_df = companies_df.head(10)
    for symbol in companies_df['symbol']:
        try:
            shareholders = fetch_shareholders(symbol, err_file_path)
            shareholders_df = pd.DataFrame(shareholders)
            shareholders_df['symbol'] = symbol
            cols = ['symbol'] + [col for col in shareholders_df.columns if col != 'symbol']
            shareholders_df = shareholders_df[cols]
            write_or_append_csv(shareholders_df, file_path)
            print(f"Shareholders data for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching shareholders data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

@retry_on_error
def fetch_dividends(symbol, err_file_path):
    """Fetch dividends data for a symbol."""
    company = Company(symbol=symbol)
    return company.dividends()

def get_dividends(companies_df, file_path, err_file_path, is_test=True):
    """Fetch and save dividends data."""
    print('Start collecting dividends data')
    if is_test:
        companies_df = companies_df.head(10)
    for symbol in companies_df['symbol']:
        try:
            dividends = fetch_dividends(symbol, err_file_path)
            dividends_df = pd.DataFrame(dividends)
            dividends_df['symbol'] = symbol
            cols = ['symbol'] + [col for col in dividends_df.columns if col != 'symbol']
            dividends_df = dividends_df[cols]
            write_or_append_csv(dividends_df, file_path)
            print(f"Dividends data for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching dividends data for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

@retry_on_error
def fetch_stock_quote_history(symbol, start_date, end_date, err_file_path):
    """Fetch stock quote history data for a symbol."""  
    stock = Vnstock().stock(symbol=symbol, source='VCI')
    quote_history = stock.quote.history(start=start_date, end=end_date)
    return pd.DataFrame(quote_history)


def get_stock_quote_history(companies_df, file_path, err_file_path, start_date="2020-01-01", end_date=None, is_test=True):
    """Fetch and save stock quote history data."""
    print('Start collecting stock quote history data')
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if is_test:
        companies_df = companies_df.head(3)
    for symbol in companies_df['symbol']:
        try:
            quote_history_df = fetch_stock_quote_history(symbol, start_date, end_date, err_file_path)
            quote_history_df['symbol'] = symbol
            cols = ['symbol'] + [col for col in quote_history_df.columns if col != 'symbol']
            quote_history_df = quote_history_df[cols]
            write_or_append_csv(quote_history_df, file_path)
            print(f"Stock quote history for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching stock quote history for {symbol}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)


@retry_on_error
def fetch_with_retry(fetch_func, symbol, period, lang, err_file_path):
    """Retry fetching data with exponential backoff."""
    stock = Vnstock().stock(symbol=symbol, source='VCI')
    return fetch_func(stock, period=period, lang=lang)

def get_financial_data(func_fetch, companies_df, file_path, err_file_path, period_type="quarter", is_test=True):
    """
    Generalized function to fetch and save financial data (income statement, balance sheet,
    cash flow, financial ratio) for a list of companies.
    """
    print(f"Start collecting financial data using {func_fetch.__name__}")
    if is_test:
        companies_df = companies_df.head(10)
    for symbol in companies_df['symbol']:
        try:
            # stock = Vnstock().stock(symbol=symbol, source='VCI')
            data = fetch_with_retry(func_fetch, symbol, period=period_type, lang='vi', err_file_path=err_file_path)
            df = pd.DataFrame(data)
            write_or_append_csv(df, file_path)
            print(f"Financial data for {symbol} successfully written to {file_path}")
        except Exception as e:
            error_message = f"Error fetching financial data for {symbol} using {func_fetch.__name__}: {e}"
            log_error(err_file_path, error_message)
            print(error_message)

def get_income_statement(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save income statement data."""
    period = 'quarter' if quarter else 'year'
    get_financial_data(lambda stock, period, lang: stock.finance.income_statement(period=period, lang=lang),
                       companies_df, file_path, err_file_path, period_type=period, is_test=is_test)

def get_balance_sheet(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save balance sheet data."""
    period = 'quarter' if quarter else 'year'
    get_financial_data(lambda stock, period, lang: stock.finance.balance_sheet(period=period, lang=lang),
                       companies_df, file_path, err_file_path, period_type=period, is_test=is_test)

def get_cash_flow(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save cash flow data."""
    period = 'quarter' if quarter else 'year'
    get_financial_data(lambda stock, period, lang: stock.finance.cash_flow(period=period, lang=lang),
                       companies_df, file_path, err_file_path, period_type=period, is_test=is_test)

def get_ratio(companies_df, file_path, err_file_path, quarter=True, is_test=True):
    """Fetch and save financial ratio data."""
    period = 'quarter' if quarter else 'year'
    get_financial_data(lambda stock, period, lang: stock.finance.ratio(period=period, lang=lang),
                       companies_df, file_path, err_file_path, period_type=period, is_test=is_test)
