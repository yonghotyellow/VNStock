import os
from datetime import datetime
from data_utils import get_stock_quote_history
from companies import get_companies_df
from gcs_utils import upload_bytes_to_gcs
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")

def main(is_test):
    # Get the companies DataFrame
    companies_df = get_companies_df()
    
    # Define the date range
    end_date = datetime.today().strftime("%Y-%m-%d")
    
    # Fetch stock quote history grouped by symbol and year
    buffers = get_stock_quote_history(companies_df, ERROR_LOG_FILE, start_date="2020-01-01", end_date=end_date, is_test=is_test)

    if buffers:
        for (symbol, year), buffer in buffers.items():
            file_path = f"raw/stock_quote/{symbol}/stock_quote_{year}.parquet"
            upload_bytes_to_gcs(buffer, file_path)
        print("Uploaded in-memory Parquet files to GCS successfully.")
    else:
        print("Failed to fetch any stock quote history data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Stock Quote Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
