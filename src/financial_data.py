import os
import pandas as pd
from companies import get_companies_df
from data_utils import get_financial_data
from gcs_utils import upload_bytes_to_gcs
from dotenv import load_dotenv

load_dotenv()

# Environment variables
COMPANIES_FILE = os.getenv("COMPANIES_FILE")
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")
    

def main(is_test):
    # Get the companies DataFrame
    companies_df = get_companies_df()

    # Fetch and upload financial data
    financial_data = get_financial_data(companies_df, ERROR_LOG_FILE, period_type="quarter", is_test=is_test)

    if financial_data:
        for data_type, buffers in financial_data.items():
            for symbol, buffer in buffers.items():
                file_path = f"raw/{data_type}/{symbol}/{data_type}.parquet"
                upload_bytes_to_gcs(buffer, file_path)
        print("Uploaded in-memory Parquet files to GCS successfully.")
    else:
        print("Failed to fetch financial data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Financial Data Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
