import os
from ultis.data_utils import get_dividends
from ultis.gcs_utils import upload_bytes_to_gcs
from companies import get_companies_df
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")

def main(is_test):
    companies_df = get_companies_df()

    # Fetch dividends data grouped by symbol and year
    buffers = get_dividends(companies_df, ERROR_LOG_FILE, is_test)

    if buffers:
        for (symbol, year), buffer in buffers.items():
            file_path = f"raw/dividends/{symbol}/dividends_{year}.parquet"
            upload_bytes_to_gcs(buffer, file_path)
        print("Uploaded in-memory Parquet files to GCS successfully.")
    else:
        print("Failed to fetch any dividends data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Dividends Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
