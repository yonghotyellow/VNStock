import os
from data_utils import get_dividends
from gcs_utils import upload_bytes_to_gcs
from companies import get_companies_df
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")

def main(is_test):
    companies_df = get_companies_df()

    # Fetch dividends data grouped by year
    yearly_buffers = get_dividends(companies_df, ERROR_LOG_FILE, is_test)

    if yearly_buffers:
        for year, buffer in yearly_buffers.items():
            file_path = f"raw/dividends/{year}/dividends.parquet"
            upload_bytes_to_gcs(buffer, file_path)
    else:
        print("Failed to fetch any dividends data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Dividends Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
