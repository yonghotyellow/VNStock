import os
from utils.data_utils import get_shareholders
from utils.gcs_utils import upload_bytes_to_gcs
from companies import get_companies_df
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")
def main(is_test):
    companies_df = get_companies_df()

    # Fetch shareholders' data and prepare it as a Parquet buffer
    parquet_buffer = get_shareholders(companies_df, ERROR_LOG_FILE, is_test)
    if parquet_buffer:
        # Upload the Parquet buffer to GCS
        upload_bytes_to_gcs(parquet_buffer, "raw/shareholders/shareholders.parquet")
        print("Shareholders data uploaded to GCS successfully.")
    else:
        print("Failed to fetch shareholders data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Shareholders Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
