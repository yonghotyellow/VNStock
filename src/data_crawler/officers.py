import os
from data_utils import get_officers
from gcs_utils import upload_bytes_to_gcs, get_gcs_client
from companies import get_companies_df
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")
IS_TEST = os.getenv("IS_TEST", "True").lower() in ("true", "1", "t")
def main(is_test):
    companies_df = get_companies_df()

    # Fetch officers' data and prepare it as a Parquet buffer
    parquet_buffer = get_officers(companies_df, ERROR_LOG_FILE, is_test)
    if parquet_buffer:
        client = get_gcs_client()
        # Upload the Parquet buffer to GCS
        upload_bytes_to_gcs(parquet_buffer, "raw/officers/officers.parquet", client=client)
        print("Officers data uploaded to GCS successfully.")
    else:
        print("Failed to fetch officers data.")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run the Officers Service pipeline.")
    # parser.add_argument("--test", action=argparse.BooleanOptionalAction, default=True,
    #                     help="Run in test mode (default: True).")
    # args = parser.parse_args()
    main(is_test=IS_TEST)
