import os
from dotenv import load_dotenv
from data_crawler.ultis.data_utils import get_companies
from data_crawler.ultis.gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs

load_dotenv()

ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")

def get_companies_df():
    try:
        companies_df = load_parquet_from_gcs("raw/companies/companies.parquet")
    except Exception as e:
        print(f"⚠️ Failed to load from GCS. Fetching from source instead.\n{e}")
        companies_df = get_companies(ERROR_LOG_FILE)
    
    return companies_df

def main():
    parquet_buffer = get_companies(ERROR_LOG_FILE)
    if parquet_buffer:
        upload_bytes_to_gcs(parquet_buffer, "raw/companies/companies.parquet")

if __name__ == "__main__":
    main()
