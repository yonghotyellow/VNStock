import os
from dotenv import load_dotenv
from data_utils import get_companies
from gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs, get_gcs_client

load_dotenv()

ERROR_LOG_FILE = os.getenv("ERROR_LOG_FILE")

def get_companies_df():
    try:
        client = get_gcs_client()
        companies_df = load_parquet_from_gcs("raw/companies/companies.parquet", client=client)
    except Exception as e:
        print(f"⚠️ Failed to load from GCS. Fetching from source instead.\n{e}")
        companies_df = get_companies(ERROR_LOG_FILE)
    
    return companies_df

def main():
    parquet_buffer = get_companies(ERROR_LOG_FILE)
    if parquet_buffer:
        client = get_gcs_client()
        upload_bytes_to_gcs(parquet_buffer, "raw/companies/companies.parquet", client=client)
        print("Companies data uploaded to GCS successfully.")

if __name__ == "__main__":
    main()
