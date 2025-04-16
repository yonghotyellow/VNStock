from google.cloud import storage
import os
import pandas as pd
from dotenv import load_dotenv
import io

load_dotenv()

CREDENTIALS_PATH = os.getenv("GCS_CREDENTIALS")
BUCKET_NAME = os.getenv("GCS_BUCKET")


def get_gcs_client(credentials_path=None):
    """
    Initialize a GCS client using provided service account credentials or default credentials.
    """
    if credentials_path:
        return storage.Client.from_service_account_json(credentials_path)
    return storage.Client()  # Uses GOOGLE_APPLICATION_CREDENTIALS env var if set


def upload_to_gcs(source_file_path, destination_blob_name, credentials_path=None, client=None):
    """
    Upload a local file to a specified GCS bucket and path.

    Parameters:
        source_file_path (str): Local path to the file to upload.
        destination_blob_name (str): Destination path in GCS bucket.
        credentials_path (str): Optional path to service account JSON.
        client (google.cloud.storage.Client): Optional pre-initialized GCS client.
    """
    if not BUCKET_NAME:
        raise ValueError("❌ GCS_BUCKET not set in environment variables!")

    if client is None:
        client = get_gcs_client(credentials_path or CREDENTIALS_PATH)

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

    print(f"✅ Uploaded {source_file_path} to gs://{BUCKET_NAME}/{destination_blob_name}")


def upload_bytes_to_gcs(byte_buffer, destination_blob_name, credentials_path=None, client=None):
    """
    Upload in-memory bytes (e.g., Parquet) to a specified GCS path.

    Parameters:
        byte_buffer (BytesIO): The in-memory byte stream to upload.
        destination_blob_name (str): Destination path in GCS bucket.
        credentials_path (str): Optional path to service account JSON.
        client (google.cloud.storage.Client): Optional pre-initialized GCS client.
    """
    if not BUCKET_NAME:
        raise ValueError("❌ GCS_BUCKET not set in environment variables!")

    if client is None:
        client = get_gcs_client(credentials_path or CREDENTIALS_PATH)

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(byte_buffer, rewind=True)

    print(f"📤 Uploaded in-memory Parquet to gs://{BUCKET_NAME}/{destination_blob_name}")


def load_parquet_from_gcs(blob_name, credentials_path=None, client=None):
    """
    Load a parquet file from GCS directly into a pandas DataFrame.

    Parameters:
        blob_name (str): The blob path in the GCS bucket.
        credentials_path (str): Optional path to the service account JSON.

    Returns:
        pd.DataFrame: The loaded DataFrame from parquet in memory.
    """
    if not BUCKET_NAME:
        raise ValueError("❌ GCS_BUCKET not set in environment variables!")

    if client is None:
        client = get_gcs_client(credentials_path or CREDENTIALS_PATH)
        
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)

    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer)
    return df
