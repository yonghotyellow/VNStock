from gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs, get_gcs_client, list_parquet_files_in_gcs
from spark_session import get_spark
import io
import pandas as pd

if __name__ == "__main__":
    # Initialize Spark session
    spark = get_spark("Dividends Data", key_path="../../gcs_credentials.json")

    # Define base path for raw dividends data
    base_path = "raw/dividends"

    # Initialize GCS client
    client = get_gcs_client('../../gcs_credentials.json')

    # Get the list of all .parquet files in raw/dividends
    parquet_files = list_parquet_files_in_gcs(base_path, client=client)
    count = 0
    for file in parquet_files:
        try:
            count += 1
            if count == 50:
                break
            # Load Parquet file from GCS into a Pandas DataFrame
            pandas_df = load_parquet_from_gcs(file, client=client)

            # Convert Pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df)

            # Drop duplicate rows
            df_cleaned = spark_df.dropDuplicates()

            # Convert the cleaned Spark DataFrame back to Pandas DataFrame
            cleaned_pandas_df = df_cleaned.toPandas()

            # Convert the cleaned Pandas DataFrame to Parquet in memory
            buffer = io.BytesIO()
            cleaned_pandas_df.to_parquet(buffer, index=False)
            buffer.seek(0)

            # Upload the cleaned Parquet file back to GCS
            cleaned_gcs_path = file.replace("raw", "cleaned")
            print(cleaned_gcs_path)
            # upload_bytes_to_gcs(buffer, cleaned_gcs_path, client=client)
            print(f"✅ Cleaned data uploaded to GCS at {cleaned_gcs_path}")

        except Exception as e:
            print(f"❌ Failed to process {file}: {e}")