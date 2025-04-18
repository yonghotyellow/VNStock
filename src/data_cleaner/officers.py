from gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs, get_gcs_client
from spark_session import get_spark
import io
import pandas as pd

if __name__ == "__main__":
    # Initialize Spark session
    spark = get_spark("Officer Data")
    gcs_path = "raw/officers/officers.parquet"

    # Load the Parquet file from GCS into a Pandas DataFrame
    client = get_gcs_client('../../gcs_credentials.json')
    pandas_df = load_parquet_from_gcs(gcs_path, client=client)

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    # Filter out rows where officer_name is "None"
    df_cleaned = spark_df.filter(spark_df["officer_name"] != "None")

    # Show counts before and after cleaning
    print(f"Original count: {spark_df.count()}")
    print(f"Cleaned count: {df_cleaned.count()}")
    print(f"Removed count: {spark_df.count() - df_cleaned.count()}")

    # Convert the cleaned Spark DataFrame back to Parquet in memory
    pdf_cleaned = df_cleaned.toPandas()
    # print(pdf_cleaned.head(50))
    buffer = io.BytesIO()
    pdf_cleaned.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload the cleaned Parquet file back to GCS
    cleaned_gcs_path = "cleaned/officers/officers.parquet"
    upload_bytes_to_gcs(buffer, cleaned_gcs_path, client=client)
    print(f"Cleaned data uploaded to GCS at {cleaned_gcs_path}")

