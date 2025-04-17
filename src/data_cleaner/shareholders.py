from gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs, get_gcs_client
from spark_session import get_spark
import io
import pandas as pd

if __name__ == "__main__":
    # Initialize Spark session
    spark = get_spark("Shareholder Data", key_path="../../gcs_credentials.json")
    gcs_path = "raw/shareholders/shareholders.parquet"

    # Load Parquet from GCS into Pandas
    client = get_gcs_client('../../gcs_credentials.json')
    pandas_df = load_parquet_from_gcs(gcs_path, client=client)

    # Convert Pandas to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    # Filter out rows where share_own_percent == 0 OR share_holder == "Khác"
    df_cleaned = spark_df.filter(
        (spark_df["share_own_percent"] != 0.0) & (spark_df["share_holder"] != "Khác")
    )

    # Show counts
    print(f"Original count: {spark_df.count()}")
    print(f"Cleaned count: {df_cleaned.count()}")
    print(f"Removed count: {spark_df.count() - df_cleaned.count()}")

    # Convert to Pandas -> Parquet buffer -> Upload
    pdf_cleaned = df_cleaned.toPandas()
    # print(pdf_cleaned.head(50))
    buffer = io.BytesIO()
    pdf_cleaned.to_parquet(buffer, index=False)
    buffer.seek(0)

    cleaned_gcs_path = "cleaned/shareholders/shareholders.parquet"
    upload_bytes_to_gcs(buffer, cleaned_gcs_path, client=client)
    print(f"Cleaned data uploaded to GCS at {cleaned_gcs_path}")
