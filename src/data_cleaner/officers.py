from ultis.gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs
from ultis.spark_session import get_spark


if __name__ == "__main__":
    spark = get_spark("Officer Data")
    gcs_path = "raw/officers/officers.parquet"
    
    # Load the parquet file from GCS
    df = load_parquet_from_gcs(spark, gcs_path)
    
    # Show the first 50 lines
    df.show(50, truncate=False)