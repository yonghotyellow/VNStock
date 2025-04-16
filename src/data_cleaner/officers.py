from gcs_utils import upload_bytes_to_gcs, load_parquet_from_gcs, get_gcs_client
from spark_session import get_spark
import io

if __name__ == "__main__":
    spark = get_spark("Officer Data")
    gcs_path = "raw/officers/officers.parquet"
    
    # Load the parquet file from GCS
    client = get_gcs_client('../../gcs_credentials.json')
    df = load_parquet_from_gcs(gcs_path, client=client)
    
    # Show the first 50 lines
    # print(df.head(50))
    print(df.count())

    df_cleaned = df.filter(df["officer_name"] != "None")
    print(df_cleaned.count())

    buffer = io.BytesIO()
    df_cleaned.to_parquet(buffer, index=False)
    buffer.seek(0)

    