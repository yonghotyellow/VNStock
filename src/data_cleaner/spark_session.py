from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

CREDENTIALS_PATH = os.getenv("GCS_CREDENTIALS")

def get_spark(app_name="DataCleaner", key_path=None):
    # Use CREDENTIALS_PATH if key_path is not provided
    key_path = key_path or CREDENTIALS_PATH

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .master("local[*]")
        .getOrCreate()
    )