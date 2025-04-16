from pyspark.sql import SparkSession

def get_spark(app_name="DataCleaner"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = get_spark()
    print(f"Spark app '{spark.sparkContext.appName}' started successfully.")
    print(f"Spark version: {spark.version}")
    spark.stop()