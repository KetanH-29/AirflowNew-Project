from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import yaml


def copy_s3_data():
    """Copy data between two S3 directories with conditions using Spark."""

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("S3 Conditional Data Copy") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")
    app_secrets_path = "/opt/bitnami/spark/.secrets"

    # Load AWS credentials from secrets file
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # Get today's date for dynamic path generation
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    # Define the date-based folder structure
    date_folder = f"year={year}/month={month:02d}/day={day:02d}/"

    # List of table names to automate
    table_names = ["transactions", "customers", "campaigns"]

    # Iterate over table names to dynamically create paths and copy data
    for table_name in table_names:
        # Define source, staging, and mirror paths dynamically
        source_path = f"s3a://ketan-staging-bucket/MySQL_DB/test_db/type/incremental/tables/{table_name}/{date_folder}"
        mirror_previous_path = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/incremental/tables/{table_name}/{date_folder}"

        try:
            # Step 1: Check and copy data from source to staging
            print(f"Attempting to read data from {source_path}...")
            df_source = spark.read.parquet(source_path)
            print("Data found. Copying to staging...")
            df_source.show()
            df_source.write.mode("overwrite").parquet(mirror_previous_path)
            print(f"Data successfully copied to {mirror_previous_path}")
        except AnalysisException:
            print(f"Data is absent at {source_path}. Skipping process.")
            continue

if __name__ == "__main__":
    copy_s3_data()

# Command to run:
# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/mirroring_data_incremental_append.py
