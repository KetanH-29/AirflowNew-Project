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

    # Paths
    source_path_staging = f"s3a://ketan-staging-bucket/MySQL_DB/test_db/random_data/{date_folder}"
    destination_path_staging = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/random_data/previous/current/{date_folder}"
    destination_path_mirror = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/random_data/previous/{date_folder}"

    # Step 1: Check and copy data from source_path_staging to destination_path_staging
    try:
        print(f"Attempting to read data from {source_path_staging}...")
        df_staging = spark.read.parquet(source_path_staging)
        print("Data found. Copying to destination...")
        df_staging.write.mode("overwrite").parquet(destination_path_staging)
        print(f"Data successfully copied to {destination_path_staging}")
    except AnalysisException:
        print(f"Data is absent at {source_path_staging}. Skipping process.")
        return

    # Step 2: Check and copy data from destination_path_staging to destination_path_mirror
    try:
        print(f"Attempting to read data from {destination_path_staging}...")
        df_mirror = spark.read.parquet(destination_path_staging)
        print("Data found. Copying to final destination...")
        df_mirror.write.mode("overwrite").parquet(destination_path_mirror)
        print(f"Data successfully copied to {destination_path_mirror}")
    except AnalysisException:
        print(f"Data has not been copied from the source_staging path. Skipping process.")

if __name__ == "__main__":
    copy_s3_data()

# Command to run:
# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/mirroring_data.py
