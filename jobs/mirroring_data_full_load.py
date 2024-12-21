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
    source_path_staging0 = f"s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/transactions/{date_folder}"
    source_path_staging1 = f"s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/customers/{date_folder}"
    source_path_staging2 = f"s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/campaigns/{date_folder}"
    destination_path_current_mirror0 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/current/full_load/tables/transactions/{date_folder}"
    destination_path_current_mirror1 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/current/full_load/tables/customers/{date_folder}"
    destination_path_current_mirror2 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/current/full_load/tables/campaigns/{date_folder}"
    destination_path_mirror0 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/full_load/tables/transactions/{date_folder}"
    destination_path_mirror1 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/full_load/tables/customers/{date_folder}"
    destination_path_mirror2 = f"s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/full_load/tables/campaigns/{date_folder}"

    # Step 1: Check and copy data from source_path_staging to destination_path_staging
    try:
        print(f"Attempting to read data from {source_path_staging0}...")
        df_staging0 = spark.read.parquet(source_path_staging0)
        print("Data found. Copying to destination...")
        df_staging0.show()
        df_staging0.write.mode("overwrite").parquet(destination_path_current_mirror0)
        print(f"Data successfully copied to {destination_path_current_mirror0}")
    except AnalysisException:
        print(f"Data is absent at {source_path_staging0}. Skipping process.")
        return
    try:
        print(f"Attempting to read data from {source_path_staging1}...")
        df_staging1 = spark.read.parquet(source_path_staging1)
        print("Data found. Copying to destination...")
        df_staging1.show()
        df_staging1.write.mode("overwrite").parquet(destination_path_current_mirror1)
        print(f"Data successfully copied to {destination_path_current_mirror1}")
    except AnalysisException:
        print(f"Data is absent at {source_path_staging1}. Skipping process.")
        return
    try:
        print(f"Attempting to read data from {source_path_staging2}...")
        df_staging2 = spark.read.parquet(source_path_staging2)
        print("Data found. Copying to destination...")
        df_staging2.show()
        df_staging2.write.mode("overwrite").parquet(destination_path_current_mirror2)
        print(f"Data successfully copied to {destination_path_current_mirror2}")
    except AnalysisException:
        print(f"Data is absent at {source_path_staging2}. Skipping process.")
        return

    # Step 2: Check and copy data from destination_path_staging to destination_path_mirror
    try:
        print(f"Attempting to read data from {destination_path_current_mirror0}...")
        df_mirror0 = spark.read.parquet(destination_path_current_mirror0)
        print("Data found. Copying to final destination...")
        df_mirror0.show()
        df_mirror0.write.mode("overwrite").parquet(destination_path_mirror0)
        print(f"Data successfully copied to {destination_path_mirror0}")
    except AnalysisException:
        print(f"Data has not been copied from the source_staging path0. Skipping process.")

    try:
        print(f"Attempting to read data from {destination_path_current_mirror1}...")
        df_mirror1 = spark.read.parquet(destination_path_current_mirror1)
        print("Data found. Copying to final destination...")
        df_mirror1.show()
        df_mirror1.write.mode("overwrite").parquet(destination_path_mirror1)
        print(f"Data successfully copied to {destination_path_mirror1}")
    except AnalysisException:
        print(f"Data has not been copied from the source_staging path1. Skipping process.")

    try:
        print(f"Attempting to read data from {destination_path_current_mirror2}...")
        df_mirror2 = spark.read.parquet(destination_path_current_mirror2)
        print("Data found. Copying to final destination...")
        df_mirror2.show()
        df_mirror2.write.mode("overwrite").parquet(destination_path_mirror2)
        print(f"Data successfully copied to {destination_path_mirror2}")
    except AnalysisException:
        print(f"Data has not been copied from the source_staging path2. Skipping process.")

if __name__ == "__main__":
    copy_s3_data()

# Command to run:
# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/mirroring_data_full_load.py
