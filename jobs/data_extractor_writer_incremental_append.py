from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max as spark_max
from datetime import datetime
import yaml

# Import the full load function from the other script
from data_extractor_writer_full_load import extract_and_write_to_s3
from mirroring_data_full_load import copy_s3_data
def get_max_date(s3_path, spark):
    """Fetch the maximum date value from an S3 Parquet file."""
    try:
        s3_df = spark.read.parquet(s3_path)
        max_date = s3_df.agg(spark_max("date").alias("max_date")).collect()[0]["max_date"]
        return max_date
    except Exception as e:
        print(f"Could not fetch max date from {s3_path}: {e}")
        return None

def incremental_append():
    """Generalized function to handle multiple tables for incremental or full load."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL to S3") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    # Load secrets from the YAML file
    app_secrets_path = "/opt/bitnami/spark/.secrets"
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # List of tables to process
    tables = ["transactions", "customers", "campaigns"]  # Add table names as needed

    # Define the base paths for S3 storage
    base_path = "s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/current/full_load/tables"
    output_path = "s3a://ketan-staging-bucket/MySQL_DB/test_db/type"
    # Iterate through each table
    for table in tables:
        print(f"Processing table: {table}")
        source_path = f"{base_path}/{table}"
        incremental_output_path = f"{output_path}/incremental/tables/{table}"
        data_exists = False
        attempts = 0
        max_attempts = 3  # Set a maximum number of attempts to avoid infinite loops

        # Get the maximum date from the source path
        max_date = get_max_date(source_path, spark)
        print(f"Max date for table {table}: {max_date}")

        # Retry loop to check for data existence and handle loading
        while not data_exists and attempts < max_attempts:
            try:
                # Attempt to read existing data
                existing_data = spark.read.parquet(source_path)
                print(f"Data found at {source_path}. Proceeding with incremental load for table {table}...")
                data_exists = True  # Exit the loop once data is confirmed
            except Exception as e:
                print(f"No data found at {source_path}. Running full load instead (Attempt {attempts + 1}/{max_attempts}) for table {table}.")
                extract_and_write_to_s3() # Call the full load function directly from the imported module
                copy_s3_data()
                attempts += 1

        # If data exists, proceed with the incremental load
        if data_exists and max_date:
            print(f"Executing incremental query for table: {table}")
            jdbc_url = "jdbc:mysql://mysql:3306/test_db"
            jdbc_properties = {
                "user": "airflow",
                "password": "airflow_password",
                "driver": "com.mysql.cj.jdbc.Driver"
            }

            # Define the query for incremental load
            query = f"""
            SELECT * FROM {table}
            WHERE date > '{max_date}'
            """

            # Fetch the data from MySQL using the query
            try:
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) AS filtered_data") \
                    .option("user", jdbc_properties["user"]) \
                    .option("password", jdbc_properties["password"]) \
                    .option("driver", jdbc_properties["driver"]) \
                    .load()

                print(f"Schema of the data read from MySQL for table {table}:")
                df.printSchema()
                df.show()

                # Write the result to S3
                current_date = datetime.now()
                current_year = current_date.year
                current_month = current_date.month
                current_day = current_date.day

                df = df.withColumn("year", lit(current_year)) \
                    .withColumn("month", lit(f"{current_month:02d}")) \
                    .withColumn("day", lit(f"{current_day:02d}"))

                df.write.partitionBy("year", "month", "day").parquet(incremental_output_path, mode="overwrite")
                print(f"Incremental data written to {incremental_output_path} for table {table}")
            except Exception as e:
                print(f"Failed to load data for table {table} using query: {query}. Error: {e}")
        else:
            print(f"Skipping table {table} as no data exists for incremental load or max_date could not be determined.")


if __name__ == "__main__":
    incremental_append()

# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/data_extractor_writer_incremental_append.py
