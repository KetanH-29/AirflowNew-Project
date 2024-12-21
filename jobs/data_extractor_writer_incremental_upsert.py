from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import yaml

# Import the full load function from the other script
from data_extractor_writer_incremental_append import incremental_append
from mirroring_data_incremental_append import copy_s3_data

def incremental_upsert():
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
    base_staging_path = "s3a://ketan-staging-bucket/MySQL_DB/test_db/type/incremental/tables"
    base_previous_path = "s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/incremental/tables"
    output_path = "s3a://ketan-staging-bucket/MySQL_DB/test_db/type"

    # Iterate through each table
    for table in tables:
        print(f"Processing table: {table}")
        source_staging_path = f"{base_staging_path}/{table}"
        source_previous_path = f"{base_previous_path}/{table}"
        upsert_output_path = f"{output_path}/upsert/tables/{table}"

        data_exists_in_staging = False
        data_exists_in_previous = False

        # Check if data exists in staging path (partition-aware)
        try:
            staging_data = spark.read.parquet(source_staging_path)
            data_exists_in_staging = True
            print(f"Data found in staging path: {source_staging_path}")
        except Exception as e:
            print(f"No data found in staging path: {source_staging_path}. Error: {e}")

        # Check if data exists in previous path (partition-aware)
        try:
            previous_data = spark.read.parquet(source_previous_path)
            data_exists_in_previous = True
            print(f"Data found in previous path: {source_previous_path}")
        except Exception as e:
            print(f"No data found in previous path: {source_previous_path}. Error: {e}")

        # If data exists in both paths, proceed with incremental load
        if data_exists_in_staging and data_exists_in_previous:
            print(f"Data found in both staging and previous paths for table: {table}. Proceeding with incremental load...")

            # Perform an anti-join (using Spark SQL or DataFrame API)
            try:
                # Register DataFrames as temporary views
                staging_data.createOrReplaceTempView("staging_table")
                previous_data.createOrReplaceTempView("previous_table")

                # Anti-join SQL query
                query = f"""
                SELECT *
                FROM staging_table s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM previous_table p
                    WHERE {" AND ".join([f"s.{col} = p.{col}" for col in staging_data.columns])}
                )
                """
                incremental_data = spark.sql(query)

                # Check if incremental data is not empty and write to output path
                if incremental_data.count() > 0:
                    # Get current date for partitioning
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = current_date.month
                    current_day = current_date.day

                    # Add year, month, and day columns for partitioning
                    incremental_data = incremental_data.withColumn("year", lit(current_year)) \
                        .withColumn("month", lit(f"{current_month:02d}")) \
                        .withColumn("day", lit(f"{current_day:02d}"))

                    # Write data to S3 with partitioning
                    incremental_data.write.partitionBy("year", "month", "day") \
                        .parquet(upsert_output_path, mode="append")
                    print(f"Incremental data successfully written to {upsert_output_path} for table {table}")
                else:
                    print(f"No new incremental data found for table {table}. Skipping write operation.")
            except Exception as e:
                print(f"Error during incremental load for table {table}: {e}")
        else:
            print(f"Data missing in either or both paths for table {table}. Running full load instead.")
            incremental_append()  # Call the full load function directly
            copy_s3_data()  # Run additional copy logic if needed

if __name__ == "__main__":
    incremental_upsert()


# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/data_extractor_writer_incremental_upsert.py


