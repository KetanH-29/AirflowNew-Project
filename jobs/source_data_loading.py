from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max as spark_max
from datetime import datetime
import yaml
import os.path
from pyspark.sql.utils import AnalysisException

def get_max_date(s3_path, spark):
    """Fetch the maximum date value from an S3 Parquet file."""
    try:
        s3_df = spark.read.parquet(s3_path)
        max_date = s3_df.agg(spark_max("date").alias("max_date")).collect()[0]["max_date"]
        return max_date
    except Exception as e:
        print(f"Could not fetch max date from {s3_path}: {e}")
        return None

def full_load(spark, app_conf, table , app_secret):
    """Function to perform full load of data into S3"""

    # Read data from MySQL
    print(f"Loading table: {table}")
    df0 = spark.read.format("jdbc") \
        .option("url", app_secret["mysql_conf"]["jdbc_url"]) \
        .option("dbtable", table) \
        .option("user", app_secret["mysql_conf"]["user"]) \
        .option("password", app_secret["mysql_conf"]["password"]) \
        .option("driver", app_secret["mysql_conf"]["driver"]) \
        .load()

    print(f"Schema of the data read from MySQL for table {table}:")
    df0.printSchema()
    df0.show()

    # Get the current date
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day

    # Add year, month, and day columns for partitioning
    df0 = df0.withColumn("year", lit(current_year)) \
        .withColumn("month", lit(f"{current_month:02d}")) \
        .withColumn("day", lit(f"{current_day:02d}"))

    # Define S3 output path
    output_path0 = app_conf["Full_load"]["source"]["S3_bucket"]["s3_staging_path"] + table + "/"

    # Write data to S3 in Parquet format with partitioning
    df0.write.partitionBy("year", "month", "day").parquet(output_path0, mode="overwrite")
    print(f"Data extracted from MySQL and written to {output_path0} on S3")

    # Get today's date for dynamic path generation
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    # Define the date-based folder structure
    date_folder = f"year={year}/month={month:02d}/day={day:02d}/"

    source_path_staging0_read = app_conf["Full_load"]["source"]["S3_bucket"]["s3_staging_path"] + table + "/" + date_folder
    source_current_mirror_read = app_conf["Full_load"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "current" + "/" + date_folder
    source_current_mirror_write = app_conf["Full_load"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "current" + "/"
    destination_previous_mirror = app_conf["Full_load"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "previous" + "/"

    def handle_mirror_data(spark, source_path_staging0_read, source_current_mirror_read, source_current_mirror_write,destination_previous_mirror):
        """Handle mirroring of data with partitioning by year, month, and day."""
        try:
            # Attempt to read data from source_current_mirror
            print(f"Attempting to read data from {source_current_mirror_read}...")
            df_mirror_current = spark.read.parquet(source_current_mirror_read)
            print("Data found in source_current_mirror_read. Copying to destination_previous_mirror...")

            # Get the current date for partitioning
            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day

            # Add year, month, and day as columns
            df_mirror_current = df_mirror_current.withColumn("year", lit(current_year)) \
                .withColumn("month", lit(f"{current_month:02d}")) \
                .withColumn("day", lit(f"{current_day:02d}"))

            # Write data to destination_previous_mirror with partitioning
            df_mirror_current.write.partitionBy("year", "month", "day").mode("overwrite").parquet(
                destination_previous_mirror)
            print(f"Data successfully copied to {destination_previous_mirror} with partitioning by year, month, day.")

        except AnalysisException:
            # If source_current_mirror is absent, read data from source_path_staging
            print(
                f"Data not found in {source_current_mirror_read}. Attempting to read from {source_path_staging0_read}...")
            try:
                df_staging = spark.read.parquet(source_path_staging0_read)
                print("Data found in source_path_staging. Copying to source_current_mirror...")

                # Get the current date for partitioning
                current_date = datetime.now()
                current_year = current_date.year
                current_month = current_date.month
                current_day = current_date.day

                # Add year, month, and day as columns
                df_staging = df_staging.withColumn("year", lit(current_year)) \
                    .withColumn("month", lit(f"{current_month:02d}")) \
                    .withColumn("day", lit(f"{current_day:02d}"))

                # Write data to source_current_mirror with partitioning
                df_staging.write.partitionBy("year", "month", "day").mode("overwrite").parquet(
                    source_current_mirror_write)
                print(
                    f"Data successfully copied to {source_current_mirror_write} with partitioning by year, month, day.")

            except AnalysisException:
                print(f"No data found in {source_path_staging0_read}. Skipping the process.")

    handle_mirror_data(spark, source_path_staging0_read, source_current_mirror_read, source_current_mirror_write, destination_previous_mirror)

def append(spark, app_conf, table, app_secret):
    """Incrementally append data to the S3 bucket or perform a full load if no data exists."""
    print(f"Loading table: {table}")

    # Get today's date for dynamic path generation
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    # Define the date-based folder structure
    date_folder = f"year={year}/month={month:02d}/day={day:02d}/"

    source_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "current" + "/" + date_folder
    append_output_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_staging_path"] + table + "/" + "append" + "/"
    append_current_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "append" + "/" + "current" + "/"

    # Fetch the max_date from the source path
    max_date = get_max_date(source_path, spark)
    print(f"Max date for table {table}: {max_date}")

    try:
        # Attempt to read existing data
        print(f"Trying to read data from {source_path}...")
        existing_data = spark.read.parquet(source_path)
        print(f"Data found at {source_path}. Proceeding with incremental load for table {table}...")
    except Exception as e:
        print(f"No data found at {source_path}. Attempting full load for table {table}. Error: {e}")
        # Call the full load function
        full_load(spark, app_conf, table, app_secret)

        # Retry reading the source path after full load
        try:
            print(f"Retrying to read data from {source_path} after full load...")
            existing_data = spark.read.parquet(source_path)
            print(f"Data successfully found at {source_path} after full load.")
        except Exception as retry_exception:
            print(f"Failed to read data from {source_path} after full load. Skipping table {table}. Error: {retry_exception}")

    # Perform incremental load if max_date is available
    if max_date:
        print(f"Executing incremental query for table: {table}")
        query = app_conf["Incremental_append"]["loadingQuery"].format(table=table, max_date=max_date)

        try:
            df1 = spark.read.format("jdbc") \
                .option("url", app_secret["mysql_conf"]["jdbc_url"]) \
                .option("dbtable", f"({query}) AS filtered_data") \
                .option("user", app_secret["mysql_conf"]["user"]) \
                .option("password", app_secret["mysql_conf"]["password"]) \
                .option("driver", app_secret["mysql_conf"]["driver"]) \
                .load()

            print(f"Schema of the data read from MySQL for table {table}:")
            df1.printSchema()
            df1.show()

            # Write the result to S3
            df1 = df1.withColumn("year", lit(year)) \
                .withColumn("month", lit(f"{month:02d}")) \
                .withColumn("day", lit(f"{day:02d}"))

            df1.write.partitionBy("year", "month", "day").parquet(append_output_path, mode="append")
            print(f"Incremental data written to {append_output_path} for table {table}")

            # Mirror data to append_current_path
            print(f"Mirroring data from {append_output_path} to {append_current_path}...")
            df1.write.partitionBy("year", "month", "day").mode("overwrite").parquet(append_current_path)
            print(f"Data successfully mirrored to {append_current_path} for table {table}")
        except Exception as e:
            print(f"Failed to load data for table {table} using query: {query}. Error: {e}")
    else:
        print(f"Skipping incremental load for table {table} as max_date could not be determined.")


def upsert(spark, app_conf, table, app_secret):
    """Check if data present in current else call append and mirror data from curr to prev.
    Then run the upsert query on curr and prev """
    print(f"Loading table: {table}")

    # Get today's date for dynamic path generation
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    # Define the date-based folder structure
    date_folder = f"year={year}/month={month:02d}/day={day:02d}/"

    source_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "append" + "/" + "current" + "/" + date_folder
    append_prev_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "append" + "/" + "previous" + "/"
    source_path_staging_read = app_conf["Full_load"]["source"]["S3_bucket"]["s3_staging_path"] + table + "/" + date_folder
    append_prev_path_read = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_mirror_path"] + table + "/" + "append" + "/" + "previous" + "/" + date_folder
    output_upsert_path = app_conf["Incremental_append"]["source"]["S3_bucket"]["s3_staging_path"] + table + "/" + "upsert" + "/"

    try:
        # Attempt to read existing data
        print(f"Trying to read data from {source_path}...")
        existing_data = spark.read.parquet(source_path)
        existing_data.show()
        print(f"Data found at {source_path}. Mirroring data to {append_prev_path} with partitioning...")

        # Mirror data from source_path to append_prev_path with partitioning by date
        existing_data = existing_data.withColumn("year", lit(year)) \
                                     .withColumn("month", lit(f"{month:02d}")) \
                                     .withColumn("day", lit(f"{day:02d}"))

        existing_data.write.partitionBy("year", "month", "day").mode("overwrite").parquet(append_prev_path)
        print(f"Data successfully mirrored from {source_path} to {append_prev_path} with partitioning for table {table}")

    except Exception as e:
        print(f"No data found at {source_path}. Attempting append for table {table}. Error: {e}")
        # Call the append function
        append(spark, app_conf, table, app_secret)

        # Recheck if data now exists after appending
        try:
            print(f"Rechecking data from {source_path} after appending...")
            existing_data = spark.read.parquet(source_path)
            existing_data.show()
            print(f"Data found at {source_path} after appending. Mirroring to {append_prev_path}...")

            # Mirror data to append_prev_path
            existing_data = existing_data.withColumn("year", lit(year)) \
                .withColumn("month", lit(f"{month:02d}")) \
                .withColumn("day", lit(f"{day:02d}"))

            existing_data.write.partitionBy("year", "month", "day").mode("overwrite").parquet(append_prev_path)
            print(f"Data successfully mirrored from {source_path} to {append_prev_path} after appending for table {table}")
        except Exception as recheck_error:
            print(f"Data still not found at {source_path} after appending. Skipping mirroring for table {table}. Error: {recheck_error}")

    # Additional code for the anti-left join to get new records
    from pyspark.sql.functions import lit
    from datetime import datetime

    try:
        print(f"Reading staging data from {source_path_staging_read}...")
        staging_df = spark.read.parquet(source_path_staging_read)
        print(f"Reading previous data from {append_prev_path_read}...")
        previous_df = spark.read.parquet(append_prev_path_read)

        # Register DataFrames as temporary views
        staging_df.createOrReplaceTempView("staging")
        previous_df.createOrReplaceTempView("previous")

        # Fetch the query from the application config
        query = app_conf["Incremental_upsert"]["loadingQuery"]
        print(f"Executing the query: {query}")
        new_records_df = spark.sql(query)

        print(f"New records found in table {table}:")
        new_records_df.show()

        # Get today's date for dynamic partitioning
        today = datetime.now()
        year = today.year
        month = today.month
        day = today.day

        # Add partitioning columns (year, month, day)
        new_records_df = new_records_df.withColumn("year", lit(year)) \
            .withColumn("month", lit(f"{month:02d}")) \
            .withColumn("day", lit(f"{day:02d}"))

        # Define the output path with date-based partitioning
        output_upsert_path = app_conf["Incremental_append"]["source"]["S3_bucket"][
                                 "s3_staging_path"] + table + "/" + "upsert" + "/"

        # Write the new records with partitioning by year, month, and day
        new_records_df.write.partitionBy("year", "month", "day").mode("overwrite").parquet(output_upsert_path)
        print(f"New records successfully written to {output_upsert_path}")

    except Exception as join_error:
        print(f"Error during staging and previous data join for table {table}: {join_error}")


def loading_source_data():
    """Main function to check and load source data"""

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
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_secrets_path = "/opt/bitnami/spark/.secrets"
    app_config_path = "/opt/bitnami/spark/applications.yml"

    # Load secrets from the YAML file
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    with open(app_config_path) as conf_file:
        app_conf = yaml.load(conf_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    tgt_list = app_conf["target_list"]

    for tgt in tgt_list: 
        if tgt == 'Full_load':
            print('Performing full_load')

            # Loop over the tables and load each one separately
            for table in app_conf["Full_load"]["tables"]:
                # Call the full load function directly
                full_load(spark, app_conf, table, app_secret)

        elif tgt == 'Incremental_append':
            print('Performing Incremental_append')

            # Loop over the tables and load each one separately
            for table in app_conf["Incremental_append"]["tables"]:
                # Call the full load function directly
                append(spark, app_conf, table , app_secret)

        elif tgt == 'Incremental_upsert':
            print('Performing Incremental_upsert')

            # Loop over the tables and load each one separately
            for table in app_conf["Incremental_upsert"]["tables"]:
                # Call the full load function directly
                upsert(spark, app_conf, table, app_secret)



if __name__ == "__main__":
    loading_source_data()

# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/source_data_loading.py
