from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import yaml
import os.path

def extract_and_write_to_s3():
    """Extract data from MySQL and save it to S3 in Parquet format."""

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

    # Load secrets from the YAML file
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # MySQL JDBC connection details
    jdbc_url = "jdbc:mysql://mysql:3306/test_db"
    jdbc_properties = {
        "user": "airflow",  # Replace with your MySQL username if necessary
        "password": "airflow_password",  # Replace with your MySQL password if necessary
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Read data from MySQL
    df0 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "transactions") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    print("Schema of the data read from MySQL:")
    df0.printSchema()
    df0.show()


    df1 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "customers") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    print("Schema of the data read from MySQL:")
    df1.printSchema()
    df1.show()


    df2 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "campaigns") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    # Print schema of the data for verification
    print("Schema of the data read from MySQL:")
    df2.printSchema()
    df2.show()

    # Get the current date
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day

    # Add year, month, and day columns for partitioning
    df0 = df0.withColumn("year", lit(current_year)) \
           .withColumn("month", lit(f"{current_month:02d}")) \
           .withColumn("day", lit(f"{current_day:02d}"))
    df1 = df1.withColumn("year", lit(current_year)) \
        .withColumn("month", lit(f"{current_month:02d}")) \
        .withColumn("day", lit(f"{current_day:02d}"))
    df2 = df2.withColumn("year", lit(current_year)) \
        .withColumn("month", lit(f"{current_month:02d}")) \
        .withColumn("day", lit(f"{current_day:02d}"))

    # Define S3 output path
    output_path0 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/transactions/"
    output_path1 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/customers/"
    output_path2 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/campaigns/"

    # Write data to S3 in Parquet format with partitioning
    df0.write.partitionBy("year", "month", "day").parquet(output_path0, mode="overwrite")
    print(f"Data extracted from MySQL and written to {output_path0} on S3")
    df1.write.partitionBy("year", "month", "day").parquet(output_path1, mode="overwrite")
    print(f"Data extracted from MySQL and written to {output_path1} on S3")
    df2.write.partitionBy("year", "month", "day").parquet(output_path2, mode="overwrite")
    print(f"Data extracted from MySQL and written to {output_path2} on S3")

if __name__ == "__main__":
    extract_and_write_to_s3()

#spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/data_extractor_writer_full_load.py
