import random
import string
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime, timedelta


def generate_random_string(length=10):
    """Generates a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_date():
    """Generates a random date."""
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')


def create_spark_session():
    """Creates a Spark session with the necessary JDBC driver for MySQL."""
    spark = SparkSession.builder \
        .appName("Insert Random Records into MySQL") \
        .master("local") \
        .config("spark.jars", "file:///C:/Spark/jars/mysql-connector-java-8.0.30.jar") \
        .getOrCreate()
    return spark


def insert_random_records():
    """Inserts random records into the MySQL database using Spark."""

    # Generate random data for transactions, customers, and campaigns
    data_transactions = [(generate_random_string(), random.randint(18, 70), generate_random_string(10),
                          random.randint(1000, 9999), generate_random_date()) for _ in range(10)]
    data_customers = [(generate_random_string(), random.randint(18, 70), generate_random_string(10),
                       random.randint(1000, 9999), generate_random_date()) for _ in range(10)]
    data_campaigns = [(generate_random_string(), random.randint(18, 70), generate_random_string(10),
                       random.randint(1000, 9999), generate_random_date()) for _ in range(10)]

    # Convert data to pandas DataFrames
    df_transactions = pd.DataFrame(data_transactions, columns=["name", "age", "phone_number", "id", "date"])
    df_customers = pd.DataFrame(data_customers, columns=["name", "age", "phone_number", "id", "date"])
    df_campaigns = pd.DataFrame(data_campaigns, columns=["name", "age", "phone_number", "id", "date"])

    # Create Spark DataFrames from the pandas DataFrames
    spark = create_spark_session()
    spark_df_transactions = spark.createDataFrame(df_transactions)
    spark_df_customers = spark.createDataFrame(df_customers)
    spark_df_campaigns = spark.createDataFrame(df_campaigns)

    # MySQL connection properties
    jdbc_url = "jdbc:mysql://localhost:3306/test_db"  # MySQL service name and database name
    connection_properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Write the DataFrames to MySQL using Spark JDBC
    # Insert data into transactions table with primary key on 'id'
    spark_df_transactions.write.jdbc(url=jdbc_url, table="transactions", mode="append", properties=connection_properties)

    # Insert data into customers table with primary key on 'id'
    spark_df_customers.write.jdbc(url=jdbc_url, table="customers", mode="append", properties=connection_properties)

    # Insert data into campaigns table (without primary key)
    spark_df_campaigns.write.jdbc(url=jdbc_url, table="campaigns", mode="append", properties=connection_properties)


if __name__ == "__main__":
    insert_random_records()
