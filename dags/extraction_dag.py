from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 26),  # Set to your desired start date
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'extract_mysql_data_dag',
    default_args=default_args,
    schedule_interval='@once',  # Runs once when triggered
    catchup=False,
)

# Define the SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/airflow/jobs/extract_task.py',  # Path to the Python script you created
    conn_id='spark_default',  # Connection ID for your Spark cluster
    executor_memory='2g',  # Adjust memory as per your requirements
    total_executor_cores=2,  # Adjust cores as needed
    driver_class_path='/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar',  # Path to MySQL connector JAR
    conf={
        'spark.jars': '/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar',  # MySQL connector JAR path
    },
    dag=dag,
)

# Set the task sequence
spark_submit_task
