FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system dependencies
USER root

# Install necessary system dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jdk wget && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create necessary directories for Spark jars
RUN mkdir -p /opt/spark/jars

# Switch back to the airflow user
USER airflow

# Install necessary Python packages with Airflow constraints
ARG AIRFLOW_VERSION=2.7.1
ARG AIRFLOW_CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark \
    pandas \
    pymysql \
    boto3 \
    pyyaml \
    --constraint "${AIRFLOW_CONSTRAINT_URL}"

# Copy the MySQL connector JAR file into the container
COPY ./jars/mysql-connector-java-8.0.30.jar /opt/airflow/jars/mysql-connector-java-8.0.30.jar
COPY ./jars/mysql-connector-java-8.0.30.jar /opt/spark/jars/mysql-connector-java-8.0.30.jar

# Copy Hadoop AWS JAR for S3 integration (optional, based on your Spark version)
COPY ./jars/hadoop-aws-3.2.0.jar /opt/spark/jars/hadoop-aws-3.2.0.jar
COPY ./jars/aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar

# Set Spark and Hadoop environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Verify that all necessary files are in place
RUN ls /opt/airflow/jars/mysql-connector-java-8.0.30.jar && \
    ls /opt/spark/jars/mysql-connector-java-8.0.30.jar && \
    ls /opt/spark/jars/hadoop-aws-3.2.0.jar && \
    ls /opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar
