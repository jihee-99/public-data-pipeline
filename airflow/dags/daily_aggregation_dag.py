from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="daily_bus_aggregation",
    default_args=default_args,
    start_date=datetime(2026, 5, 28),
    schedule_interval="0 1 * * *",  # 매일 새벽 1시
    catchup=False
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="daily_aggregation",
        application="/opt/airflow/dags/spark/batch/daily_aggregation.py",
        conn_id="spark_default",
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        }
    )