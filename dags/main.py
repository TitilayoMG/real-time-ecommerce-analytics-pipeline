"""
dag_orders_pipeline.py
----------------------
Airflow DAG: streams 20 rows per run from orders.csv → Kafka → S3.

Each run advances the csv_offset Variable by 20.
25 runs exhaust the 500-row file.

Place this file and producer.py / consumer.py in $AIRFLOW_HOME/dags/

Airflow Variable to create before first run:
    csv_offset  →  0

Airflow Connections to create:
    kafka_default  →  Host = broker host, Port = 9092
    aws_default    →  Login    = AWS Access Key ID
                      Password = AWS Secret Access Key
                      Extra    = {"region_name": "us-east-1", "s3_bucket": "your-bucket"}
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from services.consumer import consume_data
from services.producer import produce_data
from services.redshift_loader import load_data_from_s3_to_redshift
from services.dbt_layers import run_dbt_layers, check_data_quality

default_args = {
    "owner":            "kafka_streaming",
    "depends_on_past":  False,
    "retries":          0,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="kafka_to_s3",
    default_args=default_args,
    description="Stream 8 rows/run through Kafka and land in S3 partitioned by date",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kafka", "s3"],
) as dag:

    produce_task = PythonOperator(
        task_id="produce_data",
        python_callable=produce_data,
    )

    consume_task = PythonOperator(
        task_id="consume_data",
        python_callable=consume_data,
        execution_timeout=timedelta(minutes=10),
    )

    redshift_task = PythonOperator(
        task_id="load_data_to_redshift",
        python_callable=load_data_from_s3_to_redshift,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_layers = PythonOperator(
        task_id="dbt_transformations",
        python_callable=run_dbt_layers
    )

    run_data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=check_data_quality
    )

    produce_task >> consume_task >> redshift_task >> dbt_layers >> run_data_quality_checks