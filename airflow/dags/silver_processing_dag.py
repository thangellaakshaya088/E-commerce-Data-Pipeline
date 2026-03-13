from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "shopstream",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_processing",
    default_args=default_args,
    description="Clean and transform Bronze → Silver",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "processing"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingestion",
        external_dag_id="bronze_ingestion",
        external_task_id="load_flat_files_to_bronze",
        timeout=3600,
        mode="reschedule",
    )

    clean_clickstream = BashOperator(
        task_id="clean_clickstream",
        bash_command="cd /opt/airflow && python -m src.processing.bronze_to_silver clickstream",
    )

    clean_orders = BashOperator(
        task_id="clean_orders",
        bash_command="cd /opt/airflow && python -m src.processing.bronze_to_silver orders",
    )

    clean_users = BashOperator(
        task_id="clean_users",
        bash_command="cd /opt/airflow && python -m src.processing.bronze_to_silver users",
    )

    build_sessions = BashOperator(
        task_id="build_sessions",
        bash_command="cd /opt/airflow && python -m src.processing.session_builder",
    )

    wait_for_bronze >> [clean_clickstream, clean_orders, clean_users]
    clean_clickstream >> build_sessions
