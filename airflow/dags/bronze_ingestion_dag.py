from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "shopstream",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,
    "email_on_failure": False,
}

with DAG(
    dag_id="bronze_ingestion",
    default_args=default_args,
    description="Extract data from PostgreSQL to Bronze Delta tables",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    extract_postgres = BashOperator(
        task_id="extract_postgres_to_bronze",
        bash_command="cd /opt/airflow && python -m src.ingestion.postgres_to_bronze",
    )

    load_flat_files = BashOperator(
        task_id="load_flat_files_to_bronze",
        bash_command="echo 'No flat files to load today'",
    )

    extract_postgres >> load_flat_files
