from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "shopstream",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Run Great Expectations validations across all layers",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "validation"],
) as dag:

    wait_for_dbt = ExternalTaskSensor(
        task_id="wait_for_dbt",
        external_dag_id="dbt_transformations",
        external_task_id="dbt_test",
        timeout=3600,
        mode="reschedule",
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command="cd /opt/airflow && python -m src.quality.run_validations bronze",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command="cd /opt/airflow && python -m src.quality.run_validations silver",
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command="cd /opt/airflow && python -m src.quality.run_validations gold",
    )

    wait_for_dbt >> validate_bronze >> validate_silver >> validate_gold
