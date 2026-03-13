from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "shopstream",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_transformations",
    default_args=default_args,
    description="Run dbt models after gold aggregation",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transformations"],
) as dag:

    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="gold_aggregation",
        external_task_id="export_gold_to_postgres",
        timeout=3600,
        mode="reschedule",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --select country_codes",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    wait_for_gold >> dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
