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
    dag_id="gold_aggregation",
    default_args=default_args,
    description="Aggregate Silver → Gold business metrics",
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "aggregation"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_processing",
        external_dag_id="silver_processing",
        external_task_id="build_sessions",
        timeout=3600,
        mode="reschedule",
    )

    build_daily_revenue = BashOperator(
        task_id="build_daily_revenue",
        bash_command="cd /opt/airflow && python -m src.processing.silver_to_gold daily_revenue",
    )

    build_conversion_funnel = BashOperator(
        task_id="build_conversion_funnel",
        bash_command="cd /opt/airflow && python -m src.processing.silver_to_gold conversion_funnel",
    )

    build_customer_metrics = BashOperator(
        task_id="build_customer_metrics",
        bash_command="cd /opt/airflow && python -m src.processing.silver_to_gold customer_metrics",
    )

    export_to_postgres = BashOperator(
        task_id="export_gold_to_postgres",
        bash_command="cd /opt/airflow && python -m src.serving.export_to_postgres",
    )

    wait_for_silver >> [build_daily_revenue, build_conversion_funnel, build_customer_metrics]
    [build_daily_revenue, build_conversion_funnel, build_customer_metrics] >> export_to_postgres
