import os
import requests
from airflow.hooks.base import BaseHook

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack_alert(context: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        return

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    exec_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = {
        "text": f":red_circle: *DAG Failed*\n*DAG:* `{dag_id}`\n*Task:* `{task_id}`\n*Date:* {exec_date}\n<{log_url}|View Logs>"
    }
    requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
