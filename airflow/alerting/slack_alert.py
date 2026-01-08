import json
import os
import textwrap
import urllib.request
from typing import Any, Dict


def _truncate(value: str, limit: int = 500) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "..."


def _build_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    payload = {
        "dag_id": context.get("dag").dag_id if context.get("dag") else None,
        "task_id": task_instance.task_id if task_instance else None,
        "run_id": context.get("run_id"),
        "execution_date": str(context.get("execution_date")),
        "try_number": task_instance.try_number if task_instance else None,
        "log_url": task_instance.log_url if task_instance else None,
        "exception": _truncate(str(exception)) if exception else None,
    }
    return payload


def slack_failure_callback(context: Dict[str, Any]) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    payload = _build_payload(context)
    body = {
        "text": "Airflow task failure",
        "attachments": [
            {
                "color": "#D00000",
                "title": "Airflow Task Failure",
                "text": textwrap.dedent(
                    f"""
                    dag_id: {payload['dag_id']}
                    task_id: {payload['task_id']}
                    run_id: {payload['run_id']}
                    execution_date: {payload['execution_date']}
                    try_number: {payload['try_number']}
                    log_url: {payload['log_url']}
                    exception: {payload['exception']}
                    """
                ).strip(),
            }
        ],
    }

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        response.read()
