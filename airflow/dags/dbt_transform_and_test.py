import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from alerting.slack_alert import slack_failure_callback  # noqa: E402


def run_data_quality_checks():
    logger = logging.getLogger(__name__)
    repo_root = Path(__file__).resolve().parents[2]
    checks_path = repo_root / "sql" / "data_quality_checks.sql"

    if not checks_path.exists():
        logger.info("No data quality checks file found at %s", checks_path)
        return

    checks_sql = checks_path.read_text()
    freshness_error_hours = os.getenv("RAW_EVENTS_FRESHNESS_ERROR_HOURS", "48")
    volume_pct = os.getenv("VOLUME_ANOMALY_PCT", "0.5")
    volume_lookback = os.getenv("VOLUME_ANOMALY_LOOKBACK_DAYS", "7")

    checks_sql = (
        checks_sql.replace("__FRESHNESS_ERROR_HOURS__", freshness_error_hours)
        .replace("__VOLUME_ANOMALY_PCT__", volume_pct)
        .replace("__VOLUME_LOOKBACK_DAYS__", volume_lookback)
    )
    logger.info(
        "Data quality check parameters: RAW_EVENTS_FRESHNESS_ERROR_HOURS=%s, VOLUME_ANOMALY_PCT=%s, VOLUME_ANOMALY_LOOKBACK_DAYS=%s",
        freshness_error_hours,
        volume_pct,
        volume_lookback,
    )
    logger.info("Would execute data quality checks:\n%s", checks_sql)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_callback,
}

project_dir = os.getenv("DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))
profiles_dir = os.getenv("DBT_PROFILES_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))

with DAG(
    dag_id="dbt_transform_and_test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=90),
    catchup=False,
    tags=["silver", "gold", "dbt"],
) as dag:
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"dbt source freshness --project-dir {project_dir} --profiles-dir {profiles_dir}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {project_dir} --profiles-dir {profiles_dir}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {project_dir} --profiles-dir {profiles_dir}",
    )

    dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    dbt_freshness >> dbt_run >> dbt_test >> dq_checks
