import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def run_data_quality_checks():
    logger = logging.getLogger(__name__)
    repo_root = Path(__file__).resolve().parents[2]
    checks_path = repo_root / "sql" / "data_quality_checks.sql"

    if not checks_path.exists():
        logger.info("No data quality checks file found at %s", checks_path)
        return

    checks_sql = checks_path.read_text()
    logger.info("Would execute data quality checks:\n%s", checks_sql)


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

project_dir = os.getenv("DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))
profiles_dir = os.getenv("DBT_PROFILES_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))

with DAG(
    dag_id="dbt_transform_and_test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
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
