import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from alerting.slack_alert import slack_failure_callback  # noqa: E402


def load_s3_to_snowflake_raw():
    """
    Placeholder load job.
    Expected behavior: COPY INTO RAW.EVENTS from S3 stage or trigger Snowpipe.
    """
    logger = logging.getLogger(__name__)
    database = os.getenv("SNOWFLAKE_DATABASE", "SPORTS_CARD_ANALYTICS")
    raw_schema = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    stage = f"{database}.{raw_schema}.S3_EVENTS_STAGE"
    table = f"{database}.{raw_schema}.EVENTS"

    copy_sql = (
        "COPY INTO {table} FROM @{stage} "
        "FILE_FORMAT=(FORMAT_NAME={database}.{raw_schema}.JSON_EVENTS) "
        "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;"
    ).format(table=table, stage=stage, database=database, raw_schema=raw_schema)

    logger.info("Would execute COPY INTO: %s", copy_sql)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="s3_to_snowflake_load_raw",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=45),
    catchup=False,
    tags=["bronze", "snowflake"],
) as dag:
    load_raw = PythonOperator(
        task_id="load_raw_events",
        python_callable=load_s3_to_snowflake_raw,
    )

    load_raw
