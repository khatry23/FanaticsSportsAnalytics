import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def backfill_events_to_s3():
    """
    Placeholder backfill job.
    Expected behavior: read events from Postgres by inserted_at watermark and write JSON to S3.
    """
    logger = logging.getLogger(__name__)
    postgres_host = os.getenv("POSTGRES_HOST")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_PREFIX", "raw/events")

    if not postgres_host or not s3_bucket:
        logger.info("Missing POSTGRES_HOST or S3_BUCKET; running in stub mode.")

    # Local stub: write a placeholder file to /tmp to simulate extraction.
    stub_payload = {
        "event_id": "00000000-0000-0000-0000-000000000000",
        "source": "backfill",
        "event_type": "OrderCreated",
        "payload": {
            "order_id": "order_0",
            "order_ts": "2024-01-01T00:00:00Z",
            "customer_id": "cust_0",
            "sku": "sku_0",
            "quantity": 1,
            "price": {"units": 1999, "scale": 2},
            "status": "CREATED",
        },
        "inserted_at": "2024-01-01T00:00:00Z",
    }

    out_path = Path("/tmp/events_backfill_stub.json")
    out_path.write_text(json.dumps(stub_payload))
    logger.info("Wrote stub backfill file to %s (upload to s3://%s/%s).", out_path, s3_bucket, s3_prefix)


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="events_backfill_to_s3",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["backfill", "bronze"],
) as dag:
    backfill = PythonOperator(
        task_id="backfill_events",
        python_callable=backfill_events_to_s3,
    )

    backfill
