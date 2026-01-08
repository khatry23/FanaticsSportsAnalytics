# Monitoring and Alerting

This repo includes lightweight, production-oriented monitoring for ingestion, loads, transformations, and data health checks.

## What Is Monitored
- **Airflow failures**: DAG/task failures trigger Slack alerts.
- **dbt transforms/tests**: `dbt run` and `dbt test` failures fail the DAG.
- **Freshness and anomaly checks**: SQL checks run after dbt tests and can be wired to Snowflake.
- **Lambda ingestion**: CloudWatch alarms for Errors, Throttles, and p95 Duration.

## Alert Channels and Setup

### Airflow → Slack
- Set `SLACK_WEBHOOK_URL` in the Airflow environment.
- Alerts include: `dag_id`, `task_id`, `run_id`, `execution_date`, `try_number`, `log_url`, and exception snippet.

### CloudWatch → SNS Email
- Deploy `infra/cloudformation/monitoring_alarms.yml`.
- Set `AlarmEmail` to a real address; confirm the SNS subscription email.
- To route to Slack, replace the email subscription with AWS Chatbot or a webhook relay.

## Runbook (When an Alert Fires)

### 1) Airflow failure
- Check the task log via the `log_url` in the Slack alert.
- Common causes: transient connection issues, bad credentials, upstream data drift.
- Replay: re-run the failed task or re-trigger the DAG. RAW data is immutable, so retries are safe.

### 2) dbt test failure
- Inspect the failing test in dbt logs and identify violated constraints.
- If upstream data is malformed, isolate in RAW and reprocess once fixed.
- Replay: re-run `dbt test` or the Airflow DAG after remediation.

### 3) Freshness or anomaly check failure
- Review recent ingestion volume and latest `inserted_at` in `RAW.EVENTS`.
- Check upstream RabbitMQ/Lambda health and S3 landing.
- Replay: re-run the ingestion trigger or backfill DAG, then re-run dbt.

### 4) Lambda error/throttle alarm
- Check CloudWatch logs for the Lambda invocation errors.
- Common causes: malformed messages, permission errors, downstream S3 issues.
- Replay: after fix, allow retries or reprocess from queue if needed.

## Suggested Thresholds
- **Freshness**: warn at 24h, error at 48h (adjust via env vars).
- **Volume anomaly**: 50% deviation from 7-day rolling average.
- **Lambda duration**: p95 < 25s unless timeout is higher.

## Tuning
- Adjust data health thresholds via:
  - `RAW_EVENTS_FRESHNESS_WARN_HOURS`
  - `RAW_EVENTS_FRESHNESS_ERROR_HOURS`
  - `VOLUME_ANOMALY_PCT`
  - `VOLUME_ANOMALY_LOOKBACK_DAYS`
- Update CloudWatch alarm thresholds in `infra/cloudformation/monitoring_alarms.yml`.
