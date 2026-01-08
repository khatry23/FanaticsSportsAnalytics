# Sports Card Analytics Platform

Event-driven medallion (Bronze/Silver/Gold) ELT pipeline for sports card analytics using RabbitMQ, PostgreSQL, S3, Snowflake, dbt, and Airflow.

## Repository Layout
- airflow/dags: Airflow DAGs for backfill, load, and dbt
- dbt: dbt project (sources, staging, marts, tests, macros)
- sql: Snowflake DDL, data quality checks, and examples

## Architecture Summary
- Bronze: raw immutable events in S3 + Snowflake RAW.EVENTS (payload as VARIANT)
- Silver: typed staging models per event type in dbt
- Gold: marts powering analytics (dim_card, fct_orders, fct_batch_performance)

## Data Flow
1) Backfill from Postgres event store into S3 (Airflow DAG: events_backfill_to_s3)
2) Load S3 raw JSON into Snowflake RAW.EVENTS (Airflow DAG: s3_to_snowflake_load_raw)
3) Transform and test in dbt (Airflow DAG: dbt_transform_and_test)

## Configuration
Copy and edit the env template:

```
cp .env.example .env
```

Key env vars:
- Postgres: POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
- S3: AWS_REGION, S3_BUCKET, S3_PREFIX
- Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE
- dbt: DBT_TARGET, DBT_PROFILES_DIR
- Data quality: RAW_EVENTS_FRESHNESS_WARN_HOURS, RAW_EVENTS_FRESHNESS_ERROR_HOURS, VOLUME_ANOMALY_PCT, VOLUME_ANOMALY_LOOKBACK_DAYS

## Snowflake Setup
Run the DDL to create warehouse, database, schemas, stage, and RAW table:

```
cat sql/snowflake_ddl.sql
```

## dbt Usage
From the repo root:

```
dbt run --project-dir ./dbt --profiles-dir ./dbt
DBT_TARGET=dev dbt test --project-dir ./dbt --profiles-dir ./dbt
```

## Airflow
Airflow DAGs are located in `airflow/dags`:
- `events_backfill_to_s3.py` (Postgres backfill stub)
- `s3_to_snowflake_load_raw.py` (COPY INTO stub)
- `dbt_transform_and_test.py` (dbt run/test + quality checks stub)

## Data Quality Checks
Data quality is enforced via dbt tests and source freshness in Airflow:
- Source freshness on RAW.EVENTS (configurable thresholds via env vars)
- Custom tests: defect rate within [0,1], quantity/price non-negative
- Volume anomaly test on RAW.EVENTS vs rolling 7-day average

See `sql/data_quality_checks.sql` for equivalent ad-hoc checks. The Airflow DAG logs the SQL that would be executed in Snowflake.

## Example Queries
See `sql/examples.sql` for sample analytics queries.

## Assumptions and Tradeoffs
- Snowpipe is optional; the load DAG uses a COPY INTO stub for clarity.
- Backfill is implemented as a stub for local dev; replace with Postgres extraction logic.
- Event dedupe is handled in staging using event_id and inserted_at ordering.
- Status accepted values are simplified; adjust to match your production enum.
- No secrets are hardcoded; use environment variables.

## Testing
- dbt run
- dbt test
- Airflow DAG import (no syntax errors)
