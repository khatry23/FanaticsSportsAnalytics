# Assumptions, Tradeoffs, and Future Work

This document captures the explicit assumptions in the design, key architectural tradeoffs, and possible future enhancements.

## Assumptions

### Data and Events
- **Event immutability**: Events are append-only; corrections arrive as new events.
- **Globally unique event identifiers**: Every event has a stable `event_id` for idempotency and deduplication.
- **At-least-once delivery**: RabbitMQ provides at-least-once semantics; duplicates are handled downstream.
- **Stable Protobuf schemas**: Schema evolution is backward compatible; changes are versioned as needed.

### Authoritative Events
- **OrderCreated** is the source of truth for order-level facts.
- **CardCatalogEntryInserted** is the source of truth for SKU metadata.
- **ManufacturingBatch** is the source of truth for batch-level production.

### Workload Assumptions
- The system is optimized for analytics and reporting, not transactional workloads.

## Tradeoffs

### 1) ELT vs ETL
- **Decision**: Use ELT (land raw data, transform in Snowflake via dbt).
- **Pros**: Preserves raw events, leverages Snowflake compute, simplifies ingestion, enables fast iteration.
- **Cons**: Requires stronger access controls for raw data; malformed records can land before validation.
- **Rationale**: Snowflake + dbt are primary tools; ELT aligns with the analytical workload.

### 2) Medallion Architecture vs Single-Layer Warehouse
- **Decision**: Use Bronze–Silver–Gold layering.
- **Pros**: Clear lineage, quality gates, easier debugging, supports multiple consumers.
- **Cons**: More models/tables and slightly higher operational complexity.
- **Rationale**: The medallion pattern improves trust and maintainability for shared analytics.

### 3) Managed Ingestion vs Orchestrator-Based Ingestion
- **Decision**: Use Lambda for RabbitMQ ingestion; keep Airflow for orchestration.
- **Pros**: Decouples ingestion from orchestration, improves reliability, avoids long-running workers.
- **Cons**: Additional infrastructure and monitoring outside Airflow.
- **Rationale**: Airflow is not suited for streaming ingestion; Lambda handles triggers reliably.

### 4) Snowflake as the Primary Analytical Store
- **Decision**: Centralize analytics in Snowflake rather than querying the data lake directly.
- **Pros**: Strong BI performance, semi-structured support, centralized governance.
- **Cons**: Vendor lock-in and cost management requirements.
- **Rationale**: Snowflake already powers BI (Sigma), making it the natural analytics layer.

### 5) Simplicity vs Premature Optimization
- **Decision**: Prioritize clarity and maintainability.
- **Pros**: Easier onboarding and lower operational risk.
- **Cons**: May require refactoring at very large scale.
- **Rationale**: Meets current needs while remaining extensible.

## Future Work

1) **Schema registry and versioning**
   - Add schema registry for Protobuf definitions.
   - Enforce backward-compatibility checks at ingestion time.

2) **Late-arriving and out-of-order events**
   - Add Silver-layer handling for late dimensions.
   - Introduce SCD Type 2 where catalog changes are required.

3) **Advanced data quality**
   - Add anomaly detection for revenue, volume, and defect rates.
   - Combine statistical checks with dbt tests.

4) **Real-time analytics**
   - Enable Snowpipe Streaming for low-latency ingestion.
   - Support near-real-time operational dashboards.

5) **Cost optimization**
   - Tune warehouse auto-scaling policies.
   - Optimize clustering and partitioning.
   - Introduce tiered Bronze retention.

6) **Advanced analytics and ML**
   - Add demand forecasting models.
   - Predict manufacturing defect rates.
   - Build feature stores from Gold tables.

7) **Governance and access control**
   - Add row- and column-level security.
   - Introduce data cataloging and lineage visualization.
