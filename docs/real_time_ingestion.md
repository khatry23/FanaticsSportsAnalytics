# Real-Time Ingestion (AmazonMQ -> Lambda -> S3)

## Wiring Overview
AmazonMQ (RabbitMQ) pushes message batches to AWS Lambda via an event source mapping. The Lambda function canonicalizes each message and writes JSON lines to S3 in the Bronze layer. Snowflake RAW ingestion (COPY INTO or Snowpipe) remains unchanged.

## Batch, Ack, and Retry Semantics
- Lambda receives batches from RabbitMQ based on the configured `BatchSize`.
- If the handler raises an exception, the entire batch is retried.
- No manual acknowledgements are performed; RabbitMQ/Lambda handles acknowledgements when the invocation succeeds.

## Idempotency
Events are immutable and downstream models dedupe by `event_id`. The ingestion layer only normalizes and persists records, leaving dedupe to Silver/Gold transformations.

## Canonical Event Mapping
Each message is mapped to a canonical record:

```
{
  "event_id": "<uuid from message if present else generated>",
  "source": "<from headers/payload else unknown>",
  "event_type": "<from headers/payload else unknown>",
  "payload": "<parsed JSON or base64 wrapper>",
  "inserted_at": "<from message if present else now>",
  "ingested_at": "<now>"
}
```

If the body is valid JSON, it is used directly as `payload`. If decoding fails, the payload is wrapped as base64 with `_encoding: "base64"`. A protobuf decoder stub is included for future implementation.

## S3 Partitioning
Records are written as JSON lines to:

```
s3://{S3_BUCKET}/{S3_PREFIX}/source={source}/type={event_type}/dt=YYYY-MM-DD/hour=HH/part-<uuid>.json
```

The date and hour are derived from `ingested_at` in UTC.
