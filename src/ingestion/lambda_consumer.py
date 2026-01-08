import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_segment(value: str) -> str:
    cleaned = value.strip().replace("/", "_").replace(" ", "_")
    return cleaned or "unknown"


def _parse_json_bytes(body_bytes: bytes) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(body_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def decode_protobuf_stub(body_bytes: bytes) -> Dict[str, Any]:
    # TODO: Implement protobuf decoding with the appropriate schema.
    raise NotImplementedError("Protobuf decoding is not implemented.")


def _extract_headers(message: Dict[str, Any]) -> Dict[str, Any]:
    headers = {}
    basic_props = message.get("basicProperties") or {}
    headers.update(basic_props.get("headers") or {})
    headers.update(message.get("headers") or {})
    return headers


def _extract_body_and_headers(message: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
    headers = _extract_headers(message)
    if "data" in message:
        body_bytes = base64.b64decode(message["data"])
        return body_bytes, headers
    if "body" in message:
        body_value = message["body"]
        if isinstance(body_value, bytes):
            return body_value, headers
        return str(body_value).encode("utf-8"), headers
    return b"", headers


def _canonical_event_from_message(message: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    body_bytes, headers = _extract_body_and_headers(message)
    payload = _parse_json_bytes(body_bytes)
    if payload is None:
        try:
            payload = decode_protobuf_stub(body_bytes)
        except NotImplementedError:
            payload = {
                "_encoding": "base64",
                "data": base64.b64encode(body_bytes).decode("utf-8"),
            }

    event_id = (
        headers.get("event_id")
        or payload.get("event_id")
        or payload.get("id")
        or str(uuid.uuid4())
    )
    source = headers.get("source") or payload.get("source") or "unknown"
    event_type = headers.get("event_type") or payload.get("event_type") or "unknown"
    inserted_at = headers.get("inserted_at") or payload.get("inserted_at") or _utc_now_iso()

    ingested_at = _utc_now_iso()
    record = {
        "event_id": str(event_id),
        "source": str(source),
        "event_type": str(event_type),
        "payload": payload,
        "inserted_at": str(inserted_at),
        "ingested_at": ingested_at,
    }
    return record, ingested_at


def _s3_key(prefix: str, source: str, event_type: str, ingested_at: str) -> str:
    fromiso = getattr(datetime, "fromisoformat", None)
    if fromiso:
        dt = fromiso(ingested_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        cleaned = ingested_at.replace("Z", "+00:00")
        tz_sep = max(cleaned.rfind("+"), cleaned.rfind("-"))
        if tz_sep > 0:
            base = cleaned[:tz_sep]
            tz = cleaned[tz_sep:]
            if "." in base:
                base = base.split(".", 1)[0]
            if len(tz) == 6 and tz[3] == ":":
                tz = tz[:3] + tz[4:]
            cleaned = base + tz
        if "+" not in cleaned and "-" not in cleaned[10:]:
            cleaned = cleaned + "+0000"
        dt = datetime.strptime(cleaned, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)
    date_part = dt.strftime("%Y-%m-%d")
    hour_part = dt.strftime("%H")
    part_id = str(uuid.uuid4())
    safe_source = _safe_segment(source)
    safe_type = _safe_segment(event_type)
    key_prefix = prefix.strip("/")
    if key_prefix:
        key_prefix = f"{key_prefix}/"
    return (
        f"{key_prefix}source={safe_source}/type={safe_type}/dt={date_part}/hour={hour_part}/"
        f"part-{part_id}.json"
    )


def _iter_messages(event: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    if "messages" in event:
        return event["messages"]
    if "Records" in event:
        return event["Records"]
    return []


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("ingestion.lambda_consumer")
    if not logger.handlers:
        handler = logging.StreamHandler()
        logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    return logger


def _log_structured(logger: logging.Logger, level: str, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    getattr(logger, level.lower())(json.dumps(payload))


def _put_to_s3(bucket: str, key: str, record: Dict[str, Any]) -> None:
    import boto3

    s3_client = boto3.client("s3")
    body = json.dumps(record) + "\n"
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger = _get_logger()
    bucket = os.getenv("S3_BUCKET", "")
    prefix = os.getenv("S3_PREFIX", "raw/events")
    dry_run = os.getenv("DRY_RUN", "1").lower() in {"1", "true", "yes"}
    write_to_s3 = os.getenv("WRITE_TO_S3", "0").lower() in {"1", "true", "yes"}

    if write_to_s3:
        dry_run = False

    records: List[Dict[str, Any]] = []
    for message in _iter_messages(event):
        record, ingested_at = _canonical_event_from_message(message)
        key = _s3_key(prefix, record["source"], record["event_type"], ingested_at)
        records.append({"s3_key": key, "record": record})

        _log_structured(
            logger,
            "info",
            "prepared_event_record",
            event_id=record["event_id"],
            source=record["source"],
            event_type=record["event_type"],
            s3_key=key,
            dry_run=dry_run,
        )

        if dry_run:
            continue
        if not bucket:
            raise ValueError("S3_BUCKET is required when WRITE_TO_S3 is enabled.")
        _put_to_s3(bucket, key, record)

    if not records:
        _log_structured(logger, "warning", "no_messages_received")
    return {
        "record_count": len(records),
        "dry_run": dry_run,
        "records": records,
    }
