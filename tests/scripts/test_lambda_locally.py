import json
import os
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from src.ingestion.lambda_consumer import lambda_handler  # noqa: E402


def main() -> int:
    fixture_path = Path(os.getenv("AMAZONMQ_FIXTURE", "tests/fixtures/amazonmq_event.json"))
    if not fixture_path.exists():
        print(f"Fixture not found: {fixture_path}")
        return 1

    event = json.loads(fixture_path.read_text())

    os.environ.setdefault("S3_BUCKET", "local-bucket")
    os.environ.setdefault("S3_PREFIX", "raw/events")

    write_to_s3 = os.getenv("WRITE_TO_S3", "0").lower() in {"1", "true", "yes"}
    if write_to_s3:
        os.environ["DRY_RUN"] = "0"
    else:
        os.environ["DRY_RUN"] = "1"

    result = lambda_handler(event, None)
    for item in result.get("records", []):
        print("S3_KEY:", item["s3_key"])
        print("RECORD:", json.dumps(item["record"], indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
