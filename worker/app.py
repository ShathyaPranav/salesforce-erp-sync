"""Lambda handler: one SQS batch in, a partial batch response out.

Each record is processed on its own, so one bad message can't fail the other
nine. The return value lists only the records that failed
({"batchItemFailures": [...]}); Lambda deletes the rest from the queue. This
only works because the event source mapping has ReportBatchItemFailures
switched on (template.yaml); without it a normal return deletes everything.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config

from erp.writer import WriteResult, write_order
from worker.logs import log
from worker.mapping import parse_body, to_order

_clients: dict[str, Any] = {}


def dynamodb() -> Any:
    if "dynamodb" not in _clients:
        _clients["dynamodb"] = boto3.client(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(retries={"mode": "standard", "max_attempts": 3}),
        )
    return _clients["dynamodb"]


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        try:
            process(record)
        except Exception as exc:  # Phase 1.4 splits this into transient and permanent
            log("error", "failed", message_id=record.get("messageId"), error=repr(exc))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def process(record: dict[str, Any]) -> WriteResult:
    event = parse_body(record["body"])
    order = to_order(event)
    result = write_order(dynamodb(), order, now=datetime.now(UTC).isoformat())
    log("info", result.value, event_key=order.event_key, version=order.version)
    return result
