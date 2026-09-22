"""Lambda handler: one SQS batch in, a partial batch response out.

Each record ends in exactly one of three ways:

  success    written, duplicate or stale: Lambda deletes the message
  transient  the worker sets the message's visibility timeout to the backoff
             delay and reports it failed; SQS redelivers it after the delay,
             and moves it to the DLQ after 5 receives (the queue's redrive policy)
  permanent  the worker sends it to the DLQ itself, with a `reason` attribute,
             and reports success so Lambda deletes the original. No retries

The return value lists only transient failures ({"batchItemFailures": [...]}).
That only works because the event source mapping has ReportBatchItemFailures
on (template.yaml): without it, a normal return deletes the whole batch.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config

from erp.writer import WriteResult, write_order
from worker.backoff import next_visibility_timeout
from worker.chaos import Chaos
from worker.classify import Kind, classify
from worker.logs import log
from worker.mapping import parse_body, to_order
from worker.metrics import emit

PREFIX = os.environ.get("RELAY_PARAM_PREFIX", "/relay")
DLQ_NAME = os.environ.get("DLQ_NAME", "relay-events-dlq")
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE_SECONDS", "60"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP_SECONDS", "900"))
# Stop starting new records when less than this is left before the Lambda
# timeout, and hand the rest back: a timeout would fail the whole batch.
TIME_GUARD_MS = int(os.environ.get("TIME_GUARD_MS", "5000"))

_clients: dict[str, Any] = {}
_queue_urls: dict[str, str] = {}


def client(service: str) -> Any:
    if service not in _clients:
        _clients[service] = boto3.client(  # type: ignore[call-overload]
            service,
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(retries={"mode": "standard", "max_attempts": 3}),
        )
    return _clients[service]


# Outcome of one record -> the metric it counts towards.
METRIC_FOR = {
    "written": "OrdersWritten",
    "duplicate": "DuplicatesSkipped",
    "stale": "StaleVersionsDropped",
    "retry": "TransientRetries",
    "dead_lettered": "DeadLettered",
}
RETRY_OUTCOMES = {"retry", "dlq_send_failed"}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    chaos = Chaos.load(client("ssm"), PREFIX)
    records = event.get("Records", [])
    failures: list[dict[str, str]] = []
    counts = dict.fromkeys(METRIC_FOR.values(), 0)
    try:
        for i, record in enumerate(records):
            if _out_of_time(context):
                log(
                    "warn",
                    "near timeout, returning unprocessed records",
                    remaining=len(records) - i,
                )
                failures.extend({"itemIdentifier": r["messageId"]} for r in records[i:])
                break
            outcome = handle_record(record, chaos)
            if outcome in METRIC_FOR:
                counts[METRIC_FOR[outcome]] += 1
            if outcome in RETRY_OUTCOMES:
                failures.append({"itemIdentifier": record["messageId"]})
            chaos.maybe_crash(processed=i + 1)
    finally:
        emit("worker", counts)
    return {"batchItemFailures": failures}


def handle_record(record: dict[str, Any], chaos: Chaos) -> str:
    """Process one record and return its outcome: written, duplicate, stale,
    dead_lettered (all acknowledged), or retry / dlq_send_failed (redelivered)."""
    event_key = _event_key(record)
    attempt = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
    try:
        result = process(record, chaos)
    except Exception as exc:
        decision = classify(exc)
        if decision.kind is Kind.PERMANENT:
            parked = dead_letter(record, decision.reason, str(exc), event_key)
            return "dead_lettered" if parked else "dlq_send_failed"
        delay = next_visibility_timeout(attempt, BACKOFF_BASE, BACKOFF_CAP)
        log(
            "warn",
            "retry",
            event_key=event_key,
            reason=decision.reason,
            error=str(exc),
            attempt=attempt,
            retry_in_s=delay,
        )
        _delay_redelivery(record, delay)
        return "retry"
    log("info", result.value, event_key=event_key, attempt=attempt)
    return result.value


def process(record: dict[str, Any], chaos: Chaos) -> WriteResult:
    order = to_order(parse_body(record["body"]))
    chaos.maybe_fail_erp()
    return write_order(client("dynamodb"), order, now=datetime.now(UTC).isoformat())


def dead_letter(record: dict[str, Any], reason: str, detail: str, event_key: str) -> bool:
    """Send a permanently failed message to the DLQ. True if it's safe to delete."""
    attrs = {
        "reason": reason,
        "detail": detail[:1000],
        "event_key": event_key,
        "source_message_id": record["messageId"],
        "failed_at": datetime.now(UTC).isoformat(),
    }
    try:
        client("sqs").send_message(
            QueueUrl=_queue_url(DLQ_NAME, record),
            MessageBody=record["body"],
            MessageAttributes={
                k: {"DataType": "String", "StringValue": v or "-"} for k, v in attrs.items()
            },
        )
    except Exception as exc:
        # Couldn't park it: let SQS redeliver it, and try again then.
        log("error", "dead-letter send failed", event_key=event_key, error=repr(exc))
        return False
    log("warn", "dead_lettered", event_key=event_key, reason=reason, detail=detail)
    return True


def _delay_redelivery(record: dict[str, Any], seconds: int) -> None:
    try:
        client("sqs").change_message_visibility(
            QueueUrl=_queue_url(_queue_name(record), record),
            ReceiptHandle=record["receiptHandle"],
            VisibilityTimeout=seconds,
        )
    except Exception as exc:
        # Not fatal: the message still comes back, after the queue's default timeout.
        log("warn", "could not set backoff", error=repr(exc))


def _queue_name(record: dict[str, Any]) -> str:
    return str(record["eventSourceARN"]).rsplit(":", 1)[1]


def _queue_url(name: str, record: dict[str, Any]) -> str:
    if name not in _queue_urls:
        account = str(record["eventSourceARN"]).split(":")[4]
        _queue_urls[name] = client("sqs").get_queue_url(
            QueueName=name, QueueOwnerAWSAccountId=account
        )["QueueUrl"]
    return _queue_urls[name]


def _event_key(record: dict[str, Any]) -> str:
    attr = record.get("messageAttributes", {}).get("event_key", {})
    return str(attr.get("stringValue") or record.get("messageId"))


def _out_of_time(context: Any) -> bool:
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return remaining is not None and remaining() < TIME_GUARD_MS
