"""Helpers for driving the docker compose stack from tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

import requests

from tests.integration.conftest import FAKE_SF

INGEST = "http://127.0.0.1:9001/2015-03-31/functions/function/invocations"
WATERMARK = "/relay/ingest/watermark"


def invoke_ingest() -> dict[str, Any]:
    """Invoke the ingest Lambda the way EventBridge Scheduler does."""
    resp = requests.post(INGEST, data="{}", timeout=60)
    resp.raise_for_status()
    result: dict[str, Any] = resp.json()
    return result


def freeze_salesforce(t: datetime | None) -> None:
    requests.post(
        f"{FAKE_SF}/__admin/clock",
        json={"now": None if t is None else t.isoformat()},
        timeout=5,
    ).raise_for_status()


def queue_url(sqs: Any, name: str = "relay-events") -> str:
    url: str = sqs.get_queue_url(QueueName=name)["QueueUrl"]
    return url


def drain(sqs: Any, url: str) -> list[dict[str, Any]]:
    """Receive and delete everything on a queue."""
    messages: list[dict[str, Any]] = []
    while True:
        got = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["All"],
        ).get("Messages", [])
        if not got:
            return messages
        for m in got:
            messages.append(m)
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])


def queue_is_empty(sqs: Any, url: str) -> bool:
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return all(int(v) == 0 for v in attrs.values())


def wait_for(check: Callable[[], Any], timeout: float = 30.0, what: str = "condition") -> Any:
    """Poll until check() returns something truthy, and return it."""
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(0.3)


def get_item(ddb: Any, table: str, key: str, value: str) -> dict[str, Any] | None:
    item: dict[str, Any] | None = ddb.get_item(
        TableName=table, Key={key: {"S": value}}, ConsistentRead=True
    ).get("Item")
    return item


def delete_erp(ddb: Any, order_id: str, customer_id: str | None = None) -> None:
    ddb.delete_item(TableName="orders", Key={"order_id": {"S": order_id}})
    ddb.delete_item(TableName="invoices", Key={"invoice_id": {"S": f"INV-{order_id}"}})
    if customer_id:
        ddb.delete_item(TableName="customers", Key={"customer_id": {"S": customer_id}})
