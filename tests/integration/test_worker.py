"""Phase 1.3: the worker image, end to end.

Messages go onto the real (LocalStack) queue; the esm-pump harness feeds them
to the worker container exactly as Lambda's event source mapping would; the
assertions read DynamoDB. The first test starts one step earlier, at the Go
poller, so it also proves the two languages agree on the message format.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Any

import pytest
import requests

from tests.events import make_event
from tests.integration.conftest import FAKE_SF, pump
from tests.integration.stack import (
    WATERMARK,
    delete_erp,
    drain,
    freeze_salesforce,
    get_item,
    invoke_ingest,
    queue_is_empty,
    queue_url,
    wait_for,
)

V1 = datetime(2026, 9, 15, 9, 20, tzinfo=UTC)
V2 = V1 + timedelta(minutes=5)
V3 = V1 + timedelta(minutes=9)
VALID_SEEDED = [
    "006FAKE00000000001",
    "006FAKE00000000002",
    "006FAKE00000000004",
    "006FAKE00000000005",
]


def version(t: datetime) -> str:
    return str(int(t.timestamp() * 1000))


@pytest.fixture
def queue(sqs: Any) -> Any:
    """Both queues emptied (with the pump paused, so nothing is in flight),
    then the pump feeding the worker again."""
    url = queue_url(sqs)
    pump("pause")
    try:
        drain(sqs, url)
        drain(sqs, queue_url(sqs, "relay-events-dlq"))
    finally:
        pump("resume")
    return url


@pytest.fixture
def opp_id(ddb: Any) -> Any:
    oid = f"006W{uuid.uuid4().hex[:14].upper()}"
    yield oid
    delete_erp(ddb, oid)


def send(sqs: Any, url: str, event: dict[str, Any]) -> None:
    sqs.send_message(
        QueueUrl=url,
        MessageBody=json.dumps(event),
        MessageAttributes={"event_key": {"DataType": "String", "StringValue": event["event_key"]}},
    )


def settled(sqs: Any, url: str) -> None:
    wait_for(lambda: queue_is_empty(sqs, url), what="the worker to drain the queue")


def test_happy_path_from_salesforce_poll_to_erp(sqs, ssm, ddb, fake_sf, queue):
    # Only the valid seeded deals here; Phase 1.4 covers the invalid ones.
    for bad in ("006FAKE00000000003", "006FAKE00000000008"):
        requests.delete(f"{FAKE_SF}/__admin/opportunities/{bad}", timeout=5).raise_for_status()
    for oid in VALID_SEEDED:
        delete_erp(ddb, oid)
    ssm.put_parameter(Name=WATERMARK, Value="1970-01-01T00:00:00Z", Type="String", Overwrite=True)
    freeze_salesforce(datetime(2026, 9, 22, 10, 0, tzinfo=UTC))
    try:
        assert invoke_ingest()["published"] == 4
    finally:
        freeze_salesforce(None)

    orders = {
        oid: wait_for(partial(get_item, ddb, "orders", "order_id", oid), what=oid)
        for oid in VALID_SEEDED
    }
    acme = orders["006FAKE00000000001"]
    assert acme["customer_id"] == {"S": "001FAKE00000000001"}
    assert acme["customer_name"] == {"S": "Acme Robotics"}
    assert Decimal(acme["total"]["N"]) == Decimal("125000")
    assert acme["order_date"] == {"S": "2026-09-01"}
    assert acme["event_key"] == {"S": "006FAKE00000000001:2026-09-15T09:00:00.000Z"}
    assert acme["revision"] == {"N": "1"}

    invoice = get_item(ddb, "invoices", "invoice_id", "INV-006FAKE00000000001")
    assert invoice is not None and Decimal(invoice["amount"]["N"]) == Decimal("125000")
    customer = get_item(ddb, "customers", "customer_id", "001FAKE00000000001")
    assert customer is not None and customer["name"] == {"S": "Acme Robotics"}
    settled(sqs, queue)


def test_same_event_three_times_gives_one_order(sqs, ddb, queue, opp_id):
    event = make_event(opp_id, V1)
    for _ in range(3):
        send(sqs, queue, event)
    settled(sqs, queue)
    order = get_item(ddb, "orders", "order_id", opp_id)
    assert order is not None
    assert order["revision"] == {"N": "1"}  # written once; the other two were duplicates
    assert order["version"] == {"N": version(V1)}


def test_newer_edit_delivered_first_survives_the_older_one(sqs, ddb, queue, opp_id):
    send(sqs, queue, make_event(opp_id, V2, amount=15000.0))
    wait_for(lambda: get_item(ddb, "orders", "order_id", opp_id), what="v2 to be written")
    send(sqs, queue, make_event(opp_id, V1, amount=12000.0))
    settled(sqs, queue)

    order = get_item(ddb, "orders", "order_id", opp_id)
    invoice = get_item(ddb, "invoices", "invoice_id", f"INV-{opp_id}")
    assert order is not None and invoice is not None
    assert order["version"] == {"N": version(V2)}
    assert Decimal(order["total"]["N"]) == Decimal("15000")
    assert Decimal(invoice["amount"]["N"]) == Decimal("15000")
    assert order["revision"] == {"N": "1"}  # the stale v1 never wrote


def test_a_deal_edited_twice_ends_at_the_latest_amount(sqs, ddb, queue, opp_id):
    def stored_version() -> Any:
        return (get_item(ddb, "orders", "order_id", opp_id) or {}).get("version")

    for when, amount in ((V1, 12000.0), (V2, 15000.0), (V3, 18000.0)):
        send(sqs, queue, make_event(opp_id, when, amount=amount))
        wait_for(partial(lambda v: stored_version() == v, {"N": version(when)}), what=str(when))
    order = get_item(ddb, "orders", "order_id", opp_id)
    assert order is not None
    assert Decimal(order["total"]["N"]) == Decimal("18000")
    assert order["revision"] == {"N": "3"}
