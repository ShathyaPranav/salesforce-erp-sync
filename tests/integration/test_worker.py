"""Phase 1.3: the worker image, end to end (the happy paths).

Messages go onto the real (LocalStack) queue; the esm-pump harness feeds them
to the worker container exactly as Lambda's event source mapping would; the
assertions read DynamoDB. The first test starts one step earlier, at the Go
poller, so it also proves the two languages agree on the message format.

Every row of the failure table has its own test in test_failures.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Any

import requests

from tests.events import make_event
from tests.integration.conftest import FAKE_SF
from tests.integration.stack import (
    WATERMARK,
    delete_erp,
    freeze_salesforce,
    get_item,
    invoke_ingest,
    send,
    settled,
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


def test_happy_path_from_salesforce_poll_to_erp(sqs, ssm, ddb, fake_sf, clean_queue):
    # Only the valid seeded deals here; test_failures.py covers the invalid ones.
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
    settled(sqs, clean_queue)


def test_a_deal_edited_twice_ends_at_the_latest_amount(sqs, ddb, clean_queue, opp_id):
    def stored_version() -> Any:
        return (get_item(ddb, "orders", "order_id", opp_id) or {}).get("version")

    for when, amount in ((V1, 12000.0), (V2, 15000.0), (V3, 18000.0)):
        send(sqs, clean_queue, make_event(opp_id, when, amount=amount))
        wait_for(partial(lambda v: stored_version() == v, {"N": version(when)}), what=str(when))
    order = get_item(ddb, "orders", "order_id", opp_id)
    assert order is not None
    assert Decimal(order["total"]["N"]) == Decimal("18000")
    assert order["revision"] == {"N": "3"}
