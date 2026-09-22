"""Phase 1.3: the conditional ERP write, against LocalStack's DynamoDB.

These call erp.writer directly, so each case is fast and exact. The
end-to-end versions (through SQS and the worker image) are in test_worker.py.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from erp.writer import Order, WriteResult, write_order

V1 = datetime(2026, 9, 15, 9, 20, tzinfo=UTC)
V2 = V1 + timedelta(minutes=5)
V3 = V1 + timedelta(minutes=9)


def order(
    order_id: str, when: datetime, total: str, customer_name: str = "Umbrella Health"
) -> Order:
    stamp = when.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return Order(
        order_id=order_id,
        customer_id=f"001-{order_id}",
        customer_name=customer_name,
        opportunity_name="Umbrella - pilot",
        total=Decimal(total),
        order_date="2026-09-10",
        version=int(when.timestamp() * 1000),
        source_modstamp=stamp,
        event_key=f"{order_id}:{stamp}",
    )


@pytest.fixture
def opp_id(ddb: Any) -> Any:
    oid = f"006T{uuid.uuid4().hex[:14].upper()}"
    yield oid
    ddb.delete_item(TableName="orders", Key={"order_id": {"S": oid}})
    ddb.delete_item(TableName="invoices", Key={"invoice_id": {"S": f"INV-{oid}"}})
    ddb.delete_item(TableName="customers", Key={"customer_id": {"S": f"001-{oid}"}})


def items(ddb: Any, oid: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    def get(table: str, key: str, value: str) -> dict[str, Any]:
        item: dict[str, Any] = ddb.get_item(
            TableName=table, Key={key: {"S": value}}, ConsistentRead=True
        ).get("Item", {})
        return item

    return (
        get("orders", "order_id", oid),
        get("invoices", "invoice_id", f"INV-{oid}"),
        get("customers", "customer_id", f"001-{oid}"),
    )


def test_new_deal_creates_order_invoice_and_customer(ddb, opp_id):
    assert write_order(ddb, order(opp_id, V1, "12000.0"), now="t1") is WriteResult.WRITTEN
    o, inv, cust = items(ddb, opp_id)
    assert Decimal(o["total"]["N"]) == Decimal("12000")
    assert o["version"]["N"] == str(int(V1.timestamp() * 1000))
    assert o["revision"] == {"N": "1"}
    assert o["created_at"] == {"S": "t1"}
    assert inv["order_id"] == {"S": opp_id}
    assert Decimal(inv["amount"]["N"]) == Decimal("12000")
    assert inv["status"] == {"S": "ISSUED"}
    assert inv["version"] == o["version"]
    assert cust["name"] == {"S": "Umbrella Health"}


def test_same_version_again_is_a_duplicate_and_changes_nothing(ddb, opp_id):
    write_order(ddb, order(opp_id, V1, "12000"), now="t1")
    before = items(ddb, opp_id)
    for attempt in range(3):
        result = write_order(ddb, order(opp_id, V1, "12000"), now=f"t{attempt + 2}")
        assert result is WriteResult.DUPLICATE
    assert items(ddb, opp_id) == before  # not even updated_at moved


def test_newer_version_updates_order_and_invoice_together(ddb, opp_id):
    write_order(ddb, order(opp_id, V1, "12000"), now="t1")
    result = write_order(ddb, order(opp_id, V2, "15000", customer_name="Umbrella Corp"), now="t2")
    assert result is WriteResult.WRITTEN
    o, inv, cust = items(ddb, opp_id)
    assert Decimal(o["total"]["N"]) == Decimal("15000")
    assert o["revision"] == {"N": "2"}
    assert o["created_at"] == {"S": "t1"} and o["updated_at"] == {"S": "t2"}
    assert Decimal(inv["amount"]["N"]) == Decimal("15000")
    assert inv["version"] == o["version"]
    assert cust["name"] == {"S": "Umbrella Corp"}


def test_older_version_after_newer_is_stale_and_dropped(ddb, opp_id):
    write_order(ddb, order(opp_id, V2, "15000"), now="t1")
    before = items(ddb, opp_id)
    assert write_order(ddb, order(opp_id, V1, "12000"), now="t2") is WriteResult.STALE
    assert items(ddb, opp_id) == before  # the invoice and customer weren't touched either


def test_any_delivery_order_converges_to_the_newest_version(ddb, opp_id):
    deliveries = [(V2, "15000"), (V1, "12000"), (V3, "18000"), (V2, "15000"), (V1, "12000")]
    results = [write_order(ddb, order(opp_id, when, total), now="t") for when, total in deliveries]
    assert results == [
        WriteResult.WRITTEN,
        WriteResult.STALE,
        WriteResult.WRITTEN,
        WriteResult.STALE,
        WriteResult.STALE,
    ]
    o, inv, _ = items(ddb, opp_id)
    assert Decimal(o["total"]["N"]) == Decimal("18000")
    assert Decimal(inv["amount"]["N"]) == Decimal("18000")
    assert o["revision"] == {"N": "2"}  # written exactly twice: V2, then V3
