"""Validation and field mapping: Salesforce event -> ERP order."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from erp.writer import Order
from tests.events import body, make_event
from worker.mapping import PermanentError, parse_body, to_order

V1 = datetime(2026, 9, 15, 9, 20, tzinfo=UTC)


def order_for(**overrides: Any) -> Order:
    return to_order(parse_body(body(make_event("006FAKE00000000004", V1, **overrides))))


def test_maps_salesforce_fields_to_erp_fields():
    order = order_for()
    assert order.order_id == "006FAKE00000000004"
    assert order.customer_id == "001FAKE00000000004"
    assert order.customer_name == "Umbrella Health"
    assert order.opportunity_name == "Umbrella - pilot"
    assert order.order_date == "2026-09-10"
    assert order.version == int(V1.timestamp() * 1000)
    assert order.event_key == "006FAKE00000000004:2026-09-15T09:20:00.000Z"


def test_amount_is_an_exact_decimal_never_a_float():
    order = to_order(parse_body(body(make_event("006X", V1)).replace("12000.0", "1234.10")))
    assert order.total == Decimal("1234.10")
    assert isinstance(order.total, Decimal)
    assert order_for(amount=5).total == Decimal(5)


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"amount": None}, "missing_amount"),
        ({"account_id": None}, "missing_account"),
        ({"account_id": ""}, "missing_account"),
        ({"account_name": None}, "missing_field"),
        ({"close_date": None}, "missing_field"),
        ({"close_date": "10/09/2026"}, "invalid_field"),
        ({"amount": -1}, "invalid_field"),
        ({"amount": "lots"}, "invalid_field"),
        ({"amount": True}, "invalid_field"),
    ],
)
def test_bad_data_is_a_permanent_error_with_a_reason(overrides, reason):
    with pytest.raises(PermanentError) as exc:
        order_for(**overrides)
    assert exc.value.reason == reason


def test_unknown_schema_and_broken_json_are_permanent():
    event = make_event("006X", V1)
    event["schema"] = "relay.opportunity.v2"
    with pytest.raises(PermanentError) as exc:
        to_order(event)
    assert exc.value.reason == "unknown_schema"
    with pytest.raises(PermanentError) as exc:
        parse_body("{not json")
    assert exc.value.reason == "malformed_json"
    with pytest.raises(PermanentError):
        parse_body("[1, 2]")


def test_version_must_be_a_positive_integer():
    for bad in (None, "1789", 0, -5, 1.5, True):
        event = make_event("006X", V1)
        event["version"] = bad
        with pytest.raises(PermanentError):
            to_order(event)
