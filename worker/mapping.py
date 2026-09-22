"""Validate an event and map Salesforce fields to ERP fields.

Salesforce -> ERP:
    Opportunity.Id          -> order_id (and INV-<Id> for the invoice)
    Opportunity.AccountId   -> customer_id
    Account.Name            -> customer_name
    Opportunity.Name        -> opportunity_name
    Opportunity.Amount      -> total (and invoice amount)
    Opportunity.CloseDate   -> order_date (and invoice issue_date)
    SystemModstamp          -> version (epoch ms, set by the poller)

Anything that makes an event unmappable raises PermanentError: retrying the
same message can never fix missing data, so it goes straight to the DLQ.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from erp.writer import Order

SCHEMA = "relay.opportunity.v1"
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PermanentError(Exception):
    """The message itself is wrong. `reason` is a short machine-readable code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def parse_body(body: str) -> dict[str, Any]:
    """Parse a message body. Numbers become Decimal: DynamoDB rejects floats."""
    try:
        event = json.loads(body, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise PermanentError("malformed_json", str(exc)) from exc
    if not isinstance(event, dict):
        raise PermanentError("malformed_json", "body is not a JSON object")
    return event


def to_order(event: dict[str, Any]) -> Order:
    """Validate one event and map it to an ERP order."""
    if event.get("schema") != SCHEMA:
        raise PermanentError("unknown_schema", str(event.get("schema")))
    opp = event.get("opportunity")
    if not isinstance(opp, dict):
        raise PermanentError("missing_field", "opportunity")

    order_id = _required_str(opp, "id")
    event_key = _required_str(event, "event_key")
    version = event.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise PermanentError("invalid_field", "version")

    if opp.get("account_id") in (None, ""):
        raise PermanentError("missing_account", "the deal has no Account")
    if opp.get("amount") is None:
        raise PermanentError("missing_amount", "the deal has no Amount")
    total = _amount(opp["amount"])
    close_date = _required_str(opp, "close_date")
    if not _DATE.match(close_date):
        raise PermanentError("invalid_field", "close_date")

    return Order(
        order_id=order_id,
        customer_id=_required_str(opp, "account_id"),
        customer_name=_required_str(opp, "account_name"),
        opportunity_name=str(opp.get("name") or ""),
        total=total,
        order_date=close_date,
        version=version,
        source_modstamp=_required_str(opp, "system_modstamp"),
        event_key=event_key,
    )


def _required_str(obj: dict[str, Any], name: str) -> str:
    value = obj.get(name)
    if not isinstance(value, str) or not value:
        raise PermanentError("missing_field", name)
    return value


def _amount(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise PermanentError("invalid_field", "amount")
    try:
        total = Decimal(str(value))
    except InvalidOperation as exc:
        raise PermanentError("invalid_field", "amount") from exc
    if not total.is_finite() or total < 0:
        raise PermanentError("invalid_field", "amount")
    return total
