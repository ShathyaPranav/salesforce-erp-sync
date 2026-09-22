"""The idempotent, version-checked ERP write.

One DynamoDB transaction writes the order, its invoice and the customer. Only
the order carries a condition:

    attribute_not_exists(order_id) OR version < :incoming_version

  * no order yet            -> condition passes: order created (revision 1)
  * incoming version newer  -> condition passes: order updated (revision + 1)
  * same version again      -> condition fails: DUPLICATE, nothing changes
  * incoming version older  -> condition fails: STALE, nothing changes

Because all three items are in one transaction, a failed condition on the
order cancels the invoice and customer writes too, so the three can never
disagree. That is what makes a redelivered message harmless: "exactly-once
effect" comes from this condition, not from the queue.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from erp.schema import CUSTOMERS, INVOICES, ORDERS, invoice_id_for


class WriteResult(enum.Enum):
    WRITTEN = "written"  # created or updated
    DUPLICATE = "duplicate"  # this exact version is already stored
    STALE = "stale"  # a newer version is already stored


class ErpUnavailable(Exception):
    """The ERP couldn't take the write right now; retrying later can succeed."""


class ErpRejected(Exception):
    """The ERP refused the item itself; retrying the same data won't help."""


# Cancellation codes that mean "try again later" rather than "your data is wrong".
_TRANSIENT_CODES = {
    "TransactionConflict",  # another transaction touched one of the items
    "ThrottlingError",
    "ProvisionedThroughputExceeded",
    "RequestLimitExceeded",
}
_TRANSIENT_ERRORS = {
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "TransactionInProgressException",
    "InternalServerError",
    "ServiceUnavailable",
}


@dataclass(frozen=True)
class Order:
    """One deal, already validated and mapped to ERP fields."""

    order_id: str  # Opportunity.Id
    customer_id: str  # Opportunity.AccountId
    customer_name: str  # Account.Name
    opportunity_name: str
    total: Decimal  # Amount
    order_date: str  # CloseDate, YYYY-MM-DD
    version: int  # SystemModstamp in epoch milliseconds
    source_modstamp: str
    event_key: str


def write_order(ddb: Any, order: Order, now: str) -> WriteResult:
    """Write the order, invoice and customer in one transaction."""
    try:
        ddb.transact_write_items(TransactItems=_transaction(order, now))
    except ClientError as exc:
        return _classify(ddb, exc, order)
    return WriteResult.WRITTEN


def _transaction(order: Order, now: str) -> list[dict[str, Any]]:
    s = _s
    return [
        # 0: the order, the only conditional item.
        {
            "Update": {
                "TableName": ORDERS.name,
                "Key": {"order_id": s(order.order_id)},
                "UpdateExpression": (
                    "SET customer_id = :cid, customer_name = :cname, opportunity_name = :oname,"
                    " #total = :total, order_date = :odate, #version = :v,"
                    " source_modstamp = :ms, event_key = :ek, updated_at = :now,"
                    " created_at = if_not_exists(created_at, :now),"
                    " revision = if_not_exists(revision, :zero) + :one"
                ),
                "ConditionExpression": "attribute_not_exists(order_id) OR #version < :v",
                "ExpressionAttributeNames": {"#total": "total", "#version": "version"},
                "ExpressionAttributeValues": {
                    ":cid": s(order.customer_id),
                    ":cname": s(order.customer_name),
                    ":oname": s(order.opportunity_name),
                    ":total": _n(order.total),
                    ":odate": s(order.order_date),
                    ":v": _n(order.version),
                    ":ms": s(order.source_modstamp),
                    ":ek": s(order.event_key),
                    ":now": s(now),
                    ":zero": _n(0),
                    ":one": _n(1),
                },
                # On failure, DynamoDB hands back the stored order, so we can
                # tell a duplicate from a stale version without another read.
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
            }
        },
        # 1: the invoice mirrors the latest order version.
        {
            "Update": {
                "TableName": INVOICES.name,
                "Key": {"invoice_id": s(invoice_id_for(order.order_id))},
                "UpdateExpression": (
                    "SET order_id = :oid, customer_id = :cid, amount = :amt, issue_date = :d,"
                    " #status = :st, #version = :v, event_key = :ek, updated_at = :now,"
                    " created_at = if_not_exists(created_at, :now)"
                ),
                "ExpressionAttributeNames": {"#status": "status", "#version": "version"},
                "ExpressionAttributeValues": {
                    ":oid": s(order.order_id),
                    ":cid": s(order.customer_id),
                    ":amt": _n(order.total),
                    ":d": s(order.order_date),
                    ":st": s("ISSUED"),
                    ":v": _n(order.version),
                    ":ek": s(order.event_key),
                    ":now": s(now),
                },
            }
        },
        # 2: the customer, upserted with no condition of its own (see docs/data-model.md).
        {
            "Update": {
                "TableName": CUSTOMERS.name,
                "Key": {"customer_id": s(order.customer_id)},
                "UpdateExpression": (
                    "SET #name = :name, last_event_key = :ek, updated_at = :now,"
                    " created_at = if_not_exists(created_at, :now)"
                ),
                "ExpressionAttributeNames": {"#name": "name"},
                "ExpressionAttributeValues": {
                    ":name": s(order.customer_name),
                    ":ek": s(order.event_key),
                    ":now": s(now),
                },
            }
        },
    ]


def _classify(ddb: Any, exc: ClientError, order: Order) -> WriteResult:
    code = exc.response.get("Error", {}).get("Code", "")
    if code == "TransactionCanceledException":
        reasons: list[Any] = list(exc.response.get("CancellationReasons", []))
        codes = [r.get("Code", "None") for r in reasons]
        if codes and codes[0] == "ConditionalCheckFailed":
            stored = reasons[0].get("Item") or _read_order(ddb, order.order_id)
            stored_version = int(stored["version"]["N"]) if stored else -1
            if stored_version == order.version:
                return WriteResult.DUPLICATE
            if stored_version > order.version:
                return WriteResult.STALE
            # The stored version is older, yet the condition failed: the order
            # changed between our write and our read. Retry and let it settle.
            raise ErpUnavailable("order changed during the write; retrying") from exc
        if any(c in _TRANSIENT_CODES for c in codes):
            raise ErpUnavailable(f"transaction cancelled: {codes}") from exc
        if "ValidationError" in codes:
            raise ErpRejected(f"transaction cancelled: {codes}") from exc
        raise ErpUnavailable(f"transaction cancelled: {codes}") from exc
    if code == "ValidationException":
        raise ErpRejected(exc.response.get("Error", {}).get("Message", code)) from exc
    if code in _TRANSIENT_ERRORS:
        raise ErpUnavailable(code) from exc
    raise exc


def _read_order(ddb: Any, order_id: str) -> dict[str, Any] | None:
    item: dict[str, Any] | None = ddb.get_item(
        TableName=ORDERS.name, Key={"order_id": _s(order_id)}, ConsistentRead=True
    ).get("Item")
    return item


def _s(value: str) -> dict[str, str]:
    return {"S": value}


def _n(value: Decimal | int) -> dict[str, str]:
    return {"N": str(value)}
