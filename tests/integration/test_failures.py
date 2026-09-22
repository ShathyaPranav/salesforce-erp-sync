"""The failure-injection suite: one test per row of the failure table in
docs/PLAN.md. Each test triggers the failure on purpose and asserts Relay's
documented response, through the real queue, worker image and DynamoDB.

  1 Duplicate message ............. test_1_duplicate_message_is_harmless
  2 ERP unavailable ............... test_2a_..._backoff_then_dead_letters, test_2b_..._recovers
  3 Bad data ...................... test_3_bad_data_goes_straight_to_the_dlq_with_a_reason
  4 Out-of-order updates .......... test_4_newer_version_delivered_first_survives
  5 Worker crashes mid-batch ...... test_5_crash_mid_batch_redelivers_the_batch_safely
  6 Write succeeded, ack lost ..... test_6_write_succeeded_but_ack_lost_is_a_no_op
  7 Poller misses a record ........ test_7_reconciler_repairs_missed_and_stale_orders
  8 Deal reverted or deleted ...... test_8_reverted_and_deleted_deals_are_reported_only

Locally the backoff base is 1 s and the queue's visibility timeout 10 s (see
docker-compose.yml), so these run in about a minute instead of an hour.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
import requests

from tests.events import make_event
from tests.integration.conftest import FAKE_SF, pump
from tests.integration.stack import (
    drain,
    get_item,
    invoke_reconciler,
    pump_stats,
    queue_url,
    send,
    set_chaos,
    settled,
    wait_for,
)

V1 = datetime(2026, 9, 15, 9, 20, tzinfo=UTC)
V2 = V1 + timedelta(minutes=5)


def version(t: datetime) -> dict[str, str]:
    return {"N": str(int(t.timestamp() * 1000))}


@pytest.fixture
def chaos(ssm: Any) -> Iterator[Any]:
    set_chaos(ssm)
    yield ssm
    set_chaos(ssm)


def dlq_messages(sqs: Any) -> list[dict[str, Any]]:
    url = queue_url(sqs, "relay-events-dlq")
    got: list[dict[str, Any]] = sqs.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, MessageAttributeNames=["All"]
    ).get("Messages", [])
    return got


def order(ddb: Any, opp_id: str) -> dict[str, Any] | None:
    return get_item(ddb, "orders", "order_id", opp_id)


# ---- 1. Duplicate message ----------------------------------------------------------


def test_1_duplicate_message_is_harmless(sqs, ddb, clean_queue, opp_id):
    event = make_event(opp_id, V1)
    for _ in range(3):
        send(sqs, clean_queue, event)
    settled(sqs, clean_queue)
    stored = order(ddb, opp_id)
    assert stored is not None
    assert stored["revision"] == {"N": "1"}  # written once; two copies were no-ops
    assert stored["version"] == version(V1)


# ---- 2. ERP unavailable -------------------------------------------------------------


def test_2a_erp_down_retries_with_backoff_then_dead_letters(sqs, ddb, chaos, clean_queue, opp_id):
    set_chaos(chaos, erp_fail_rate=1.0)
    before = pump_stats()["item_failures"]
    started = time.monotonic()
    send(sqs, clean_queue, make_event(opp_id, V1))

    moved = wait_for(lambda: dlq_messages(sqs), timeout=90, what="the message to reach the DLQ")
    elapsed = time.monotonic() - started
    assert pump_stats()["item_failures"] - before == 5  # exactly maxReceiveCount attempts
    # Backoff delays of 1, 2, 4, 8 and 16 s (base 1 s) come before the move.
    assert elapsed >= 1 + 2 + 4 + 8, f"retried too fast: {elapsed:.1f}s"
    assert json.loads(moved[0]["Body"])["opportunity"]["id"] == opp_id
    assert "reason" not in moved[0].get("MessageAttributes", {})  # SQS moved it, not the worker
    assert order(ddb, opp_id) is None


def test_2b_erp_recovers_within_the_retry_budget(sqs, ddb, chaos, clean_queue, opp_id):
    set_chaos(chaos, erp_fail_rate=1.0)
    before = pump_stats()["item_failures"]
    send(sqs, clean_queue, make_event(opp_id, V1))
    wait_for(lambda: pump_stats()["item_failures"] - before >= 2, what="two failed attempts")
    set_chaos(chaos, erp_fail_rate=0.0)  # the ERP comes back

    stored = wait_for(lambda: order(ddb, opp_id), timeout=30, what="the retried write")
    assert stored["revision"] == {"N": "1"}
    settled(sqs, clean_queue)
    assert dlq_messages(sqs) == []


# ---- 3. Bad data ----------------------------------------------------------------------


def test_3_bad_data_goes_straight_to_the_dlq_with_a_reason(sqs, ddb, clean_queue, opp_id):
    before = pump_stats()["item_failures"]
    no_amount = make_event(opp_id, V1, amount=None)
    no_account = make_event(opp_id + "X", V1, account_id=None, account_name=None)
    send(sqs, clean_queue, no_amount)
    send(sqs, clean_queue, no_account)
    settled(sqs, clean_queue)

    dead: list[dict[str, Any]] = []

    def collected_both() -> bool:
        dead.extend(dlq_messages(sqs))
        return len(dead) >= 2

    wait_for(collected_both, what="both in the DLQ")
    reasons = {m["MessageAttributes"]["reason"]["StringValue"] for m in dead}
    assert reasons == {"missing_amount", "missing_account"}
    bodies = {m["Body"] for m in dead}
    assert json.dumps(no_amount) in bodies  # the original message, untouched
    assert pump_stats()["item_failures"] == before  # never retried
    assert order(ddb, opp_id) is None


# ---- 4. Out-of-order updates -----------------------------------------------------------


def test_4_newer_version_delivered_first_survives(sqs, ddb, clean_queue, opp_id):
    send(sqs, clean_queue, make_event(opp_id, V2, amount=15000.0))
    wait_for(lambda: order(ddb, opp_id), what="v2 to be written")
    send(sqs, clean_queue, make_event(opp_id, V1, amount=12000.0))
    settled(sqs, clean_queue)

    stored = order(ddb, opp_id)
    invoice = get_item(ddb, "invoices", "invoice_id", f"INV-{opp_id}")
    assert stored is not None and invoice is not None
    assert stored["version"] == version(V2)
    assert Decimal(stored["total"]["N"]) == Decimal("15000")
    assert Decimal(invoice["amount"]["N"]) == Decimal("15000")
    assert stored["revision"] == {"N": "1"}  # the stale v1 never wrote


# ---- 5. Worker crashes mid-batch ---------------------------------------------------------


def test_5_crash_mid_batch_redelivers_the_batch_safely(sqs, ddb, chaos, clean_queue, opp_id):
    ids = [opp_id, opp_id + "B", opp_id + "C"]
    pump("pause")
    for oid in ids:
        send(sqs, clean_queue, make_event(oid, V1))
    set_chaos(chaos, crash_after=2)  # die after writing two of the three
    crashes = pump_stats()["batch_failures"]
    pump("resume")

    wait_for(lambda: pump_stats()["batch_failures"] > crashes, what="the crash")
    set_chaos(chaos)
    written_before_redelivery = [oid for oid in ids if order(ddb, oid)]
    assert len(written_before_redelivery) == 2

    # The whole batch comes back after the visibility timeout; the two already
    # written hit the version check, the third is written now.
    settled(sqs, clean_queue, timeout=40)
    for oid in ids:
        stored = order(ddb, oid)
        assert stored is not None and stored["revision"] == {"N": "1"}, oid
    for oid in ids[1:]:
        ddb.delete_item(TableName="orders", Key={"order_id": {"S": oid}})
        ddb.delete_item(TableName="invoices", Key={"invoice_id": {"S": f"INV-{oid}"}})


# ---- 6. Write succeeded, ack lost ---------------------------------------------------------


def test_6_write_succeeded_but_ack_lost_is_a_no_op(sqs, ddb, chaos, clean_queue, opp_id):
    pump("pause")
    send(sqs, clean_queue, make_event(opp_id, V1))
    set_chaos(chaos, crash_after=1)  # commit the write, then die before the ack
    crashes = pump_stats()["batch_failures"]
    pump("resume")

    wait_for(lambda: pump_stats()["batch_failures"] > crashes, what="the crash")
    set_chaos(chaos)
    first = order(ddb, opp_id)
    assert first is not None  # the transaction had committed

    settled(sqs, clean_queue, timeout=40)  # redelivered after the visibility timeout
    assert order(ddb, opp_id) == first  # not even updated_at moved: a pure no-op


# ---- 7. Poller misses a record -------------------------------------------------------------


def put_deal(opp_id: str, stamp: str, **fields: Any) -> None:
    deal = {
        "Id": opp_id,
        "Name": f"Deal {opp_id}",
        "AccountId": "001FAKE00000000001",
        "Amount": 7000,
        "CloseDate": "2026-09-12",
        "StageName": "Closed Won",
        "SystemModstamp": stamp,
        **fields,
    }
    requests.put(f"{FAKE_SF}/__admin/opportunities", json=[deal], timeout=5).raise_for_status()


def drift_for(report: dict[str, Any], opp_id: str) -> str | None:
    return next((d["kind"] for d in report["items"] if d["opportunity_id"] == opp_id), None)


def test_7_reconciler_repairs_missed_and_stale_orders(sqs, ddb, fake_sf, clean_queue, opp_id):
    # A deal the poller never saw, and a deal whose order is behind Salesforce.
    missed, stale = opp_id, opp_id[:-1] + "S"
    put_deal(missed, "2026-09-16T08:00:00.000+0000")
    put_deal(stale, "2026-09-16T09:00:00.000+0000", Amount=9900)
    send(sqs, clean_queue, make_event(stale, datetime(2026, 9, 16, 8, tzinfo=UTC), amount=9000.0))
    wait_for(lambda: order(ddb, stale), what="the old version of the stale deal")

    report = invoke_reconciler()
    assert drift_for(report, missed) == "missing_order"
    assert drift_for(report, stale) == "stale_order"
    # The seeded deal with no Amount is reported, not re-sent to die in the DLQ again.
    assert drift_for(report, "006FAKE00000000003") == "invalid_in_source"

    wait_for(lambda: order(ddb, missed), what="the missed deal to be repaired")
    wait_for(
        lambda: Decimal((order(ddb, stale) or {}).get("total", {}).get("N", "0")) == 9900,
        what="the stale order to be updated",
    )
    settled(sqs, clean_queue)
    assert all(
        json.loads(m["Body"])["opportunity"]["id"] not in ("006FAKE00000000003",)
        for m in dlq_messages(sqs)
    )
    assert drift_for(invoke_reconciler(), missed) is None  # repaired: no drift next run
    ddb.delete_item(TableName="orders", Key={"order_id": {"S": stale}})


# ---- 8. Deal reverted or deleted in Salesforce ------------------------------------------------


def test_8_reverted_and_deleted_deals_are_reported_only(sqs, ddb, fake_sf, clean_queue, opp_id):
    reverted, deleted = opp_id, opp_id[:-1] + "D"
    for oid in (reverted, deleted):
        put_deal(oid, "2026-09-16T08:00:00.000+0000")
        send(sqs, clean_queue, make_event(oid, datetime(2026, 9, 16, 8, tzinfo=UTC)))
    settled(sqs, clean_queue)
    requests.patch(
        f"{FAKE_SF}/__admin/opportunities/{reverted}",
        json={
            "changes": {"StageName": "Negotiation/Review"},
            "SystemModstamp": "2026-09-17T08:00:00Z",
        },
        timeout=5,
    ).raise_for_status()
    requests.delete(f"{FAKE_SF}/__admin/opportunities/{deleted}", timeout=5).raise_for_status()

    report = invoke_reconciler()
    assert drift_for(report, reverted) == "not_won"
    assert drift_for(report, deleted) == "not_found"
    # Reported for a human; the orders are left exactly as they were.
    assert order(ddb, reverted) is not None and order(ddb, deleted) is not None
    ddb.delete_item(TableName="orders", Key={"order_id": {"S": deleted}})
    drain(sqs, clean_queue)
