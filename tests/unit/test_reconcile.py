"""The reconciler's comparison, as a pure function (no AWS, no Salesforce)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from reconciler.reconcile import compare

NOW = datetime(2026, 9, 23, 2, 0, tzinfo=UTC)
OLD = "2026-09-15T09:00:00.000+0000"
V_OLD = int(datetime(2026, 9, 15, 9, 0, tzinfo=UTC).timestamp() * 1000)


def deal(opp_id: str, stamp: str = OLD, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "Id": opp_id,
        "Name": f"Deal {opp_id}",
        "AccountId": "001A",
        "Account": {"Name": "Acme"},
        "Amount": 1000.0,
        "CloseDate": "2026-09-01",
        "StageName": "Closed Won",
        "SystemModstamp": stamp,
    }
    row.update(overrides)
    return row


def no_lookup(ids: list[str]) -> dict[str, dict[str, Any]]:
    raise AssertionError(f"unexpected lookup {ids}")


def kinds(report: Any) -> dict[str, str]:
    return {d.opportunity_id: d.kind for d in report.drift}


def test_in_sync_means_no_drift():
    report = compare([deal("006A")], {"006A": V_OLD}, no_lookup, NOW)
    assert report.drift == [] and report.to_enqueue == []


def test_missing_order_is_reenqueued_with_a_valid_event():
    report = compare([deal("006A")], {}, no_lookup, NOW)
    assert kinds(report) == {"006A": "missing_order"}
    [event] = report.to_enqueue
    assert event["event_key"] == "006A:2026-09-15T09:00:00.000Z"
    assert event["version"] == V_OLD


def test_order_behind_salesforce_is_reenqueued():
    report = compare([deal("006A")], {"006A": V_OLD - 60_000}, no_lookup, NOW)
    assert kinds(report) == {"006A": "stale_order"}
    assert len(report.to_enqueue) == 1


def test_invalid_deal_is_reported_not_reenqueued():
    report = compare([deal("006B", Amount=None)], {}, no_lookup, NOW)
    assert kinds(report) == {"006B": "invalid_in_source"}
    assert report.drift[0].detail == "missing_amount"
    assert report.to_enqueue == []  # re-sending would only refill the DLQ


def test_recent_changes_are_left_to_the_poller():
    recent = (NOW - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    report = compare([deal("006A", recent)], {}, no_lookup, NOW)
    assert report.drift == [] and report.checked_deals == 0


def test_reverted_and_deleted_deals_are_reported_only():
    def lookup(ids: list[str]) -> dict[str, dict[str, Any]]:
        assert ids == ["006GONE", "006REV"]
        return {"006REV": {"Id": "006REV", "StageName": "Negotiation/Review", "IsWon": False}}

    orders = {"006REV": V_OLD, "006GONE": V_OLD, "006SMOKEabc": V_OLD}
    report = compare([], orders, lookup, NOW)
    assert kinds(report) == {"006REV": "not_won", "006GONE": "not_found"}
    assert report.to_enqueue == []  # smoke-test orders are ignored too
