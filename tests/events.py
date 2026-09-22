"""Build relay.opportunity.v1 events in tests, exactly as the Go poller does
(internal/events/event.go). test_ingest checks the real poller's output
against the same shape."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

UNSET: Any = object()


def make_event(
    opp_id: str,
    modstamp: datetime,
    *,
    amount: Any = 12000.0,
    account_id: Any = "001FAKE00000000004",
    account_name: Any = "Umbrella Health",
    name: str = "Umbrella - pilot",
    close_date: Any = "2026-09-10",
) -> dict[str, Any]:
    stamp = modstamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {
        "schema": "relay.opportunity.v1",
        "event_key": f"{opp_id}:{stamp}",
        "version": int(modstamp.timestamp() * 1000),
        "opportunity": {
            "id": opp_id,
            "name": name,
            "stage_name": "Closed Won",
            "amount": amount,
            "close_date": close_date,
            "account_id": account_id,
            "account_name": account_name,
            "system_modstamp": stamp,
        },
        "published_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def body(event: dict[str, Any]) -> str:
    return json.dumps(event)
