"""Build relay.opportunity.v1 events from Salesforce rows, in Python.

The Go poller (internal/events/event.go) is the main producer. The reconciler
re-enqueues missed deals, so it needs the same builder. To stop the two from
drifting apart, both are checked against one golden file:
tests/contract/fixtures/event_golden.json.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

SCHEMA = "relay.opportunity.v1"


def parse_modstamp(value: str) -> datetime:
    """Parse Salesforce's 2026-09-15T09:00:00.000+0000 (or ...Z) into aware UTC."""
    text = value.replace("Z", "+00:00")
    if len(text) >= 5 and text[-5] in "+-" and text[-3] != ":":
        text = text[:-2] + ":" + text[-2:]
    return datetime.fromisoformat(text).astimezone(UTC)


def from_salesforce(row: dict[str, Any], published_at: datetime) -> dict[str, Any]:
    stamp = parse_modstamp(row["SystemModstamp"])
    normalised = stamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{stamp.microsecond // 1000:03d}Z"
    account = row.get("Account")
    return {
        "schema": SCHEMA,
        "event_key": f"{row['Id']}:{normalised}",
        "version": int(stamp.timestamp() * 1000),
        "opportunity": {
            "id": row["Id"],
            "name": row.get("Name") or "",
            "stage_name": row.get("StageName") or "",
            "amount": row.get("Amount"),
            "close_date": row.get("CloseDate"),
            "account_id": row.get("AccountId"),
            "account_name": account.get("Name") if isinstance(account, dict) else None,
            "system_modstamp": normalised,
        },
        "published_at": published_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
