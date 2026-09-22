"""Nightly reconciliation: compare Salesforce with the ERP and repair drift.

The poller can miss things (a transaction that commits later than the lag, a
bug, a long outage that outlasts the retry budget). This is the safety net.
Per Closed Won deal in Salesforce:

  no ERP order, deal valid        -> missing_order: re-enqueue it
  no ERP order, deal invalid      -> invalid_in_source: report only (re-enqueueing
                                     would just dead-letter it again every night)
  ERP order at an older version   -> stale_order: re-enqueue the current version
  ERP order at a newer version    -> erp_ahead: report (shouldn't happen)

Per ERP order whose deal is no longer Closed Won in Salesforce:

  deal exists but isn't won       -> not_won: report only (reverted after closing)
  deal not returned at all        -> not_found: report only (deleted)

Reverts and deletes are reported, not "fixed": undoing an order or invoice is
a business decision for a human, not something a sync job should guess at.

Deals modified within the last `grace` are skipped: the poller simply hasn't
reached them yet, and they aren't drift.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from reconciler.events import from_salesforce, parse_modstamp
from worker.mapping import PermanentError, to_order

SMOKE_PREFIX = "006SMOKE"  # synthetic deals created by the CI smoke test
_SF_ID = re.compile(r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?")

WON_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp "
    "FROM Opportunity WHERE IsWon = true ORDER BY SystemModstamp ASC, Id ASC"
)


@dataclass(frozen=True)
class Drift:
    kind: str
    opportunity_id: str
    detail: str = ""


@dataclass
class Report:
    drift: list[Drift] = field(default_factory=list)
    to_enqueue: list[dict[str, Any]] = field(default_factory=list)
    checked_deals: int = 0
    checked_orders: int = 0

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for d in self.drift:
            out[d.kind] = out.get(d.kind, 0) + 1
        return out


def compare(
    won_deals: Iterable[dict[str, Any]],
    order_versions: dict[str, int],
    lookup_stages: Callable[[list[str]], dict[str, dict[str, Any]]],
    now: datetime,
    grace: timedelta = timedelta(minutes=10),
) -> Report:
    """Pure comparison. lookup_stages(ids) returns {Id: row} for ids that exist."""
    report = Report(checked_orders=len(order_versions))
    won_ids: set[str] = set()
    for row in won_deals:
        opp_id = row["Id"]
        won_ids.add(opp_id)
        if parse_modstamp(row["SystemModstamp"]) > now - grace:
            continue  # still in flight
        report.checked_deals += 1
        event = from_salesforce(row, now)
        stored = order_versions.get(opp_id)
        if stored is None:
            try:
                to_order(event)
            except PermanentError as exc:
                report.drift.append(Drift("invalid_in_source", opp_id, exc.reason))
                continue
            report.drift.append(Drift("missing_order", opp_id))
            report.to_enqueue.append(event)
        elif stored < event["version"]:
            report.drift.append(Drift("stale_order", opp_id, f"{stored} < {event['version']}"))
            report.to_enqueue.append(event)
        elif stored > event["version"]:
            report.drift.append(Drift("erp_ahead", opp_id, f"{stored} > {event['version']}"))

    orphans = sorted(
        oid for oid in order_versions if oid not in won_ids and not oid.startswith(SMOKE_PREFIX)
    )
    if orphans:
        found = lookup_stages(orphans)
        for oid in orphans:
            current = found.get(oid)
            if current is None:
                report.drift.append(Drift("not_found", oid, "deleted in Salesforce"))
            else:
                report.drift.append(Drift("not_won", oid, f"stage is {current.get('StageName')}"))
    return report


def stage_lookup(sf: Any) -> Callable[[list[str]], dict[str, dict[str, Any]]]:
    def lookup(ids: list[str]) -> dict[str, dict[str, Any]]:
        # Only well-formed Salesforce IDs go into the SOQL string, so nothing
        # read from the ERP table can change the query's meaning.
        safe = [x for x in ids if _SF_ID.fullmatch(x)]
        found: dict[str, dict[str, Any]] = {}
        for i in range(0, len(safe), 100):
            quoted = ", ".join(f"'{x}'" for x in safe[i : i + 100])
            soql = f"SELECT Id, StageName, IsWon FROM Opportunity WHERE Id IN ({quoted})"  # noqa: S608
            for row in sf.query_all(soql):
                found[row["Id"]] = row
        return found

    return lookup


def scan_order_versions(ddb: Any, table: str) -> dict[str, int]:
    versions: dict[str, int] = {}
    kwargs: dict[str, Any] = {
        "TableName": table,
        "ProjectionExpression": "order_id, #v",
        "ExpressionAttributeNames": {"#v": "version"},
    }
    while True:
        page = ddb.scan(**kwargs)
        for item in page.get("Items", []):
            versions[item["order_id"]["S"]] = int(item["version"]["N"])
        if "LastEvaluatedKey" not in page:
            return versions
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
