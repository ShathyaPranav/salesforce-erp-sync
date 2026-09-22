"""Reconciler Lambda: EventBridge Scheduler runs it nightly; relayctl can too.

Invoke with {} for a normal run, or {"dry_run": true} to report without
re-enqueueing anything.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3

from erp.schema import ORDERS
from reconciler.reconcile import WON_SOQL, compare, scan_order_versions, stage_lookup
from reconciler.salesforce import Salesforce
from worker.logs import log
from worker.metrics import emit

PREFIX = os.environ.get("RELAY_PARAM_PREFIX", "/relay")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "relay-events")
GRACE_MINUTES = int(os.environ.get("RECONCILE_GRACE_MINUTES", "10"))


def handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    event = event or {}
    region = os.environ.get("AWS_REGION", "us-east-1")
    ssm = boto3.client("ssm", region_name=region)
    ddb = boto3.client("dynamodb", region_name=region)
    sqs = boto3.client("sqs", region_name=region)

    sf = _salesforce(ssm)
    now = datetime.now(UTC)
    report = compare(
        won_deals=sf.query_all(WON_SOQL),
        order_versions=scan_order_versions(ddb, ORDERS.name),
        lookup_stages=stage_lookup(sf),
        now=now,
        grace=timedelta(minutes=int(event.get("grace_minutes", GRACE_MINUTES))),
    )

    enqueued = 0
    if report.to_enqueue and not event.get("dry_run"):
        url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        for i in range(0, len(report.to_enqueue), 10):
            chunk = report.to_enqueue[i : i + 10]
            out = sqs.send_message_batch(
                QueueUrl=url,
                Entries=[
                    {
                        "Id": str(n),
                        "MessageBody": json.dumps(ev),
                        "MessageAttributes": {
                            "event_key": {"DataType": "String", "StringValue": ev["event_key"]},
                            "source": {"DataType": "String", "StringValue": "reconciler"},
                        },
                    }
                    for n, ev in enumerate(chunk)
                ],
            )
            enqueued += len(out.get("Successful", []))

    for d in report.drift:
        log("warn", "drift", kind=d.kind, opportunity_id=d.opportunity_id, detail=d.detail)
    summary = {
        "checked_deals": report.checked_deals,
        "checked_orders": report.checked_orders,
        "drift": report.counts(),
        "drift_found": len(report.drift),
        "reenqueued": enqueued,
        "dry_run": bool(event.get("dry_run")),
        "items": [d.__dict__ for d in report.drift],
    }
    log("info", "reconcile complete", **{k: v for k, v in summary.items() if k != "items"})
    emit("reconciler", {"DriftFound": len(report.drift)})
    return summary


def _salesforce(ssm: Any) -> Salesforce:
    names = [
        f"{PREFIX}/salesforce/{n}"
        for n in ("login_url", "api_version", "client_id", "client_secret")
    ]
    params = ssm.get_parameters(Names=names, WithDecryption=True)
    values = {p["Name"].rsplit("/", 1)[1]: p["Value"] for p in params["Parameters"]}
    missing = [n for n in names if n.rsplit("/", 1)[1] not in values]
    if missing:
        raise RuntimeError(f"missing SSM parameters: {missing}")
    return Salesforce(
        login_url=values["login_url"],
        api_version=values["api_version"],
        client_id=values["client_id"],
        client_secret=values["client_secret"],
    )
