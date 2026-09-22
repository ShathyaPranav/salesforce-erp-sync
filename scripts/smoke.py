"""Smoke test: one synthetic deal through the real queue, worker and ERP.

    python scripts/smoke.py --local              # the docker compose stack
    python scripts/smoke.py --profile relay      # your deployed stack
    python scripts/smoke.py                      # CI, with the OIDC role's credentials

Every run uses a fresh Opportunity ID (006SMOKE...) and version, so it can't
pass by finding a previous run's order, and it deletes what it wrote. The
reconciler ignores the 006SMOKE prefix.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.aws_session import client, refuse_emulator_env, session, stack_outputs

CUSTOMER_ID = "001SMOKE0000000000"


def synthetic_event(now: datetime) -> dict[str, Any]:
    opp_id = "006SMOKE" + secrets.token_hex(5).upper()  # 18 characters, like a real ID
    stamp = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {
        "schema": "relay.opportunity.v1",
        "event_key": f"{opp_id}:{stamp}",
        "version": int(now.timestamp()) * 1000,
        "opportunity": {
            "id": opp_id,
            "name": "Smoke test deal",
            "stage_name": "Closed Won",
            "amount": 1.0,
            "close_date": now.strftime("%Y-%m-%d"),
            "account_id": CUSTOMER_ID,
            "account_name": "Smoke Test Ltd",
            "system_modstamp": stamp,
        },
        "published_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--stack", default="relay-dev")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()
    if not args.local:
        refuse_emulator_env()

    sess = session(args.local, args.profile)
    if args.local:
        sqs = client(sess, "sqs", True)
        queue_url = sqs.get_queue_url(QueueName="relay-events")["QueueUrl"]
        tables = {"orders": "orders", "invoices": "invoices", "customers": "customers"}
    else:
        out = stack_outputs(sess, args.stack)
        queue_url = out["QueueUrl"]
        tables = {
            "orders": out["OrdersTableName"],
            "invoices": out["InvoicesTableName"],
            "customers": out["CustomersTableName"],
        }
    sqs = client(sess, "sqs", args.local)
    ddb = client(sess, "dynamodb", args.local)

    event = synthetic_event(datetime.now(UTC))
    opp_id = event["opportunity"]["id"]
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event),
        MessageAttributes={"event_key": {"DataType": "String", "StringValue": event["event_key"]}},
    )
    print(f"sent {event['event_key']}")

    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            order = ddb.get_item(
                TableName=tables["orders"], Key={"order_id": {"S": opp_id}}, ConsistentRead=True
            ).get("Item")
            invoice = ddb.get_item(
                TableName=tables["invoices"],
                Key={"invoice_id": {"S": f"INV-{opp_id}"}},
                ConsistentRead=True,
            ).get("Item")
            if order and invoice and order["version"]["N"] == str(event["version"]):
                print(f"PASS: order and invoice written for {opp_id}")
                return 0
            time.sleep(2)
        print(f"FAIL: no order for {opp_id} within {args.timeout:.0f}s", file=sys.stderr)
        return 1
    finally:
        ddb.delete_item(TableName=tables["orders"], Key={"order_id": {"S": opp_id}})
        ddb.delete_item(TableName=tables["invoices"], Key={"invoice_id": {"S": f"INV-{opp_id}"}})
        ddb.delete_item(TableName=tables["customers"], Key={"customer_id": {"S": CUSTOMER_ID}})


if __name__ == "__main__":
    sys.exit(main())
