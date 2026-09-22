"""Create Relay's queues, tables and parameters on the local AWS emulator.

Runs as the one-shot `bootstrap` service in docker compose, and can be re-run
from your machine at any time (every step is idempotent):

    python scripts/local_bootstrap.py

This is the local mirror of template.yaml. Names come from the same env vars
the Lambdas read, so the code under test is configured exactly as on AWS.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from erp.schema import ALL_TABLES

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
REGION = os.environ.get("AWS_REGION", "us-east-1")
QUEUE = os.environ.get("QUEUE_NAME", "relay-events")
DLQ = os.environ.get("DLQ_NAME", "relay-events-dlq")
MAX_RECEIVE_COUNT = int(os.environ.get("MAX_RECEIVE_COUNT", "5"))
VISIBILITY_TIMEOUT = int(os.environ.get("QUEUE_VISIBILITY_TIMEOUT", "30"))
PREFIX = os.environ.get("RELAY_PARAM_PREFIX", "/relay")

# The local stack talks to the fake Salesforce. Nothing here is a real secret.
PARAMETERS: dict[str, tuple[str, str]] = {
    f"{PREFIX}/salesforce/login_url": (
        os.environ.get("FAKE_SF_URL", "http://fake-salesforce:8080"),
        "String",
    ),
    f"{PREFIX}/salesforce/api_version": ("v66.0", "String"),
    f"{PREFIX}/salesforce/client_id": ("fake-client-id", "String"),
    f"{PREFIX}/salesforce/client_secret": ("fake-client-secret", "SecureString"),
    f"{PREFIX}/ingest/watermark": ("1970-01-01T00:00:00.000Z", "String"),
    f"{PREFIX}/chaos/erp_fail_rate": ("0", "String"),
}


def session() -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=REGION,
    )


def wait_for_endpoint(timeout: float = 90.0) -> None:
    sqs = session().client("sqs", endpoint_url=ENDPOINT)
    deadline = time.monotonic() + timeout
    while True:
        try:
            sqs.list_queues()
            return
        except Exception as exc:  # the emulator is still starting
            if time.monotonic() > deadline:
                raise SystemExit(f"AWS emulator at {ENDPOINT} not reachable: {exc}") from exc
            time.sleep(1)


def create_tables() -> None:
    ddb = session().client("dynamodb", endpoint_url=ENDPOINT)
    for spec in ALL_TABLES:
        try:
            ddb.create_table(
                TableName=spec.name,
                AttributeDefinitions=[{"AttributeName": spec.partition_key, "AttributeType": "S"}],
                KeySchema=[{"AttributeName": spec.partition_key, "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            print(f"table   {spec.name}: created")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceInUseException":
                raise
            print(f"table   {spec.name}: exists")
        ddb.get_waiter("table_exists").wait(TableName=spec.name)


def create_queues() -> None:
    sqs = session().client("sqs", endpoint_url=ENDPOINT)
    dlq_url = sqs.create_queue(
        QueueName=DLQ, Attributes={"MessageRetentionPeriod": str(14 * 24 * 3600)}
    )["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    queue_url = sqs.create_queue(QueueName=QUEUE)["QueueUrl"]
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "VisibilityTimeout": str(VISIBILITY_TIMEOUT),
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(MAX_RECEIVE_COUNT)}
            ),
        },
    )
    print(f"queue   {QUEUE} -> DLQ {DLQ} after {MAX_RECEIVE_COUNT} receives")


def put_parameters(overwrite_watermark: bool) -> None:
    ssm = session().client("ssm", endpoint_url=ENDPOINT)
    for name, (value, ptype) in PARAMETERS.items():
        is_state = name.endswith("/watermark") or name.endswith("/erp_fail_rate")
        if is_state and not overwrite_watermark:
            try:
                ssm.get_parameter(Name=name)
                print(f"param   {name}: kept")
                continue
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ParameterNotFound":
                    raise
        ssm.put_parameter(Name=name, Value=value, Type=ptype, Overwrite=True)  # type: ignore[arg-type]
        print(f"param   {name}: set")


def main() -> None:
    reset = "--reset-state" in sys.argv
    wait_for_endpoint()
    create_tables()
    create_queues()
    put_parameters(overwrite_watermark=reset)
    print("bootstrap complete")


if __name__ == "__main__":
    main()
