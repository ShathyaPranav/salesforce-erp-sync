"""Phase 1.1: the local stack is up and configured like AWS will be.

The last two tests check that the pinned LocalStack really implements the two
AWS behaviours the failure-injection suite depends on. If a LocalStack upgrade
ever breaks them, these fail first, with an obvious name.
"""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from erp.schema import ALL_TABLES

REPO = Path(__file__).resolve().parents[2]


def test_three_erp_tables_exist_with_salesforce_keys(ddb):
    for spec in ALL_TABLES:
        table = ddb.describe_table(TableName=spec.name)["Table"]
        assert table["KeySchema"] == [{"AttributeName": spec.partition_key, "KeyType": "HASH"}]


def test_main_queue_dead_letters_after_five_receives(sqs):
    url = sqs.get_queue_url(QueueName="relay-events")["QueueUrl"]
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["RedrivePolicy"])["Attributes"]
    policy = json.loads(attrs["RedrivePolicy"])
    assert int(policy["maxReceiveCount"]) == 5
    assert policy["deadLetterTargetArn"].endswith(":relay-events-dlq")


def test_parameters_exist_and_secret_is_secure_string(ssm):
    names = [
        "/relay/salesforce/login_url",
        "/relay/salesforce/client_id",
        "/relay/salesforce/client_secret",
        "/relay/ingest/watermark",
        "/relay/chaos/erp_fail_rate",
    ]
    params = {p["Name"]: p for p in ssm.get_parameters(Names=names)["Parameters"]}
    assert set(params) == set(names)
    secret = ssm.describe_parameters(
        ParameterFilters=[{"Key": "Name", "Values": ["/relay/salesforce/client_secret"]}]
    )["Parameters"][0]
    assert secret["Type"] == "SecureString"


def test_sf_query_cli_works_against_the_fake(fake_sf):
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(REPO / "scripts" / "sf_query.py"), "--fake"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "6 record(s)" in result.stderr
    assert "006FAKE00000000001" in result.stdout


# ---- emulator fidelity: behaviours the failure tests rely on ---------------------------


def test_emulator_moves_message_to_dlq_after_max_receives(sqs):
    name = f"probe-{uuid.uuid4().hex[:8]}"
    dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    url = sqs.create_queue(
        QueueName=name,
        Attributes={
            "VisibilityTimeout": "0",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}),
        },
    )["QueueUrl"]
    try:
        sqs.send_message(QueueUrl=url, MessageBody="poison")
        receives = 0
        for _ in range(10):
            got = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1).get("Messages", [])
            receives += len(got)
        moved = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=2).get("Messages", [])
        assert receives == 2
        assert [m["Body"] for m in moved] == ["poison"]
    finally:
        sqs.delete_queue(QueueUrl=url)
        sqs.delete_queue(QueueUrl=dlq_url)


def test_emulator_returns_old_item_when_transaction_condition_fails(ddb):
    order_id = f"probe-{uuid.uuid4().hex[:8]}"
    ddb.put_item(TableName="orders", Item={"order_id": {"S": order_id}, "version": {"N": "5"}})
    try:
        with pytest.raises(ClientError) as exc:
            ddb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": "orders",
                            "Item": {"order_id": {"S": order_id}, "version": {"N": "3"}},
                            "ConditionExpression": (
                                "attribute_not_exists(order_id) OR version < :v"
                            ),
                            "ExpressionAttributeValues": {":v": {"N": "3"}},
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                        }
                    }
                ]
            )
        err = exc.value.response
        assert err["Error"]["Code"] == "TransactionCanceledException"
        reason = err["CancellationReasons"][0]
        assert reason["Code"] == "ConditionalCheckFailed"
        assert dict(reason["Item"])["version"] == {"N": "5"}
    finally:
        ddb.delete_item(TableName="orders", Key={"order_id": {"S": order_id}})


def test_smoke_script_passes_against_the_local_stack(clean_queue):
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(REPO / "scripts" / "smoke.py"), "--local", "--timeout", "30"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout
