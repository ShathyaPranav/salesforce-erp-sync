"""Shared fixtures for tests that run against the docker compose stack.

Start the stack first:  docker compose up -d
If it isn't running these tests are skipped, unless RELAY_REQUIRE_STACK=1
(set in CI), in which case they fail loudly instead.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
import requests

ENDPOINT = os.environ.get("RELAY_TEST_AWS_ENDPOINT", "http://127.0.0.1:4566")
FAKE_SF = os.environ.get("RELAY_TEST_FAKE_SF", "http://127.0.0.1:8080")
REGION = "us-east-1"


def _stack_up() -> str | None:
    try:
        requests.get(f"{ENDPOINT}/_localstack/health", timeout=2).raise_for_status()
        requests.get(f"{FAKE_SF}/__admin/health", timeout=2).raise_for_status()
    except requests.RequestException as exc:
        return str(exc)
    return None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    here = os.path.dirname(__file__)
    ours = [i for i in items if str(i.fspath).startswith(here)]
    for item in ours:
        item.add_marker(pytest.mark.integration)
    if not ours:
        return
    problem = _stack_up()
    if problem is None:
        return
    if os.environ.get("RELAY_REQUIRE_STACK") == "1":
        raise pytest.UsageError(f"docker compose stack is not reachable: {problem}")
    skip = pytest.mark.skip(reason="docker compose stack not running (docker compose up -d)")
    for item in ours:
        item.add_marker(skip)


def client(service: str) -> Any:
    return boto3.client(  # type: ignore[call-overload]
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def sqs() -> Any:
    return client("sqs")


@pytest.fixture
def ddb() -> Any:
    return client("dynamodb")


@pytest.fixture
def ssm() -> Any:
    return client("ssm")


@pytest.fixture
def fake_sf() -> Iterator[str]:
    requests.post(f"{FAKE_SF}/__admin/reset", json={"seed": True}, timeout=5).raise_for_status()
    yield FAKE_SF
    requests.post(f"{FAKE_SF}/__admin/reset", json={"seed": True}, timeout=5)


# ---- the local event source mapping (tests/harness/esm_pump.py) ----------------

PUMP = os.environ.get("RELAY_TEST_PUMP", "http://127.0.0.1:9100")


def pump(action: str) -> None:
    requests.post(f"{PUMP}/{action}", timeout=30).raise_for_status()


@pytest.fixture
def pump_paused() -> Iterator[None]:
    """The queue to ourselves: the pump stops feeding the worker until teardown."""
    pump("pause")
    yield
    pump("resume")


@pytest.fixture
def pump_running() -> None:
    pump("resume")


@pytest.fixture
def clean_queue(sqs: Any) -> str:
    """Main queue and DLQ emptied (with the pump paused, so nothing is in
    flight), then the pump feeding the worker again."""
    urls: list[str] = [
        sqs.get_queue_url(QueueName=n)["QueueUrl"] for n in ("relay-events", "relay-events-dlq")
    ]
    pump("pause")
    try:
        for url in urls:
            while True:
                got = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
                if not got.get("Messages"):
                    break
                for m in got["Messages"]:
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    finally:
        pump("resume")
    return urls[0]


@pytest.fixture
def opp_id(ddb: Any) -> Iterator[str]:
    """A fresh 18-character Opportunity ID; its ERP items are removed afterwards."""
    oid = f"006T{uuid.uuid4().hex[:14].upper()}"
    yield oid
    ddb.delete_item(TableName="orders", Key={"order_id": {"S": oid}})
    ddb.delete_item(TableName="invoices", Key={"invoice_id": {"S": f"INV-{oid}"}})
