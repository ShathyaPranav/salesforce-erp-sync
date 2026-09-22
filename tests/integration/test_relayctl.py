"""Phase 2.3: relayctl against the local stack.

relayctl runs through the compose `go` tool container (so no local Go install
is needed), on the compose network, pointed at LocalStack and the reconciler.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.integration.stack import queue_url

REPO = Path(__file__).resolve().parents[2]
RELAYCTL = [
    "docker", "compose", "run", "--rm", "-T", "go",
    "go", "run", "./relayctl", "--local",
    "--endpoint", "http://localstack:4566",
    "--reconciler-url", "http://reconciler:8080/2015-03-31/functions/function/invocations",
]  # fmt: skip


def relayctl(*args: str) -> str:
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [*RELAYCTL, *args], cwd=REPO, capture_output=True, text=True, timeout=300, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def dead_letter(sqs: Any, event_key: str, reason: str | None = None) -> None:
    attrs = {"event_key": {"DataType": "String", "StringValue": event_key}}
    if reason:
        attrs["reason"] = {"DataType": "String", "StringValue": reason}
    sqs.send_message(
        QueueUrl=queue_url(sqs, "relay-events-dlq"),
        MessageBody=json.dumps({"event_key": event_key}),
        MessageAttributes=attrs,
    )


@pytest.fixture
def chaos_off(ssm: Any) -> Any:
    yield
    relayctl("chaos", "--off")


def test_chaos_switches_are_written_to_ssm(ssm, chaos_off):
    out = relayctl("chaos", "--erp-fail-rate", "0.3")
    assert "erp fail rate 0.30" in out
    assert ssm.get_parameter(Name="/relay/chaos/erp_fail_rate")["Parameter"]["Value"] == "0.3"
    assert "(off)" in relayctl("chaos", "--off")


def test_stats_reports_queues_tables_and_watermark(clean_queue):
    out = relayctl("stats")
    for label in ("queue", "dead-letter queue", "orders", "invoices", "customers", "watermark"):
        assert label in out


def test_dlq_list_redrive_and_ack(sqs, clean_queue, pump_paused):
    dead_letter(sqs, "006RETRY0000000001:2026-09-15T09:00:00.000Z")  # moved by SQS
    dead_letter(sqs, "006BADDATA00000001:2026-09-15T09:00:00.000Z", reason="missing_amount")

    listing = relayctl("dlq", "list")
    assert "transient" in listing and "retries exhausted" in listing
    assert "permanent" in listing and "missing_amount" in listing

    out = relayctl("dlq", "redrive")
    assert "moved 1 message(s)" in out and "left 1 permanent" in out
    main = sqs.receive_message(
        QueueUrl=queue_url(sqs), WaitTimeSeconds=2, MessageAttributeNames=["All"]
    )
    [moved] = main["Messages"]
    assert moved["MessageAttributes"]["event_key"]["StringValue"].startswith("006RETRY")
    sqs.delete_message(QueueUrl=queue_url(sqs), ReceiptHandle=moved["ReceiptHandle"])

    assert "acknowledged 1" in relayctl("dlq", "ack", "006BADDATA00000001:2026-09-15T09:00:00.000Z")
    assert "empty" in relayctl("dlq", "list")


def test_reconcile_dry_run_prints_the_drift_report(fake_sf):
    out = relayctl("reconcile", "--dry-run")
    assert "drift found:" in out
    assert "invalid_in_source" in out  # the seeded deal with no Amount
