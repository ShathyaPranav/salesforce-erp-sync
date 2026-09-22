"""Phase 1.4 unit tests: classification, backoff and the handler's three paths.

AWS clients are replaced with small fakes, so these run in milliseconds. The
same behaviour is proven end to end through SQS in
tests/integration/test_failures.py.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import ClientError

from erp.writer import ErpRejected, ErpUnavailable
from tests.events import make_event
from worker import app
from worker.backoff import MAX_VISIBILITY_TIMEOUT, next_visibility_timeout
from worker.chaos import Chaos, WorkerCrash
from worker.classify import Kind, classify
from worker.mapping import PermanentError

V1 = datetime(2026, 9, 15, 9, 20, tzinfo=UTC)
ARN = "arn:aws:sqs:us-east-1:000000000000:relay-events"

# ---- classification ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "kind", "reason"),
    [
        (PermanentError("missing_amount"), Kind.PERMANENT, "missing_amount"),
        (ErpRejected("item too large"), Kind.PERMANENT, "erp_rejected"),
        (ErpUnavailable("ThrottlingException"), Kind.TRANSIENT, "erp_unavailable"),
        (KeyError("surprise"), Kind.TRANSIENT, "unexpected:KeyError"),
        (TimeoutError(), Kind.TRANSIENT, "unexpected:TimeoutError"),
    ],
)
def test_classify(exc, kind, reason):
    decision = classify(exc)
    assert (decision.kind, decision.reason) == (kind, reason)


# ---- backoff -----------------------------------------------------------------------


def test_backoff_doubles_per_attempt_plus_bounded_jitter():
    rng = random.Random(7)
    for attempt, low in [(1, 60), (2, 120), (3, 240), (4, 480)]:
        delay = next_visibility_timeout(attempt, base=60, cap=900, rng=rng)
        assert low <= delay <= low + 60, (attempt, delay)


def test_backoff_is_capped_and_within_sqs_limits():
    assert next_visibility_timeout(20, base=60, cap=900, rng=random.Random(1)) <= 960
    assert next_visibility_timeout(99, base=10**6, cap=10**7) == MAX_VISIBILITY_TIMEOUT
    assert next_visibility_timeout(0, base=1, cap=900) >= 1


# ---- the handler, with fake AWS clients ----------------------------------------------


class FakeSSM:
    def __init__(self, **chaos: str) -> None:
        self.values = {f"/relay/chaos/{k}": v for k, v in chaos.items()}

    def get_parameters(self, Names: list[str]) -> dict[str, Any]:
        return {
            "Parameters": [{"Name": n, "Value": self.values[n]} for n in Names if n in self.values]
        }


class FakeSQS:
    def __init__(self, fail_dlq_send: bool = False) -> None:
        self.dead_lettered: list[dict[str, Any]] = []
        self.visibility: dict[str, int] = {}
        self.fail_dlq_send = fail_dlq_send

    def get_queue_url(self, QueueName: str, QueueOwnerAWSAccountId: str) -> dict[str, str]:
        return {"QueueUrl": f"https://sqs/{QueueOwnerAWSAccountId}/{QueueName}"}

    def send_message(self, **kwargs: Any) -> dict[str, str]:
        if self.fail_dlq_send:
            raise ClientError({"Error": {"Code": "InternalError", "Message": "no"}}, "SendMessage")
        self.dead_lettered.append(kwargs)
        return {"MessageId": "dlq-1"}

    def change_message_visibility(
        self, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int
    ) -> None:
        self.visibility[ReceiptHandle] = VisibilityTimeout


class FakeDynamoDB:
    def __init__(self, error_code: str | None = None) -> None:
        self.writes = 0
        self.error_code = error_code

    def transact_write_items(self, TransactItems: list[Any]) -> None:
        if self.error_code:
            raise ClientError(
                {"Error": {"Code": self.error_code, "Message": "x"}}, "TransactWriteItems"
            )
        self.writes += 1


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    clients: dict[str, Any] = {"ssm": FakeSSM(), "sqs": FakeSQS(), "dynamodb": FakeDynamoDB()}
    monkeypatch.setattr(app, "client", lambda name: clients[name])
    monkeypatch.setattr(app, "_queue_urls", {})
    return clients


def record(msg_id: str, event: dict[str, Any], receive_count: int = 1) -> dict[str, Any]:
    return {
        "messageId": msg_id,
        "receiptHandle": f"rh-{msg_id}",
        "body": json.dumps(event),
        "attributes": {"ApproximateReceiveCount": str(receive_count)},
        "messageAttributes": {
            "event_key": {"stringValue": event.get("event_key"), "dataType": "String"}
        },
        "eventSourceARN": ARN,
    }


def good(msg_id: str = "m1", **kw: Any) -> dict[str, Any]:
    return record(msg_id, make_event(f"006{msg_id}", V1), **kw)


def no_amount(msg_id: str = "bad") -> dict[str, Any]:
    return record(msg_id, make_event(f"006{msg_id}", V1, amount=None))


class Ctx:
    def __init__(self, remaining_ms: int) -> None:
        self.remaining_ms = remaining_ms

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_ms


def test_success_reports_no_failures(aws):
    assert app.handler({"Records": [good()]}, Ctx(60_000)) == {"batchItemFailures": []}
    assert aws["dynamodb"].writes == 1


def test_bad_data_is_dead_lettered_with_a_reason_and_acked(aws):
    rec = no_amount()
    assert app.handler({"Records": [rec]}, Ctx(60_000)) == {"batchItemFailures": []}
    [sent] = aws["sqs"].dead_lettered
    assert sent["QueueUrl"].endswith("/relay-events-dlq")
    assert sent["MessageBody"] == rec["body"]  # the original message, untouched
    assert sent["MessageAttributes"]["reason"]["StringValue"] == "missing_amount"
    assert sent["MessageAttributes"]["source_message_id"]["StringValue"] == "bad"
    assert aws["dynamodb"].writes == 0
    assert aws["sqs"].visibility == {}  # no retry scheduled


def test_if_the_dlq_send_fails_the_message_is_retried_not_lost(aws):
    aws["sqs"].fail_dlq_send = True
    assert app.handler({"Records": [no_amount()]}, Ctx(60_000)) == {
        "batchItemFailures": [{"itemIdentifier": "bad"}]
    }


def test_transient_error_sets_backoff_and_reports_failure(aws):
    aws["dynamodb"].error_code = "ProvisionedThroughputExceededException"
    result = app.handler({"Records": [good(receive_count=3)]}, Ctx(60_000))
    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    delay = aws["sqs"].visibility["rh-m1"]
    assert 240 <= delay <= 300  # base 60 * 2^(3-1), plus up to 60 s of jitter


def test_one_bad_message_does_not_fail_the_rest_of_the_batch(aws):
    aws["ssm"] = FakeSSM(erp_fail_rate="0")
    batch = [good("a"), no_amount("b"), good("c")]
    assert app.handler({"Records": batch}, Ctx(60_000)) == {"batchItemFailures": []}
    assert aws["dynamodb"].writes == 2
    assert len(aws["sqs"].dead_lettered) == 1


def test_chaos_erp_fail_rate_one_makes_every_write_transient(aws):
    aws["ssm"] = FakeSSM(erp_fail_rate="1.0")
    result = app.handler({"Records": [good("a"), good("b")]}, Ctx(60_000))
    assert [f["itemIdentifier"] for f in result["batchItemFailures"]] == ["a", "b"]
    assert aws["dynamodb"].writes == 0


def test_chaos_crash_fails_the_whole_invocation_after_n_records(aws):
    aws["ssm"] = FakeSSM(worker_crash_after="1")
    with pytest.raises(WorkerCrash):
        app.handler({"Records": [good("a"), good("b")]}, Ctx(60_000))
    assert aws["dynamodb"].writes == 1  # the first write committed before the crash


def test_near_the_timeout_unstarted_records_are_handed_back(aws):
    result = app.handler({"Records": [good("a"), good("b")]}, Ctx(remaining_ms=1_000))
    assert [f["itemIdentifier"] for f in result["batchItemFailures"]] == ["a", "b"]
    assert aws["dynamodb"].writes == 0


def test_missing_chaos_parameters_mean_chaos_off():
    chaos = Chaos.load(FakeSSM(), "/relay")
    assert chaos == Chaos(erp_fail_rate=0.0, crash_after=0)
    chaos.maybe_fail_erp()
    chaos.maybe_crash(processed=100)
