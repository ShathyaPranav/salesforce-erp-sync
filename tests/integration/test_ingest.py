"""Phase 1.2: the ingest Lambda image, end to end on the local stack.

Each test invokes the real container (through the Runtime Interface Emulator
on :9001) the way EventBridge Scheduler would, against the fake Salesforce and
LocalStack's SQS and SSM, then reads what landed on the queue.

Where a test needs exact window edges it freezes the fake Salesforce's clock:
the poller takes "now" from Salesforce's Date header, never from its own clock.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import requests

from tests.integration.conftest import FAKE_SF

INGEST = "http://127.0.0.1:9001/2015-03-31/functions/function/invocations"
WATERMARK = "/relay/ingest/watermark"
LAG = timedelta(seconds=120)  # INGEST_LAG_SECONDS in docker-compose.yml
T = datetime(2026, 9, 22, 10, 0, 0, tzinfo=UTC)

SEEDED_CLOSED_WON = {
    "006FAKE00000000001",
    "006FAKE00000000002",
    "006FAKE00000000003",
    "006FAKE00000000004",
    "006FAKE00000000005",
    "006FAKE00000000008",
}


def sf_stamp(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def key(opp_id: str, t: datetime) -> str:
    return f"{opp_id}:{t.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"


def invoke() -> dict[str, Any]:
    resp = requests.post(INGEST, data="{}", timeout=60)
    resp.raise_for_status()
    result: dict[str, Any] = resp.json()
    return result


def freeze(t: datetime | None) -> None:
    requests.post(
        f"{FAKE_SF}/__admin/clock",
        json={"now": None if t is None else t.isoformat()},
        timeout=5,
    ).raise_for_status()


def add_deal(opp_id: str, stamp: datetime, amount: float | None = 1000.0) -> None:
    requests.put(
        f"{FAKE_SF}/__admin/opportunities",
        json=[
            {
                "Id": opp_id,
                "Name": f"Deal {opp_id}",
                "AccountId": "001FAKE00000000001",
                "Amount": amount,
                "CloseDate": "2026-09-20",
                "StageName": "Closed Won",
                "SystemModstamp": sf_stamp(stamp),
            }
        ],
        timeout=5,
    ).raise_for_status()


@pytest.fixture
def queue(sqs: Any, ssm: Any, fake_sf: str) -> Any:
    """An empty queue, a watermark at the epoch and Salesforce on real time."""
    url = sqs.get_queue_url(QueueName="relay-events")["QueueUrl"]
    drain(sqs, url)
    ssm.put_parameter(Name=WATERMARK, Value="1970-01-01T00:00:00Z", Type="String", Overwrite=True)
    yield url
    freeze(None)
    drain(sqs, url)


def drain(sqs: Any, url: str) -> list[dict[str, Any]]:
    """Receive and delete everything on the queue, oldest first."""
    messages: list[dict[str, Any]] = []
    while True:
        got = sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, MessageAttributeNames=["All"]
        ).get("Messages", [])
        if not got:
            return messages
        for m in got:
            messages.append(m)
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])


def keys(messages: list[dict[str, Any]]) -> list[str]:
    return sorted(json.loads(m["Body"])["event_key"] for m in messages)


def watermark(ssm: Any) -> str:
    value: str = ssm.get_parameter(Name=WATERMARK)["Parameter"]["Value"]
    return value


# ---- happy path -------------------------------------------------------------------


def test_first_poll_publishes_each_closed_won_deal_once(queue, sqs, ssm):
    freeze(T)
    result = invoke()
    assert result["published"] == 6

    messages = drain(sqs, queue)
    bodies = {json.loads(m["Body"])["opportunity"]["id"]: json.loads(m["Body"]) for m in messages}
    assert set(bodies) == SEEDED_CLOSED_WON  # Negotiation and Closed Lost never sync

    acme = bodies["006FAKE00000000001"]
    assert acme["schema"] == "relay.opportunity.v1"
    assert acme["event_key"] == "006FAKE00000000001:2026-09-15T09:00:00.000Z"
    assert acme["version"] == int(datetime(2026, 9, 15, 9, tzinfo=UTC).timestamp() * 1000)
    assert acme["opportunity"]["account_name"] == "Acme Robotics"
    assert acme["opportunity"]["amount"] == 125000.0
    # Bad data is published as-is; deciding it's a permanent error is the worker's job.
    assert bodies["006FAKE00000000003"]["opportunity"]["amount"] is None
    assert bodies["006FAKE00000000008"]["opportunity"]["account_id"] is None

    attr = messages[0]["MessageAttributes"]["event_key"]["StringValue"]
    assert attr == json.loads(messages[0]["Body"])["event_key"]
    assert watermark(ssm) == "2026-09-22T09:58:00Z"  # Salesforce's now minus the lag


def test_idle_org_publishes_nothing_on_later_polls(queue, sqs):
    freeze(T)
    assert invoke()["published"] == 6
    drain(sqs, queue)
    for minutes in (2, 4, 6):
        freeze(T + timedelta(minutes=minutes))
        assert invoke()["published"] == 0
    assert drain(sqs, queue) == []


# ---- the window edges ---------------------------------------------------------


def test_equal_timestamps_at_the_window_edge_are_published_once_each(queue, sqs, ssm):
    ssm.put_parameter(Name=WATERMARK, Value="2026-09-22T09:00:00Z", Type="String", Overwrite=True)
    edge = T - LAG  # the first run's upper bound, excluded by "<"
    add_deal("006TIE000000000A01", edge - timedelta(seconds=1))
    add_deal("006TIE000000000B01", edge)
    add_deal("006TIE000000000C01", edge)

    freeze(T)
    assert invoke()["published"] == 1
    freeze(T + timedelta(minutes=2))
    assert invoke()["published"] == 2
    freeze(T + timedelta(minutes=4))
    assert invoke()["published"] == 0

    assert keys(drain(sqs, queue)) == [
        key("006TIE000000000A01", edge - timedelta(seconds=1)),
        key("006TIE000000000B01", edge),
        key("006TIE000000000C01", edge),
    ]


def test_a_deal_inside_the_lag_waits_for_a_later_poll(queue, sqs, ssm):
    ssm.put_parameter(Name=WATERMARK, Value="2026-09-22T09:00:00Z", Type="String", Overwrite=True)
    add_deal("006LAG000000000001", T - timedelta(seconds=30))
    freeze(T)
    assert invoke()["published"] == 0
    freeze(T + timedelta(minutes=2))
    assert invoke()["published"] == 1
    assert keys(drain(sqs, queue)) == [key("006LAG000000000001", T - timedelta(seconds=30))]


# ---- Salesforce failures ------------------------------------------------------------


def test_salesforce_outage_fails_the_run_and_keeps_the_watermark(queue, sqs, ssm):
    requests.post(
        f"{FAKE_SF}/__admin/faults", json={"target": "query", "status": 503, "count": 1}, timeout=5
    ).raise_for_status()
    freeze(T)
    result = invoke()
    assert "errorMessage" in result and "503" in result["errorMessage"]
    assert watermark(ssm) == "1970-01-01T00:00:00Z"
    assert drain(sqs, queue) == []

    assert invoke()["published"] == 6  # the next scheduled run is the retry


def test_expired_session_is_refreshed_transparently(queue, sqs):
    freeze(T)
    invoke()  # warms the Lambda and caches a token
    drain(sqs, queue)
    requests.post(f"{FAKE_SF}/__admin/revoke-tokens", timeout=5).raise_for_status()
    add_deal("006NEW000000000001", T)
    freeze(T + timedelta(minutes=3))
    assert invoke()["published"] == 1


def test_rejected_credentials_surface_as_an_auth_error(queue, ssm):
    requests.post(f"{FAKE_SF}/__admin/revoke-tokens", timeout=5).raise_for_status()
    requests.post(
        f"{FAKE_SF}/__admin/faults",
        json={
            "target": "token",
            "status": 400,
            "count": 5,
            "body": {"error": "invalid_client", "error_description": "invalid client credentials"},
        },
        timeout=5,
    ).raise_for_status()
    result = invoke()
    assert "salesforce token request failed" in result.get("errorMessage", "")
    assert "fake-client-secret" not in json.dumps(result)
    assert watermark(ssm) == "1970-01-01T00:00:00Z"
