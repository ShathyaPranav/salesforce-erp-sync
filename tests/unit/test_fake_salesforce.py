"""The fake Salesforce must behave like the real API for everything Relay relies on."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from typing import Any

import pytest
import requests

from fake_salesforce.server import App, Config, make_handler
from fake_salesforce.soql import SoqlError, parse

CLOSED_WON_SINCE = (
    "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp "
    "FROM Opportunity WHERE StageName = 'Closed Won' AND SystemModstamp {op} {ts} "
    "ORDER BY SystemModstamp ASC, Id ASC"
)


@pytest.fixture
def fake_sf() -> Iterator[tuple[str, App]]:
    app = App(config=Config(page_size=3))
    app.load_seed()
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", app
    server.shutdown()
    server.server_close()


def get_token(base: str) -> str:
    resp = requests.post(
        f"{base}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "fake-client-id",
            "client_secret": "fake-client-secret",
        },
        timeout=5,
    )
    assert resp.status_code == 200, resp.text
    token: str = resp.json()["access_token"]
    return token


def query(base: str, token: str, soql: str) -> requests.Response:
    return requests.get(
        f"{base}/services/data/v66.0/query",
        params={"q": soql},
        headers={"Authorization": f"Bearer {token}"},
        timeout=5,
    )


def query_all(base: str, token: str, soql: str) -> list[dict[str, object]]:
    resp = query(base, token, soql)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    records = list(body["records"])
    while not body["done"]:
        body = requests.get(
            base + body["nextRecordsUrl"],
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        ).json()
        records.extend(body["records"])
    return records


# ---- OAuth ---------------------------------------------------------------------


def test_client_credentials_returns_bearer_token_and_instance_url(fake_sf):
    base, _ = fake_sf
    resp = requests.post(
        f"{base}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "fake-client-id",
            "client_secret": "fake-client-secret",
        },
        timeout=5,
    )
    body = resp.json()
    assert resp.status_code == 200
    assert body["token_type"] == "Bearer"
    assert body["instance_url"] == base
    assert "refresh_token" not in body  # client credentials never returns one


def test_wrong_secret_is_rejected_as_invalid_client(fake_sf):
    base, _ = fake_sf
    resp = requests.post(
        f"{base}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "fake-client-id",
            "client_secret": "wrong",
        },
        timeout=5,
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "invalid_client"


def test_query_without_token_is_401_invalid_session(fake_sf):
    base, _ = fake_sf
    resp = query(base, "not-a-token", "SELECT Id FROM Opportunity")
    assert resp.status_code == 401
    assert resp.json()[0]["errorCode"] == "INVALID_SESSION_ID"


def test_revoked_token_gets_401(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    requests.post(f"{base}/__admin/revoke-tokens", timeout=5)
    assert query(base, token, "SELECT Id FROM Opportunity").status_code == 401


# ---- query semantics the poller depends on ----------------------------------------


def test_ge_includes_every_record_sharing_the_boundary_timestamp(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    soql = CLOSED_WON_SINCE.format(op=">=", ts="2026-09-15T09:00:00Z")
    ids = [r["Id"] for r in query_all(base, token, soql)]
    # 001 and 005 share SystemModstamp 09:00:00; both must be returned, ordered by Id.
    assert ids[:2] == ["006FAKE00000000001", "006FAKE00000000005"]


def test_strict_gt_skips_records_at_the_boundary(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    soql = CLOSED_WON_SINCE.format(op=">", ts="2026-09-15T09:00:00Z")
    ids = {r["Id"] for r in query_all(base, token, soql)}
    assert "006FAKE00000000001" not in ids
    assert "006FAKE00000000005" not in ids


def test_only_closed_won_deals_are_returned(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    soql = CLOSED_WON_SINCE.format(op=">=", ts="1970-01-01T00:00:00Z")
    records = query_all(base, token, soql)
    assert {r["StageName"] for r in records} == {"Closed Won"}
    assert "006FAKE00000000006" not in {r["Id"] for r in records}  # Negotiation/Review
    assert "006FAKE00000000007" not in {r["Id"] for r in records}  # Closed Lost


def test_results_are_ordered_by_modstamp_then_id(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    soql = CLOSED_WON_SINCE.format(op=">=", ts="1970-01-01T00:00:00Z")
    records = query_all(base, token, soql)
    keys = [(r["SystemModstamp"], r["Id"]) for r in records]
    assert keys == sorted(keys)


def test_quoted_datetime_is_rejected_like_real_salesforce(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    resp = query(
        base,
        token,
        "SELECT Id FROM Opportunity WHERE SystemModstamp >= '2026-09-15T09:00:00Z'",
    )
    assert resp.status_code == 400
    assert resp.json()[0]["errorCode"] == "INVALID_FIELD"


def test_pagination_via_next_records_url(fake_sf):
    base, _ = fake_sf  # page_size=3 in the fixture
    token = get_token(base)
    first = query(base, token, CLOSED_WON_SINCE.format(op=">=", ts="1970-01-01T00:00:00Z"))
    body = first.json()
    assert body["done"] is False
    assert body["totalSize"] == 6
    assert len(body["records"]) == 3
    assert body["nextRecordsUrl"].startswith("/services/data/v66.0/query/01g")
    all_records = query_all(
        base, token, CLOSED_WON_SINCE.format(op=">=", ts="1970-01-01T00:00:00Z")
    )
    assert len(all_records) == 6


# ---- response shape -----------------------------------------------------------------


def test_record_shape_matches_rest_api(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    records = {
        r["Id"]: r
        for r in query_all(base, token, CLOSED_WON_SINCE.format(op=">=", ts="1970-01-01T00:00:00Z"))
    }
    acme: dict[str, Any] = records["006FAKE00000000001"]
    assert acme["attributes"] == {
        "type": "Opportunity",
        "url": "/services/data/v66.0/sobjects/Opportunity/006FAKE00000000001",
    }
    assert acme["Account"]["Name"] == "Acme Robotics"
    assert acme["Account"]["attributes"]["type"] == "Account"
    assert acme["SystemModstamp"] == "2026-09-15T09:00:00.000+0000"
    assert acme["CloseDate"] == "2026-09-01"
    assert acme["Amount"] == 125000.0
    assert records["006FAKE00000000003"]["Amount"] is None  # the no-Amount edge case
    orphan = records["006FAKE00000000008"]
    assert orphan["AccountId"] is None
    assert orphan["Account"] is None  # relationship is null, not missing


def test_admin_edit_bumps_system_modstamp(fake_sf):
    base, _ = fake_sf
    resp = requests.patch(
        f"{base}/__admin/opportunities/006FAKE00000000004",
        json={"changes": {"Amount": 21000}, "SystemModstamp": "2026-09-16T10:00:00Z"},
        timeout=5,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["Amount"] == 21000.0
    assert body["SystemModstamp"] == "2026-09-16T10:00:00.000+0000"


def test_fault_injection_fails_the_next_n_calls_only(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    requests.post(
        f"{base}/__admin/faults", json={"target": "query", "status": 503, "count": 1}, timeout=5
    )
    assert query(base, token, "SELECT Id FROM Opportunity").status_code == 503
    assert query(base, token, "SELECT Id FROM Opportunity").status_code == 200


def test_requests_log_records_the_soql_sent(fake_sf):
    base, _ = fake_sf
    token = get_token(base)
    soql = CLOSED_WON_SINCE.format(op=">=", ts="2026-09-15T09:00:00Z")
    query(base, token, soql)
    log = requests.get(f"{base}/__admin/requests", timeout=5).json()
    assert log[-1]["soql"] == soql


# ---- parser strictness --------------------------------------------------------------------


def test_unknown_field_is_invalid_field():
    with pytest.raises(SoqlError) as exc:
        parse("SELECT Bogus__c FROM Opportunity")
    assert exc.value.error_code == "INVALID_FIELD"


def test_or_is_not_supported_so_tests_stay_honest():
    with pytest.raises(SoqlError):
        parse("SELECT Id FROM Opportunity WHERE IsWon = true OR Amount = null")


def test_datetime_literal_offsets_are_normalised_to_utc():
    q = parse("SELECT Id FROM Opportunity WHERE SystemModstamp >= 2026-09-15T14:30:00+05:30")
    value = q.where[0].value
    assert str(value) == "2026-09-15 09:00:00+00:00"
