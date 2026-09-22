"""Run one SOQL query against Salesforce from the command line.

Phase 1.1 is done when a real Salesforce query works from here.

    python scripts/sf_query.py                 # your real org, credentials from .env
    python scripts/sf_query.py --fake          # the local fake on http://127.0.0.1:8080
    python scripts/sf_query.py --soql "SELECT Id, Name FROM Opportunity LIMIT 5"
    python scripts/sf_query.py --record tests/contract/fixtures/real_query_response.json

It never prints the client secret or the access token.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

# The same query the ingest poller runs, from the beginning of time.
DEFAULT_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp "
    "FROM Opportunity WHERE StageName = 'Closed Won' AND SystemModstamp >= 1970-01-01T00:00:00Z "
    "ORDER BY SystemModstamp ASC, Id ASC"
)
FAKE = {
    "SF_LOGIN_URL": "http://127.0.0.1:8080",
    "SF_CLIENT_ID": "fake-client-id",
    "SF_CLIENT_SECRET": "fake-client-secret",
    "SF_API_VERSION": "v66.0",
}


def setting(name: str, use_fake: bool) -> str:
    value = FAKE[name] if use_fake else os.environ.get(name, "")
    if not value or value.startswith("<"):
        sys.exit(f"{name} is not set. Fill it in .env (see .env.example).")
    return value


def get_token(login_url: str, client_id: str, client_secret: str) -> dict[str, Any]:
    resp = requests.post(
        f"{login_url.rstrip('/')}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        # Salesforce's OAuth error body is {"error": ..., "error_description": ...}: no secrets.
        sys.exit(f"Token request failed: HTTP {resp.status_code} {resp.text}")
    token: dict[str, Any] = resp.json()
    return token


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--fake", action="store_true", help="use the local fake Salesforce")
    parser.add_argument("--soql", default=DEFAULT_SOQL)
    parser.add_argument("--record", type=Path, help="save the raw first page of results here")
    args = parser.parse_args()

    load_dotenv()
    login_url = setting("SF_LOGIN_URL", args.fake)
    api_version = setting("SF_API_VERSION", args.fake)
    token = get_token(
        login_url, setting("SF_CLIENT_ID", args.fake), setting("SF_CLIENT_SECRET", args.fake)
    )
    instance_url = token["instance_url"]
    print(f"Authenticated. instance_url={instance_url} (token not shown)", file=sys.stderr)

    resp = requests.get(
        f"{instance_url}/services/data/{api_version}/query",
        params={"q": args.soql},
        headers={"Authorization": f"Bearer {token['access_token']}"},
        timeout=30,
    )
    usage = resp.headers.get("Sforce-Limit-Info")
    if usage:
        print(f"API usage today: {usage}", file=sys.stderr)
    if resp.status_code != 200:
        sys.exit(f"Query failed: HTTP {resp.status_code} {resp.text}")

    body = resp.json()
    if args.record:
        args.record.parent.mkdir(parents=True, exist_ok=True)
        args.record.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
        print(f"Recorded the response to {args.record}", file=sys.stderr)

    print(f"{body['totalSize']} record(s), done={body['done']}", file=sys.stderr)
    for rec in body["records"]:
        account = (rec.get("Account") or {}).get("Name")
        print(
            f"{rec.get('Id')}  {rec.get('SystemModstamp')}  "
            f"amount={rec.get('Amount')!s:>10}  account={account!s:<20} {rec.get('Name')}"
        )


if __name__ == "__main__":
    main()
