"""A small, read-only Salesforce REST client for the reconciler (stdlib only).

Same flow as the Go poller's client: client credentials token from the My
Domain URL, then /query following nextRecordsUrl. The Go and Python clients
are both checked against the same fake Salesforce in the test suite.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any


class SalesforceError(Exception):
    pass


@dataclass
class Salesforce:
    login_url: str
    api_version: str
    client_id: str
    client_secret: str
    timeout: float = 30.0
    _token: dict[str, str] | None = field(default=None, repr=False)

    def query_all(self, soql: str) -> list[dict[str, Any]]:
        path = f"/services/data/{self.api_version}/query?" + urllib.parse.urlencode({"q": soql})
        records: list[dict[str, Any]] = []
        while True:
            page = self._get(path)
            records.extend(page.get("records", []))
            if page.get("done", True) or not page.get("nextRecordsUrl"):
                return records
            path = page["nextRecordsUrl"]

    def _get(self, path: str) -> dict[str, Any]:
        for attempt in (0, 1):
            token = self._access_token(refresh=attempt > 0)
            request = urllib.request.Request(  # noqa: S310 - URL from our own config
                token["instance_url"].rstrip("/") + path,
                headers={"Authorization": f"Bearer {token['access_token']}"},
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as resp:  # noqa: S310
                    body: dict[str, Any] = json.loads(resp.read())
                    return body
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    continue  # expired session: new token, one retry
                raise SalesforceError(
                    f"query failed: HTTP {exc.code} {exc.read()[:300]!r}"
                ) from exc
        raise SalesforceError("unreachable")

    def _access_token(self, refresh: bool) -> dict[str, str]:
        if self._token is not None and not refresh:
            return self._token
        form = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        ).encode()
        request = urllib.request.Request(  # noqa: S310 - URL from our own config
            self.login_url.rstrip("/") + "/services/oauth2/token",
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as resp:  # noqa: S310
                self._token = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            # The OAuth error body carries no secrets.
            raise SalesforceError(
                f"token request failed: HTTP {exc.code} {exc.read()[:300]!r}"
            ) from exc
        return self._token
