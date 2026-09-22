"""Fake Salesforce: OAuth 2.0 client credentials + REST query API + an admin API.

Real endpoints it imitates (same paths, same JSON shapes, same error shapes):
  POST /services/oauth2/token             client credentials grant
  GET  /services/data/                    list API versions
  GET  /services/data/vNN.N/query?q=SOQL  run a query (paged by nextRecordsUrl)
  GET  /services/data/vNN.N/query/<loc>   fetch the next page

Admin endpoints (not in real Salesforce) let tests build awkward data and
inject faults:
  GET    /__admin/health | /__admin/state | /__admin/requests
  POST   /__admin/reset            {"seed": true}   clear data, optionally reload seed
  PUT    /__admin/accounts         [ {...}, ... ]
  PUT    /__admin/opportunities    [ {...}, ... ]   create/replace (may set SystemModstamp)
  PATCH  /__admin/opportunities/ID {"changes": {...}, "SystemModstamp": "..."}  an edit
  DELETE /__admin/opportunities/ID
  POST   /__admin/faults           {"target": "token"|"query", "status": 503, "count": 1}
  POST   /__admin/revoke-tokens    expire every issued access token
"""

from __future__ import annotations

import base64
import collections
import itertools
import json
import os
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote_plus, urlsplit

from fake_salesforce.soql import SoqlError, parse
from fake_salesforce.store import Record, Store

DEFAULT_SEED = Path(__file__).with_name("seed.json")
ORG_ID = "00DFAKE0000000001"
USER_ID = "005FAKE0000000001"

_VERSION_PATH = re.compile(r"^/services/data/v(?P<version>\d+\.\d)(?P<rest>/.*)?$")
_LOCATOR_PATH = re.compile(r"^/query/(?P<locator>01gFAKE\d+)-(?P<offset>\d+)$")


@dataclass
class Fault:
    status: int
    body: Any
    remaining: int


@dataclass
class Config:
    client_id: str = "fake-client-id"
    client_secret: str = "fake-client-secret"
    token_ttl_seconds: float = 7200.0
    page_size: int = 2000
    api_versions: tuple[str, ...] = ("62.0", "63.0", "64.0", "65.0", "66.0")
    daily_api_limit: int = 15000

    @classmethod
    def from_env(cls) -> Config:
        versions = os.environ.get("FAKE_SF_API_VERSIONS")
        return cls(
            client_id=os.environ.get("FAKE_SF_CLIENT_ID", cls.client_id),
            client_secret=os.environ.get("FAKE_SF_CLIENT_SECRET", cls.client_secret),
            token_ttl_seconds=float(
                os.environ.get("FAKE_SF_TOKEN_TTL_SECONDS", cls.token_ttl_seconds)
            ),
            page_size=int(os.environ.get("FAKE_SF_PAGE_SIZE", cls.page_size)),
            api_versions=tuple(versions.split(",")) if versions else cls.api_versions,
        )


@dataclass
class App:
    config: Config
    store: Store = field(default_factory=Store)
    seed_path: Path | None = DEFAULT_SEED
    tokens: dict[str, float] = field(default_factory=dict)
    cursors: dict[str, list[Record]] = field(default_factory=dict)
    faults: dict[str, list[Fault]] = field(default_factory=dict)
    requests: collections.deque[dict[str, Any]] = field(
        default_factory=lambda: collections.deque(maxlen=500)
    )
    api_calls: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    locator_ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    def load_seed(self) -> None:
        self.store.reset()
        if self.seed_path is not None and self.seed_path.exists():
            self.store.load(json.loads(self.seed_path.read_text(encoding="utf-8")))

    def take_fault(self, target: str) -> Fault | None:
        with self.lock:
            queue = self.faults.get(target, [])
            while queue and queue[0].remaining <= 0:
                queue.pop(0)
            if not queue:
                return None
            queue[0].remaining -= 1
            return queue[0]

    def token_valid(self, token: str) -> bool:
        with self.lock:
            issued = self.tokens.get(token)
        return issued is not None and time.monotonic() - issued < self.config.token_ttl_seconds


def make_handler(app: App) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "FakeSalesforce/1.0"

        # ---- plumbing ---------------------------------------------------------

        def log_message(self, format: str, *args: Any) -> None:
            pass  # we log one JSON line per request in _send instead

        def _body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def _json_body(self) -> Any:
            raw = self._body()
            return json.loads(raw) if raw else {}

        def _send(self, status: int, payload: Any, extra: dict[str, Any] | None = None) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json;charset=UTF-8")
            self.send_header("Content-Length", str(len(body)))
            if self.path.startswith("/services/data"):
                self.send_header(
                    "Sforce-Limit-Info",
                    f"api-usage={app.api_calls}/{app.config.daily_api_limit}",
                )
            self.end_headers()
            self.wfile.write(body)
            entry = {"method": self.command, "path": self.path, "status": status, **(extra or {})}
            if not self.path.startswith("/__admin"):
                app.requests.append(entry)
            print(json.dumps({"fake_salesforce": entry}), flush=True)

        def _dispatch(self) -> None:
            path = urlsplit(self.path).path
            try:
                if path.startswith("/__admin"):
                    self._admin(path)
                elif path == "/services/oauth2/token" and self.command == "POST":
                    self._token()
                elif path.rstrip("/") == "/services/data" and self.command == "GET":
                    self._versions()
                elif path.startswith("/services/data/v") and self.command == "GET":
                    self._data(path)
                else:
                    self._send(
                        404,
                        [
                            {
                                "errorCode": "NOT_FOUND",
                                "message": "The requested resource does not exist",
                            }
                        ],
                    )
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                self._send(400, {"error": "bad_request", "detail": str(exc)})

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _dispatch

        # ---- OAuth -----------------------------------------------------------------

        def _token(self) -> None:
            fault = app.take_fault("token")
            if fault is not None:
                self._send(fault.status, fault.body, {"fault": True})
                return
            form = {k: v[0] for k, v in parse_qs(self._body().decode()).items()}
            client_id = form.get("client_id")
            client_secret = form.get("client_secret")
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Basic "):
                decoded = base64.b64decode(auth[6:]).decode()
                client_id, _, client_secret = decoded.partition(":")
                client_id, client_secret = unquote_plus(client_id), unquote_plus(client_secret)
            if form.get("grant_type") != "client_credentials":
                self._send(
                    400,
                    {
                        "error": "unsupported_grant_type",
                        "error_description": "grant type not supported",
                    },
                )
                return
            if client_id != app.config.client_id:
                self._send(
                    400,
                    {
                        "error": "invalid_client_id",
                        "error_description": "client identifier invalid",
                    },
                )
                return
            if client_secret != app.config.client_secret:
                self._send(
                    400,
                    {"error": "invalid_client", "error_description": "invalid client credentials"},
                )
                return
            token = f"{ORG_ID}!{secrets.token_urlsafe(32)}"
            with app.lock:
                app.tokens[token] = time.monotonic()
            host = self.headers.get("Host", "localhost")
            self._send(
                200,
                {
                    "access_token": token,
                    "signature": secrets.token_urlsafe(24),
                    "scope": "api",
                    "instance_url": f"http://{host}",
                    "id": f"http://{host}/id/{ORG_ID}AAA/{USER_ID}AAA",
                    "token_type": "Bearer",
                    "issued_at": str(int(time.time() * 1000)),
                },
            )

        # ---- REST data API ------------------------------------------------------------

        def _versions(self) -> None:
            self._send(
                200,
                [
                    {"label": f"v{v}", "url": f"/services/data/v{v}", "version": v}
                    for v in app.config.api_versions
                ],
            )

        def _authorised(self) -> bool:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer ") and app.token_valid(auth[7:]):
                return True
            self._send(
                401, [{"message": "Session expired or invalid", "errorCode": "INVALID_SESSION_ID"}]
            )
            return False

        def _data(self, path: str) -> None:
            match = _VERSION_PATH.match(path)
            if match is None or match["version"] not in app.config.api_versions:
                self._send(
                    404,
                    [
                        {
                            "errorCode": "NOT_FOUND",
                            "message": "The requested resource does not exist",
                        }
                    ],
                )
                return
            if not self._authorised():
                return
            with app.lock:
                app.api_calls += 1
            fault = app.take_fault("query")
            if fault is not None:
                self._send(fault.status, fault.body, {"fault": True})
                return
            version = f"v{match['version']}"
            rest = match["rest"] or ""
            if rest.rstrip("/") == "/query":
                self._first_page(version)
                return
            locator = _LOCATOR_PATH.match(rest)
            if locator is not None:
                self._next_page(version, locator["locator"], int(locator["offset"]))
                return
            self._send(
                404,
                [{"errorCode": "NOT_FOUND", "message": "The requested resource does not exist"}],
            )

        def _first_page(self, version: str) -> None:
            params = parse_qs(urlsplit(self.path).query)
            soql = params.get("q", [""])[0]
            try:
                records = app.store.run_query(parse(soql), version)
            except SoqlError as exc:
                self._send(
                    400, [{"message": exc.message, "errorCode": exc.error_code}], {"soql": soql}
                )
                return
            locator = f"01gFAKE{next(app.locator_ids):012d}"
            with app.lock:
                app.cursors[locator] = records
            self._send(200, self._page(version, locator, 0), {"soql": soql})

        def _next_page(self, version: str, locator: str, offset: int) -> None:
            with app.lock:
                known = locator in app.cursors
            if not known:
                self._send(
                    400,
                    [{"message": "invalid query locator", "errorCode": "INVALID_QUERY_LOCATOR"}],
                )
                return
            self._send(200, self._page(version, locator, offset))

        def _page(self, version: str, locator: str, offset: int) -> Record:
            with app.lock:
                records = app.cursors[locator]
            size = app.config.page_size
            page = records[offset : offset + size]
            done = offset + size >= len(records)
            body: Record = {"totalSize": len(records), "done": done, "records": page}
            if not done:
                body["nextRecordsUrl"] = f"/services/data/{version}/query/{locator}-{offset + size}"
            else:
                with app.lock:
                    app.cursors.pop(locator, None)
            return body

        # ---- admin API -------------------------------------------------------------------

        def _admin(self, path: str) -> None:
            method = self.command
            if path == "/__admin/health":
                self._send(200, {"ok": True})
            elif path == "/__admin/state" and method == "GET":
                self._send(
                    200,
                    {
                        "accounts": list(app.store.accounts.values()),
                        "opportunities": [
                            app.store.project(o, _ALL_FIELDS, "v66.0")
                            for o in app.store.opportunities.values()
                        ],
                    },
                )
            elif path == "/__admin/requests" and method == "GET":
                self._send(200, list(app.requests))
            elif path == "/__admin/requests" and method == "DELETE":
                app.requests.clear()
                self._send(200, {"ok": True})
            elif path == "/__admin/reset" and method == "POST":
                body = self._json_body()
                with app.lock:
                    app.tokens.clear()
                    app.cursors.clear()
                    app.faults.clear()
                    app.requests.clear()
                if body.get("seed", True):
                    app.load_seed()
                else:
                    app.store.reset()
                self._send(200, {"ok": True})
            elif path == "/__admin/accounts" and method == "PUT":
                self._send(200, [app.store.upsert_account(a) for a in self._json_body()])
            elif path == "/__admin/opportunities" and method == "PUT":
                created = [app.store.upsert_opportunity(o) for o in self._json_body()]
                self._send(200, [app.store.project(o, _ALL_FIELDS, "v66.0") for o in created])
            elif path.startswith("/__admin/opportunities/") and method == "PATCH":
                opp_id = path.rsplit("/", 1)[1]
                body = self._json_body()
                try:
                    opp = app.store.edit_opportunity(
                        opp_id, body.get("changes", {}), body.get("SystemModstamp")
                    )
                except KeyError:
                    self._send(404, {"error": "no such opportunity", "id": opp_id})
                    return
                self._send(200, app.store.project(opp, _ALL_FIELDS, "v66.0"))
            elif path.startswith("/__admin/opportunities/") and method == "DELETE":
                deleted = app.store.delete_opportunity(path.rsplit("/", 1)[1])
                self._send(200 if deleted else 404, {"deleted": deleted})
            elif path == "/__admin/faults" and method == "POST":
                body = self._json_body()
                fault = Fault(
                    status=int(body.get("status", 503)),
                    body=body.get(
                        "body",
                        [{"message": "Service Unavailable", "errorCode": "SERVER_UNAVAILABLE"}],
                    ),
                    remaining=int(body.get("count", 1)),
                )
                with app.lock:
                    app.faults.setdefault(body["target"], []).append(fault)
                self._send(200, {"ok": True})
            elif path == "/__admin/revoke-tokens" and method == "POST":
                with app.lock:
                    app.tokens.clear()
                self._send(200, {"ok": True})
            else:
                self._send(404, {"error": f"unknown admin route {method} {path}"})

    return Handler


_ALL_FIELDS = (
    "Id",
    "Name",
    "AccountId",
    "Account.Name",
    "Amount",
    "CloseDate",
    "StageName",
    "IsWon",
    "SystemModstamp",
)


def serve(port: int | None = None) -> None:
    config = Config.from_env()
    seed_env = os.environ.get("FAKE_SF_SEED")
    seed_path = DEFAULT_SEED if seed_env is None else (Path(seed_env) if seed_env else None)
    app = App(config=config, seed_path=seed_path)
    app.load_seed()
    listen_port = port or int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", listen_port), make_handler(app))
    print(json.dumps({"fake_salesforce": f"listening on :{listen_port}"}), flush=True)
    server.serve_forever()
