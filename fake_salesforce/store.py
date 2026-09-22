"""In-memory Accounts and Opportunities, plus query evaluation.

Records are stored with Salesforce API field names. Timestamps are kept to the
second, like Salesforce stores them, which is exactly why two records edited
in the same second share a SystemModstamp (the case the poller's >= handles).
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from fake_salesforce.soql import Condition, OrderBy, Query, SoqlError, parse_datetime_literal

Record = dict[str, Any]

WON_STAGE = "Closed Won"
CLOSED_STAGES = ("Closed Won", "Closed Lost")


def format_datetime(value: datetime) -> str:
    """Salesforce's wire format: 2026-09-22T10:00:00.000+0000."""
    utc = value.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}+0000"


def _now_to_second() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class Store:
    def __init__(self, clock: Callable[[], datetime] = _now_to_second) -> None:
        self._lock = threading.RLock()
        self._clock = clock
        self._ids = itertools.count(1)
        self.accounts: dict[str, Record] = {}
        self.opportunities: dict[str, Record] = {}

    # ---- ids -------------------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        # 18 characters, like a real Salesforce ID: 3-char key prefix + 15.
        # Skip IDs the seed file already uses.
        while True:
            candidate = f"{prefix}FAKE{next(self._ids):011d}"
            if candidate not in self.accounts and candidate not in self.opportunities:
                return candidate

    # ---- writes (used by the admin API and the seed loader) ---------------

    def reset(self) -> None:
        with self._lock:
            self.accounts.clear()
            self.opportunities.clear()

    def load(self, seed: dict[str, Any]) -> None:
        with self._lock:
            for account in seed.get("accounts", []):
                self.upsert_account(account)
            for opp in seed.get("opportunities", []):
                self.upsert_opportunity(opp)

    def upsert_account(self, data: Record) -> Record:
        with self._lock:
            account_id = data.get("Id") or self._new_id("001")
            account = {**self.accounts.get(account_id, {}), **data, "Id": account_id}
            self.accounts[account_id] = account
            return dict(account)

    def upsert_opportunity(self, data: Record) -> Record:
        """Create or replace an Opportunity.

        SystemModstamp is taken from the payload if given (so tests can force
        equal or out-of-order timestamps), otherwise stamped with "now".
        """
        with self._lock:
            opp_id = data.get("Id") or self._new_id("006")
            existing = self.opportunities.get(opp_id, {})
            opp: Record = {**existing, **data, "Id": opp_id}
            stamp = self._stamp(data.get("SystemModstamp"))
            opp["SystemModstamp"] = stamp
            opp["LastModifiedDate"] = stamp
            opp.setdefault("CreatedDate", existing.get("CreatedDate", stamp))
            if isinstance(opp["CreatedDate"], str):
                opp["CreatedDate"] = parse_datetime_literal(opp["CreatedDate"])
            if opp.get("Amount") is not None:
                opp["Amount"] = Decimal(str(opp["Amount"]))
            if isinstance(opp.get("CloseDate"), str):
                opp["CloseDate"] = date.fromisoformat(opp["CloseDate"])
            opp.setdefault("AccountId", None)
            opp.setdefault("Amount", None)
            self.opportunities[opp_id] = opp
            return dict(opp)

    def edit_opportunity(
        self, opp_id: str, changes: Record, system_modstamp: str | None = None
    ) -> Record:
        """Apply a user edit: change fields and bump SystemModstamp."""
        with self._lock:
            if opp_id not in self.opportunities:
                raise KeyError(opp_id)
            payload = {**self.opportunities[opp_id], **changes, "Id": opp_id}
            payload["SystemModstamp"] = system_modstamp
            return self.upsert_opportunity(payload)

    def delete_opportunity(self, opp_id: str) -> bool:
        with self._lock:
            return self.opportunities.pop(opp_id, None) is not None

    def _stamp(self, value: Any) -> datetime:
        if value is None:
            return self._clock()
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        return parse_datetime_literal(str(value))

    # ---- reads --------------------------------------------------------------

    def _field_value(self, opp: Record, field: str) -> Any:
        if field == "IsWon":
            return opp.get("StageName") == WON_STAGE
        if field == "IsClosed":
            return opp.get("StageName") in CLOSED_STAGES
        if field.startswith("Account."):
            account = self.accounts.get(opp.get("AccountId") or "")
            return None if account is None else account.get(field.split(".", 1)[1])
        return opp.get(field)

    @staticmethod
    def _matches(value: Any, cond: Condition) -> bool:
        target = cond.value
        if cond.op == "IN":
            return isinstance(target, tuple) and value in target
        if cond.op == "=":
            return bool(value == target)
        if cond.op == "!=":
            return bool(value != target)
        if value is None or target is None:
            return False  # ordering comparisons never match null, as in SOQL
        if cond.op == ">=":
            return bool(value >= target)
        if cond.op == ">":
            return bool(value > target)
        if cond.op == "<=":
            return bool(value <= target)
        if cond.op == "<":
            return bool(value < target)
        raise SoqlError("MALFORMED_QUERY", f"unsupported operator {cond.op}")

    def _sorted(self, records: list[Record], order_by: Iterable[OrderBy]) -> list[Record]:
        # Stable multi-key sort: apply keys from last to first. SOQL puts nulls
        # first for ASC and last for DESC.
        result = list(records)
        for key in reversed(list(order_by)):
            result.sort(key=self._sort_key(key.field), reverse=key.descending)
        return result

    def _sort_key(self, field: str) -> Callable[[Record], tuple[bool, Any]]:
        def key(record: Record) -> tuple[bool, Any]:
            value = self._field_value(record, field)
            return (value is not None, value if value is not None else 0)

        return key

    def run_query(self, query: Query, api_version: str) -> list[Record]:
        with self._lock:
            rows = [
                opp
                for opp in self.opportunities.values()
                if all(self._matches(self._field_value(opp, c.field), c) for c in query.where)
            ]
            rows = self._sorted(rows, query.order_by)
            if query.limit is not None:
                rows = rows[: query.limit]
            return [self.project(opp, query.fields, api_version) for opp in rows]

    def project(self, opp: Record, fields: Iterable[str], api_version: str) -> Record:
        """Shape one record exactly like the REST API's query response."""
        out: Record = {
            "attributes": {
                "type": "Opportunity",
                "url": f"/services/data/{api_version}/sobjects/Opportunity/{opp['Id']}",
            }
        }
        account_fields: list[str] = []
        for field in fields:
            if field.startswith("Account."):
                account_fields.append(field.split(".", 1)[1])
                out.setdefault("Account", None)
                continue
            out[field] = _to_json(self._field_value(opp, field))
        if account_fields:
            account = self.accounts.get(opp.get("AccountId") or "")
            if account is not None:
                nested: Record = {
                    "attributes": {
                        "type": "Account",
                        "url": f"/services/data/{api_version}/sobjects/Account/{account['Id']}",
                    }
                }
                for name in account_fields:
                    nested[name] = _to_json(account.get(name))
                out["Account"] = nested
        return out


def _to_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value
