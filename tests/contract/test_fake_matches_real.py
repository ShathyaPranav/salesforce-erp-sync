"""Contract test: the fake Salesforce returns the same JSON shape as the real API.

The "real" side is a response recorded once from your Developer Edition org:

    python scripts/sf_query.py --record tests/contract/fixtures/real_query_response.json

Until that file exists, this test is skipped (and says so in the pytest summary).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from fake_salesforce.server import App, Config
from fake_salesforce.soql import parse

FIXTURE = Path(__file__).parent / "fixtures" / "real_query_response.json"
SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp "
    "FROM Opportunity WHERE StageName = 'Closed Won' AND SystemModstamp >= 1970-01-01T00:00:00Z "
    "ORDER BY SystemModstamp ASC, Id ASC"
)
MODSTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+0000$")

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(),
    reason="no recorded real response yet: run scripts/sf_query.py --record (needs your org)",
)


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int | float):
        return "number"
    return type(value).__name__


def fake_response() -> dict[str, Any]:
    app = App(config=Config())
    app.load_seed()
    records = app.store.run_query(parse(SOQL), "v66.0")
    return {"totalSize": len(records), "done": True, "records": records}


def test_top_level_keys_match():
    real = json.loads(FIXTURE.read_text(encoding="utf-8"))
    fake = fake_response()
    assert set(fake) - {"nextRecordsUrl"} == set(real) - {"nextRecordsUrl"}


def test_record_fields_and_types_match():
    real = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert real["records"], "record some Closed Won deals first"
    fake = fake_response()
    real_rec = real["records"][0]
    fake_rec = next(r for r in fake["records"] if r["Account"] is not None and r["Amount"])
    assert list(fake_rec) == list(real_rec), "field order and names differ"
    for key, value in real_rec.items():
        if value is None or fake_rec[key] is None:
            continue
        assert json_type(fake_rec[key]) == json_type(value), key
    assert set(fake_rec["attributes"]) == set(real_rec["attributes"])
    assert set(fake_rec["Account"]) == set(real_rec["Account"])


def test_timestamp_and_date_formats_match():
    real = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for rec in real["records"]:
        assert MODSTAMP.match(rec["SystemModstamp"]), rec["SystemModstamp"]
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", rec["CloseDate"])
