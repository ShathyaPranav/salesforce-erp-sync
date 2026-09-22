"""The Python event builder matches the shared golden file.

internal/events/golden_test.go checks the Go builder against the same file,
so the poller and the reconciler can't drift apart.
"""

from __future__ import annotations

import json
from pathlib import Path

from reconciler.events import from_salesforce, parse_modstamp

GOLDEN = Path(__file__).parent / "fixtures" / "event_golden.json"


def test_python_builder_matches_golden_file():
    cases = json.loads(GOLDEN.read_text(encoding="utf-8"))["cases"]
    assert cases
    for case in cases:
        published = parse_modstamp(case["published_at"])
        assert from_salesforce(case["salesforce_row"], published) == case["event"]
