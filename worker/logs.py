"""One JSON object per log line, so CloudWatch Logs Insights can filter on any
field. Every line about a message carries its event_key."""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

SERVICE = os.environ.get("RELAY_SERVICE", "worker")


def log(level: str, msg: str, **fields: Any) -> None:
    line = {"ts": datetime.now(UTC).isoformat(), "level": level, "service": SERVICE, "msg": msg}
    line.update(fields)
    print(json.dumps(line, default=str), file=sys.stdout, flush=True)
