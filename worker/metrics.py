"""Custom CloudWatch metrics via the Embedded Metric Format (EMF).

An EMF record is just a JSON log line with an `_aws` block describing which
fields are metrics. CloudWatch extracts them from the Lambda's logs, so
emitting a metric costs no API call and can't fail the invocation.

Every Relay metric has exactly one dimension, Service. Each (name, dimension
values) pair is a separate billed custom metric, so extra dimensions would
quickly go past the 10 in the free tier. Relay has 7.

Metrics are counts per invocation. Under retries they're approximate (a
message written on attempt 1 and redelivered counts once as written and once
as a duplicate), which is what they're for: seeing the system work.
"""

from __future__ import annotations

import json
import sys
import time

NAMESPACE = "Relay"


def emit(service: str, counts: dict[str, int]) -> None:
    if not counts:
        return
    record = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": NAMESPACE,
                    "Dimensions": [["Service"]],
                    "Metrics": [{"Name": name, "Unit": "Count"} for name in counts],
                }
            ],
        },
        "Service": service,
        **counts,
    }
    print(json.dumps(record), file=sys.stdout, flush=True)
