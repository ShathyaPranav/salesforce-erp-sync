"""Exponential backoff through SQS's visibility timeout.

A failed message becomes visible again when its visibility timeout runs out.
So instead of sleeping inside the Lambda (paying for idle time), the worker
sets that timeout per message before reporting it failed:

    delay = min(cap, base * 2^(attempt - 1)) + jitter

attempt is SQS's ApproximateReceiveCount (1 on the first delivery). The jitter
spreads retries out, so a burst of failures doesn't retry in lockstep and hit
the recovering ERP all at once.

With base 60 s the delays are about 60, 120, 240 and 480 s; the 5th receive
failing sends the message to the DLQ, so the retry budget is about 15 minutes.
"""

from __future__ import annotations

import math
import random

MAX_VISIBILITY_TIMEOUT = 12 * 60 * 60  # SQS's hard limit


def next_visibility_timeout(
    attempt: int, base: float, cap: float, rng: random.Random | None = None
) -> int:
    attempt = max(1, attempt)
    delay = min(cap, base * 2 ** (attempt - 1))
    jitter = (rng or random).uniform(0, base)
    return int(min(MAX_VISIBILITY_TIMEOUT, math.ceil(delay + jitter)))
