"""Transient or permanent? The second key decision in Relay.

  transient  -> retry later with backoff. The same message can succeed once
                the ERP recovers: throttling, a transaction conflict, a timeout,
                the chaos flag, and anything we didn't anticipate.
  permanent  -> dead-letter now, with a reason. Retrying the same message can
                never succeed: missing Amount or Account, unmappable fields, an
                item the ERP refuses.

Getting this wrong costs in both directions. Retrying a permanent error burns
the retry budget and delays the DLQ alert; dead-lettering a transient error
loses work that would have succeeded a minute later.

Unknown errors default to TRANSIENT on purpose: the queue's maxReceiveCount
(5) bounds them anyway, so a bug costs a few retries, while defaulting to
permanent would dead-letter good data after every bad deploy.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from erp.writer import ErpRejected, ErpUnavailable
from worker.mapping import PermanentError


class Kind(enum.Enum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class Decision:
    kind: Kind
    reason: str


def classify(exc: BaseException) -> Decision:
    if isinstance(exc, PermanentError):
        return Decision(Kind.PERMANENT, exc.reason)
    if isinstance(exc, ErpRejected):
        return Decision(Kind.PERMANENT, "erp_rejected")
    if isinstance(exc, ErpUnavailable):
        return Decision(Kind.TRANSIENT, "erp_unavailable")
    return Decision(Kind.TRANSIENT, f"unexpected:{type(exc).__name__}")
