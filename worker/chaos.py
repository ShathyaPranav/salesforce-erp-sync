"""Chaos flags: failures you switch on to prove the recovery paths work.

They live in SSM so the same switch works locally and on AWS
(`relayctl chaos --erp-fail-rate 0.3`). The worker reads them once per
invocation, so a change takes effect on the next batch.

  /relay/chaos/erp_fail_rate       0.0-1.0: this share of ERP writes fails with
                                   a throttling error (a transient failure)
  /relay/chaos/worker_crash_after  N >= 1: once N records are processed, the
                                   invocation dies with an unhandled error, like
                                   a Lambda timeout or crash, before any of the
                                   batch is acknowledged. 0, negative or missing = off
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from erp.writer import ErpUnavailable


class WorkerCrash(Exception):
    """Raised outside the per-record error handling, so the invocation fails."""


@dataclass(frozen=True)
class Chaos:
    erp_fail_rate: float = 0.0
    crash_after: int = 0

    @classmethod
    def load(cls, ssm: Any, prefix: str) -> Chaos:
        names = [f"{prefix}/chaos/erp_fail_rate", f"{prefix}/chaos/worker_crash_after"]
        try:
            params = ssm.get_parameters(Names=names)["Parameters"]
        except Exception:
            return cls()  # chaos is optional: never let it break real work
        values = {p["Name"].rsplit("/", 1)[1]: p["Value"] for p in params}
        return cls(
            erp_fail_rate=_float(values.get("erp_fail_rate"), 0.0),
            crash_after=int(_float(values.get("worker_crash_after"), 0)),
        )

    def maybe_fail_erp(self, rng: random.Random | None = None) -> None:
        if self.erp_fail_rate > 0 and (rng or random).random() < self.erp_fail_rate:
            raise ErpUnavailable("ThrottlingException (injected by chaos flag)")

    def maybe_crash(self, processed: int) -> None:
        if 0 < self.crash_after <= processed:
            raise WorkerCrash(f"chaos: worker crashed after {processed} record(s)")


def _float(value: str | None, default: float) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except ValueError:
        return default
