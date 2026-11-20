"""Idempotency keys for effects.

A key is derived from the run id and the step key, so it is the same every
time the same run reaches the same call: on a retry within the call, and
after a crash when the run resumes under its own id. An API that honours
idempotency keys (Stripe does, for 24 hours) applies the call once and
answers every repeat with the first response.

A replay never sends an effect, so it never uses a key. A fork that runs an
effect live does so as a new run, with keys of its own.
"""

from __future__ import annotations

import hashlib

from ledger.core.events import pb
from ledger.core.store import step_key

__all__ = ["idempotency_key"]


def idempotency_key(run_id: str, step: pb.StepRef) -> str:
    """``ledger_`` plus 40 hex digits of SHA-256 over the run id and step key."""
    digest = hashlib.sha256(f"{run_id}|{step_key(step)}".encode()).hexdigest()
    return f"ledger_{digest[:40]}"
