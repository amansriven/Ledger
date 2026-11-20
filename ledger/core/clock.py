"""Recorded time, uuids and randomness.

Agent code that reads the clock, makes an id or draws a random number gets a
different value on every run, so a replay that re-executes it would build
different prompts and state. Call these instead of the stdlib functions:

    ledger.time()       # instead of time.time()
    ledger.now()        # instead of datetime.now(); ledger.now(UTC) for aware
    ledger.uuid4()      # instead of uuid.uuid4()
    ledger.rng()        # a random.Random; instead of the random module

During a recording each value is read live and logged as a ``Nondet`` event
at the calling task's step; during a replay it comes from the log. Outside a
session they behave like the stdlib.

``ledger.rng()`` logs one seed per LangGraph task, not every draw: the first
call in a task records a seed and returns a ``random.Random`` seeded with it,
later calls in the same task return that same generator. Draw from it in the
same order and the replay draws the same numbers.

They are explicit calls rather than patches of ``time``, ``uuid`` and
``random`` because LangGraph, httpx and the OpenAI client read the clock too,
and how often they do so varies from run to run (timeouts, retries); logging
those reads would make every replay diverge.
"""

from __future__ import annotations

import os
import random
import time as _time
import uuid as _uuid
from collections.abc import Callable
from datetime import datetime, tzinfo
from typing import Any

from ledger.core.events import make_payload, new_event, pb
from ledger.core.session import Session, current_session, task_coordinates

__all__ = ["time", "now", "uuid4", "rng"]


def _read(session: Session, source: str, live: Callable[[], Any]) -> Any:
    """Read one JSON value from ``live`` (record) or from the log (replay)."""
    from ledger.intercept._common import GO_LIVE, answer_from_log

    step = session.next_step()
    if session.is_replay:
        out = answer_from_log(session, step, "nondet", None, lambda v: v, match_key=source)
        if out is not GO_LIVE:
            return out
    value = live()
    ev = new_event(step)
    ev.nondet.CopyFrom(pb.Nondet(source=source, value=make_payload(value)))
    session.append(ev)
    session.count_call(live=True, kind="nondet")
    return value


def time() -> float:
    """Seconds since the epoch, like ``time.time()``."""
    session = current_session()
    if session is None:
        return _time.time()
    return _read(session, "time", _time.time)


def now(tz: tzinfo | None = None) -> datetime:
    """The current time, like ``datetime.now(tz)``. One recorded clock read."""
    return datetime.fromtimestamp(time(), tz)


def uuid4() -> _uuid.UUID:
    """A random UUID, like ``uuid.uuid4()``."""
    session = current_session()
    if session is None:
        return _uuid.uuid4()
    return _uuid.UUID(_read(session, "uuid4", lambda: str(_uuid.uuid4())))


def rng() -> random.Random:
    """The current task's random generator, seeded from the log on replay."""
    session = current_session()
    if session is None:
        return random.Random()
    coords = task_coordinates()
    with session._lock:
        gen = session._rngs.get(coords)
    if gen is not None:
        return gen
    seed = _read(session, "rng_seed", lambda: int.from_bytes(os.urandom(8), "big"))
    gen = random.Random(seed)
    with session._lock:
        # Two threads of one task racing here is not a pattern LangGraph
        # produces; keep the first generator if it happens anyway.
        return session._rngs.setdefault(coords, gen)
