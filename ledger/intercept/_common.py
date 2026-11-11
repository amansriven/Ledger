"""Record/replay plumbing shared by the LLM and tool interceptors."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from ledger.core.events import load_payload, make_error, make_payload, new_event, pb
from ledger.core.session import Session

# Name of the field that holds the call's output, per event body kind.
RESPONSE_FIELD = {"llm_call": "response", "tool_call": "result"}
REQUEST_FIELD = {"llm_call": "request", "tool_call": "args"}


def answer_from_log(
    session: Session,
    step: pb.StepRef,
    kind: str,
    request: pb.Payload,
    decode: Callable[[Any], Any],
) -> Any:
    """Return (or re-raise) the recorded outcome of this call, and log it."""
    from ledger.replay.engine import ReplayedError

    assert session.replay_index is not None
    recorded = session.replay_index.take(step, kind, request.sha256)
    body = getattr(recorded, kind)
    ev = new_event(step)
    getattr(ev, kind).CopyFrom(body)
    session.append(ev)
    session.count_call(live=False)
    if body.HasField("error"):
        raise ReplayedError(body.error)
    return decode(load_payload(getattr(body, RESPONSE_FIELD[kind])))


def log_live(
    session: Session,
    step: pb.StepRef,
    kind: str,
    body: Any,
    *,
    response: Any = None,
    error: BaseException | None = None,
    started_ns: int,
    encode: Callable[[Any], Any] = lambda v: v,
) -> None:
    """Append the outcome of a live call. ``body`` already holds the request."""
    body.latency_us = (time.perf_counter_ns() - started_ns) // 1000
    if error is not None:
        body.error.CopyFrom(make_error(error))
    else:
        getattr(body, RESPONSE_FIELD[kind]).CopyFrom(make_payload(encode(response)))
    ev = new_event(step)
    getattr(ev, kind).CopyFrom(body)
    session.append(ev)
    session.count_call(live=True)
