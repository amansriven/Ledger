"""Record/replay plumbing shared by the LLM and tool interceptors."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from ledger.core.events import load_payload, make_error, make_payload, new_event, pb
from ledger.core.session import Session

# Name of the field that holds the call's output, per event body kind.
RESPONSE_FIELD = {"llm_call": "response", "tool_call": "result", "nondet": "value"}
REQUEST_FIELD = {"llm_call": "request", "tool_call": "args"}
KEY_FIELD = {"llm_call": "request_key", "tool_call": "args_key"}


class _GoLive:
    """Returned by answer_from_log when the call has to run live."""

    def __repr__(self) -> str:
        return "GO_LIVE"


GO_LIVE: Any = _GoLive()


def answer_from_log(
    session: Session,
    step: pb.StepRef,
    kind: str,
    request: pb.Payload | None,
    decode: Callable[[Any], Any],
    *,
    match_key: str,
) -> Any:
    """Return (or re-raise) the recorded outcome of this call, and log it.

    Returns GO_LIVE instead when the replay has diverged at or before this
    call (``on_divergence="live"``); the caller then makes the call live and
    logs it as usual. A new divergence point gets a Diverged event first.

    The replay's log entry carries the request as it was made this time (it
    may differ from the recording in masked parts) with the recorded outcome.
    """
    from ledger.replay.engine import (
        DivergencePoint,
        DivergenceReason,
        ReplayedError,
        diverged_event_body,
    )

    assert session.replay_index is not None
    recorded = session.replay_index.resolve(
        step, kind, match_key=match_key, sha256=request.sha256 if request else match_key
    )
    if recorded is None:
        return GO_LIVE
    if isinstance(recorded, DivergencePoint):
        ev = new_event(step)
        ev.diverged.CopyFrom(diverged_event_body(recorded))
        session.append(ev)
        if recorded.reason is not DivergenceReason.EDITED:
            return GO_LIVE
        return _serve_edit(session, step, kind, request, decode, match_key, recorded.step)
    body = getattr(recorded, kind)
    ev = new_event(step)
    ev.from_log = True
    logged = getattr(ev, kind)
    logged.CopyFrom(body)
    if kind in REQUEST_FIELD and request is not None:
        getattr(logged, REQUEST_FIELD[kind]).CopyFrom(request)
        setattr(logged, KEY_FIELD[kind], match_key)
    session.append(ev)
    session.count_call(live=False, kind=kind)
    if kind != "nondet" and body.HasField("error"):
        raise ReplayedError(body.error)
    return decode(load_payload(getattr(body, RESPONSE_FIELD[kind])))


def _serve_edit(
    session: Session,
    step: pb.StepRef,
    kind: str,
    request: pb.Payload | None,
    decode: Callable[[Any], Any],
    match_key: str,
    key: str,
) -> Any:
    """Log and return a fork's edited outcome for this call.

    The event keeps the recorded call's metadata, carries the request as
    made this time and the edited outcome, with any recorded error cleared.
    """
    assert session.replay_index is not None
    value = session.replay_index.edited_outcome(key)
    payload = make_payload(value)
    ev = new_event(step)
    body = getattr(ev, kind)
    body.CopyFrom(getattr(session.replay_index.calls[key], kind))
    if kind in REQUEST_FIELD:
        body.ClearField("error")
        if request is not None:
            getattr(body, REQUEST_FIELD[kind]).CopyFrom(request)
            setattr(body, KEY_FIELD[kind], match_key)
    getattr(body, RESPONSE_FIELD[kind]).CopyFrom(payload)
    session.append(ev)
    session.count_call(live=False, kind=kind, edited=True)
    return decode(load_payload(payload))


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
    session.count_call(live=True, kind=kind)
