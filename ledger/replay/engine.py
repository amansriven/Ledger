"""Replay: answer intercepted calls from a recorded run instead of the network.

Replay is strict: every call must find a recording at the same step key
whose request has the same match key (see ``ledger.core.normalize``), or
replay stops with ReplayMismatch. Divergence detection that switches to live
calls from the first mismatch comes next in M1.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from ledger.core.events import body_kind, pb
from ledger.core.session import Mode, Session, activate
from ledger.core.store import EventStore, step_key

__all__ = ["ReplayIndex", "ReplayMismatch", "ReplayDivergence", "ReplayedError", "replay"]

CALL_KINDS = ("llm_call", "tool_call", "nondet")


class ReplayMismatch(RuntimeError):
    """A call during replay has no matching recording."""


class ReplayDivergence(RuntimeError):
    """Replay finished but did not make every recorded call."""


class ReplayedError(RuntimeError):
    """A recorded call raised; replay raises this in its place."""

    def __init__(self, error: pb.Error) -> None:
        super().__init__(f"{error.type}: {error.message}")
        self.original_type = error.type
        self.original_message = error.message


def _recorded_match(event: pb.Event) -> tuple[str, str]:
    """(match key, raw digest) of a recorded call's request.

    M0 logs have no match key; they are compared on the raw digest.
    """
    kind = body_kind(event)
    if kind == "llm_call":
        return event.llm_call.request_key, event.llm_call.request.sha256
    if kind == "tool_call":
        return event.tool_call.args_key, event.tool_call.args.sha256
    if kind == "nondet":
        return event.nondet.source, event.nondet.source
    return "", ""


class ReplayIndex:
    """Recorded calls of one run, keyed by step."""

    def __init__(self, events: Iterable[pb.Event]) -> None:
        self.calls: dict[str, pb.Event] = {}
        self.output_sha256 = ""
        self.input: pb.Payload | None = None
        for ev in events:
            kind = body_kind(ev)
            if kind in CALL_KINDS:
                self.calls[step_key(ev.step)] = ev
            elif kind == "run_started":
                self.input = ev.run_started.input
            elif kind == "run_finished" and ev.run_finished.HasField("output"):
                self.output_sha256 = ev.run_finished.output.sha256
        self._consumed: set[str] = set()
        self._lock = threading.Lock()

    def take(self, step: pb.StepRef, kind: str, *, match_key: str, sha256: str) -> pb.Event:
        """The recording at ``step``, if it is a ``kind`` call with the same request.

        ``match_key`` is the normalized request's digest and ``sha256`` the raw
        one; which is compared depends on what the recording carries.
        """
        key = step_key(step)
        recorded = self.calls.get(key)
        if recorded is None:
            raise ReplayMismatch(f"no recorded {kind} at step {key}")
        if body_kind(recorded) != kind:
            raise ReplayMismatch(
                f"step {key} recorded a {body_kind(recorded)}, replay made a {kind}"
            )
        recorded_key, recorded_sha = _recorded_match(recorded)
        if recorded_key:
            expected, actual = recorded_key, match_key
        else:
            expected, actual = recorded_sha, sha256
        if expected != actual:
            raise ReplayMismatch(
                f"request at step {key} changed: recorded {expected[:12]}, "
                f"replayed {actual[:12]}"
            )
        with self._lock:
            if key in self._consumed:
                raise ReplayMismatch(f"step {key} was replayed twice")
            self._consumed.add(key)
        return recorded

    def unconsumed(self) -> list[str]:
        with self._lock:
            return sorted(set(self.calls) - self._consumed)


@contextmanager
def replay(
    source_run_id: str,
    store: EventStore | None = None,
    run_id: str | None = None,
    *,
    strict: bool = True,
) -> Iterator[Session]:
    """Replay ``source_run_id``: every intercepted call is answered from its log.

    The replay is itself logged as a new run (``run_id``, by default
    ``<source>.replay-<n>``) so it can be compared with the recording.
    With ``strict``, leaving the block with recorded calls that were never
    made raises ReplayDivergence.
    """
    owns_store = store is None
    store = store or EventStore()
    try:
        index = ReplayIndex(store.events(source_run_id))
        if not index.calls and index.input is None:
            raise KeyError(f"no recorded run {source_run_id!r} in {store.path}")
        if run_id is None:
            n = sum(1 for r in store.run_ids() if r.startswith(f"{source_run_id}.replay-"))
            run_id = f"{source_run_id}.replay-{n + 1}"
        session = Session(
            store,
            run_id,
            Mode.REPLAY,
            source_run_id=source_run_id,
            replay_index=index,
        )
        with activate(session):
            yield session
        missing = index.unconsumed()
        if strict and missing:
            raise ReplayDivergence(
                f"replay of {source_run_id} skipped {len(missing)} recorded call(s), "
                f"first at {missing[0]}"
            )
    finally:
        if owns_store:
            store.close()
