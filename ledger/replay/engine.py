"""Replay: answer intercepted calls from a recorded run instead of the network.

Every call looks up the recording at its step key and must find a call of
the same kind with the same match key (see ``ledger.core.normalize``). What
happens when it doesn't depends on ``on_divergence``:

``"raise"`` (the default)
    Replay is strict. The first mismatch raises ReplayMismatch, and leaving
    the block with recorded calls that were never made raises
    ReplayDivergence.

``"live"``
    The first mismatching call is a divergence point. It runs live, and so
    does every call downstream of it; everything else keeps coming from the
    log. A Diverged event marks the point in the replay's log, and
    ``session.divergences`` lists the points. Fork uses the same mechanism.

Downstream of a divergence at step D means, by LangGraph's superstep model:

* later calls of D's own task (same node and path, higher ``seq``);
* any call at a later superstep in D's namespace, since that superstep can
  read what D's task wrote;
* any call in a different (nested or enclosing) namespace made after D.
  Step keys carry no parent superstep, so positions across namespaces can't
  be compared, and Ledger takes the conservative side.

Parallel tasks in D's superstep are not downstream: LangGraph runs them on
the state from before the superstep, so they can't see D's output. They keep
replaying, whatever order the branches finish in. A mismatch in one of them
is a divergence point of its own. Only the last rule depends on timing, and
only for graphs with subgraphs.
"""

from __future__ import annotations

import copy
import enum
import threading
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from ledger.core.events import body_kind, load_payload, pb
from ledger.core.session import Mode, Session, activate
from ledger.core.store import EventStore, step_key

__all__ = [
    "RERUN",
    "DivergencePoint",
    "DivergenceReason",
    "ReplayIndex",
    "ReplayMismatch",
    "ReplayDivergence",
    "ReplayedError",
    "is_downstream",
    "replay",
    "replayed_exception",
]

CALL_KINDS = ("llm_call", "tool_call", "nondet")

OnDivergence = Literal["raise", "live"]


class _Rerun:
    def __repr__(self) -> str:
        return "RERUN"


# Fork edit meaning "run this call live again instead of editing it".
RERUN: Any = _Rerun()


class DivergenceReason(enum.Enum):
    CHANGED_REQUEST = "changed_request"
    UNRECORDED_CALL = "unrecorded_call"
    CHANGED_KIND = "changed_kind"
    # A fork replaced this call's recorded outcome.
    EDITED = "edited"
    # A fork ran this call live again.
    RERUN = "rerun"
    # A recorded call the replay never made, and nothing downstream of a
    # divergence explains it. Found when the block exits; nothing goes live.
    SKIPPED_CALL = "skipped_call"

    @property
    def proto(self) -> int:
        return _REASON_PROTO.get(self, pb.DIVERGENCE_REASON_UNSPECIFIED)


_REASON_PROTO = {
    DivergenceReason.CHANGED_REQUEST: pb.DIVERGENCE_REASON_CHANGED_REQUEST,
    DivergenceReason.UNRECORDED_CALL: pb.DIVERGENCE_REASON_UNRECORDED_CALL,
    DivergenceReason.CHANGED_KIND: pb.DIVERGENCE_REASON_CHANGED_KIND,
    DivergenceReason.EDITED: pb.DIVERGENCE_REASON_EDITED,
    DivergenceReason.RERUN: pb.DIVERGENCE_REASON_RERUN,
}


@dataclass(frozen=True)
class DivergencePoint:
    """Where a replay stopped matching its recording."""

    step: str
    reason: DivergenceReason
    # Body kind of the call the replay made (the recorded one, for SKIPPED_CALL).
    kind: str
    # Match keys (or kinds, for CHANGED_KIND) on each side; "" where absent.
    recorded: str
    actual: str
    # The StepRef behind ``step``; None for SKIPPED_CALL.
    ref: pb.StepRef | None = None

    def __str__(self) -> str:
        return f"{self.reason.value} at {self.step} ({self.kind})"


class ReplayMismatch(RuntimeError):
    """A call during replay has no matching recording."""

    def __init__(
        self,
        message: str,
        *,
        step: str = "",
        reason: DivergenceReason | None = None,
        kind: str = "",
        recorded: str = "",
        actual: str = "",
    ) -> None:
        super().__init__(message)
        self.step = step
        self.reason = reason
        self.kind = kind
        self.recorded = recorded
        self.actual = actual


class ReplayDivergence(RuntimeError):
    """Replay finished but did not make every recorded call."""


class ReplayedError(RuntimeError):
    """A recorded call raised an exception replay could not rebuild.

    Replay normally raises the recorded exception's own class (see
    ``replayed_exception``); this stands in when that class isn't loaded or
    can't be constructed.
    """

    def __init__(self, error: pb.Error) -> None:
        super().__init__(f"{error.type}: {error.message}")
        self.original_type = error.type
        self.original_message = error.message


REPLAYED_ATTR = "__ledger_replayed__"


def _loaded_class(qualified: str) -> type[BaseException] | None:
    """The exception class named ``module.qualname``, if its module is loaded.

    Looks only in ``sys.modules``: the log names the class, but replay never
    imports anything because of it.
    """
    import sys

    parts = qualified.split(".")
    for cut in range(len(parts) - 1, 0, -1):
        module = sys.modules.get(".".join(parts[:cut]))
        if module is None:
            continue
        obj: Any = module
        for name in parts[cut:]:
            obj = getattr(obj, name, None)
            if obj is None:
                return None
        if isinstance(obj, type) and issubclass(obj, BaseException):
            return obj
        return None
    return None


def replayed_exception(error: pb.Error) -> BaseException:
    """The exception a recorded call raised, rebuilt for replay.

    Same class, built from the recorded args (or the message, for logs
    without args), so ``except SomeError`` and ``str(exc)`` behave as they
    did when recorded. A class whose constructor wants something else (say,
    a keyword-only ``request=``) is created without running its
    ``__init__``: ``isinstance`` and ``str`` hold, attributes set in
    ``__init__`` are missing. Falls back to ReplayedError when the class
    isn't loaded. The result carries ``__ledger_replayed__ = True``.
    """
    cls = _loaded_class(error.type)
    exc: BaseException | None = None
    if cls is not None:
        args = load_payload(error.args) if error.HasField("args") else [error.message]
        if not isinstance(args, list):
            args = [error.message]
        try:
            exc = cls(*args)
        except Exception:
            try:
                exc = cls.__new__(cls, *args)
                BaseException.__init__(exc, *args)
            except Exception:
                exc = None
        if exc is not None and str(exc) != error.message:
            # A constructor that reformats its args; keep the recorded text.
            try:
                exc = cls.__new__(cls, error.message)
                BaseException.__init__(exc, error.message)
            except Exception:
                exc = None
    if exc is None:
        exc = ReplayedError(error)
    try:
        setattr(exc, REPLAYED_ATTR, True)
    except AttributeError:
        pass
    return exc


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


def is_downstream(call: pb.StepRef, point: pb.StepRef) -> bool:
    """Whether a call at ``call`` can depend on what happened at ``point``.

    See the module docstring for the rule.
    """
    if call.namespace != point.namespace:
        return True
    if call.superstep != point.superstep:
        return call.superstep > point.superstep
    if (call.node, call.path) != (point.node, point.path):
        return False  # a parallel task in the same superstep
    return call.seq >= point.seq


class ReplayIndex:
    """Recorded calls of one run, keyed by step, and where a replay left them."""

    def __init__(
        self,
        events: Iterable[pb.Event],
        *,
        on_divergence: OnDivergence = "raise",
        forks: Mapping[str, Any] | None = None,
    ) -> None:
        if on_divergence not in ("raise", "live"):
            raise ValueError(f"on_divergence must be 'raise' or 'live', not {on_divergence!r}")
        self.on_divergence = on_divergence
        self.calls: dict[str, pb.Event] = {}
        self.output_sha256 = ""
        # The error the recorded run ended with, if it failed.
        self.output_error: pb.Error | None = None
        self.input: pb.Payload | None = None
        for ev in events:
            kind = body_kind(ev)
            if kind in CALL_KINDS:
                self.calls[step_key(ev.step)] = ev
            elif kind == "run_started":
                self.input = ev.run_started.input
            elif kind == "run_finished" and ev.run_finished.HasField("output"):
                self.output_sha256 = ev.run_finished.output.sha256
            elif kind == "run_finished" and ev.run_finished.HasField("error"):
                self.output_error = ev.run_finished.error
        self.divergences: list[DivergencePoint] = []
        # Fork edits by step key: a JSON value, a callable, or RERUN.
        self.forks: dict[str, Any] = dict(forks or {})
        for key in self.forks:
            if key not in self.calls:
                raise KeyError(f"the recording has no call at step {key}")
        self._applied: set[str] = set()
        self._consumed: set[str] = set()
        self._lock = threading.Lock()

    def take(self, step: pb.StepRef, kind: str, *, match_key: str, sha256: str) -> pb.Event:
        """The recording at ``step``, if it is a ``kind`` call with the same request.

        ``match_key`` is the normalized request's digest and ``sha256`` the raw
        one; which is compared depends on what the recording carries. Raises
        ReplayMismatch otherwise, whatever ``on_divergence`` says.
        """
        key = step_key(step)
        recorded = self.calls.get(key)
        if recorded is None:
            raise ReplayMismatch(
                f"no recorded {kind} at step {key}",
                step=key, reason=DivergenceReason.UNRECORDED_CALL, kind=kind,
                actual=match_key,
            )
        recorded_kind = body_kind(recorded)
        if recorded_kind != kind:
            raise ReplayMismatch(
                f"step {key} recorded a {recorded_kind}, replay made a {kind}",
                step=key, reason=DivergenceReason.CHANGED_KIND, kind=kind,
                recorded=recorded_kind, actual=kind,
            )
        recorded_key, recorded_sha = _recorded_match(recorded)
        if recorded_key:
            expected, actual = recorded_key, match_key
        else:
            expected, actual = recorded_sha, sha256
        if expected != actual:
            raise ReplayMismatch(
                f"request at step {key} changed: recorded {expected[:12]}, "
                f"replayed {actual[:12]}",
                step=key, reason=DivergenceReason.CHANGED_REQUEST, kind=kind,
                recorded=expected, actual=actual,
            )
        with self._lock:
            if key in self._consumed:
                raise ReplayMismatch(f"step {key} was replayed twice", step=key, kind=kind)
            self._consumed.add(key)
        return recorded

    def resolve(
        self, step: pb.StepRef, kind: str, *, match_key: str, sha256: str
    ) -> pb.Event | DivergencePoint | None:
        """Decide how to answer a call.

        Returns the recorded event to serve, a new DivergencePoint when this
        call is where the replay diverges (it goes live), or None when the
        call is downstream of an earlier divergence (it goes live too). With
        ``on_divergence="raise"`` a mismatch raises instead.

        A fork step always returns a DivergencePoint (EDITED or RERUN), even
        downstream of another divergence and whatever its request: the edit
        replaces the call's outcome, so the request does not need to match.
        """
        key = step_key(step)
        if key in self.forks:
            return self._fork_point(step, key, kind, match_key)
        with self._lock:
            if any(is_downstream(step, p.ref) for p in self.divergences if p.ref is not None):
                return None
        try:
            return self.take(step, kind, match_key=match_key, sha256=sha256)
        except ReplayMismatch as exc:
            if self.on_divergence == "raise" or exc.reason is None:
                raise
            point = DivergencePoint(
                step=exc.step, reason=exc.reason, kind=exc.kind,
                recorded=exc.recorded, actual=exc.actual, ref=_copy(step),
            )
            with self._lock:
                self.divergences.append(point)
            return point

    def _fork_point(
        self, step: pb.StepRef, key: str, kind: str, match_key: str
    ) -> DivergencePoint:
        recorded = self.calls[key]
        recorded_kind = body_kind(recorded)
        if recorded_kind != kind:
            raise ReplayMismatch(
                f"fork step {key} recorded a {recorded_kind}, replay made a {kind}",
                step=key, reason=DivergenceReason.CHANGED_KIND, kind=kind,
                recorded=recorded_kind, actual=kind,
            )
        rerun = self.forks[key] is RERUN
        point = DivergencePoint(
            step=key,
            reason=DivergenceReason.RERUN if rerun else DivergenceReason.EDITED,
            kind=kind, recorded=_recorded_match(recorded)[0], actual=match_key,
            ref=_copy(step),
        )
        with self._lock:
            if key in self._applied:
                raise ReplayMismatch(f"step {key} was replayed twice", step=key, kind=kind)
            self._applied.add(key)
            self._consumed.add(key)
            self.divergences.append(point)
        return point

    def edited_outcome(self, key: str) -> Any:
        """The JSON outcome a fork edit gives the call at ``key``."""
        return apply_edit(self.calls[key], self.forks[key])

    def unapplied_forks(self) -> list[str]:
        """Fork steps the run never reached."""
        with self._lock:
            return sorted(set(self.forks) - self._applied)

    def unconsumed(self) -> list[str]:
        with self._lock:
            return sorted(set(self.calls) - self._consumed)

    def skipped(self) -> list[str]:
        """Recorded calls never made that no divergence point explains."""
        with self._lock:
            refs = [p.ref for p in self.divergences if p.ref is not None]
            return sorted(
                key
                for key in set(self.calls) - self._consumed
                if not any(is_downstream(self.calls[key].step, r) for r in refs)
            )

    @property
    def diverged(self) -> bool:
        return bool(self.divergences)


def _copy(step: pb.StepRef) -> pb.StepRef:
    ref = pb.StepRef()
    ref.CopyFrom(step)
    return ref


def apply_edit(recorded: pb.Event, edit: Any) -> Any:
    """The JSON outcome ``edit`` gives a recorded call.

    ``edit`` is the new outcome, or a callable that takes the recorded one
    (None if the call raised) and returns it. For an LLM call, a string
    replaces the text of the first choice and keeps the rest of the recorded
    completion; anything else is the whole completion.
    """
    kind = body_kind(recorded)
    body = getattr(recorded, kind)
    field = {"llm_call": "response", "tool_call": "result", "nondet": "value"}[kind]
    failed = kind != "nondet" and body.HasField("error")
    old = None if failed else load_payload(getattr(body, field))
    value = edit(copy.deepcopy(old)) if callable(edit) else edit
    if kind == "llm_call" and isinstance(value, str):
        if not old or not old.get("choices"):
            raise ValueError(
                f"step {step_key(recorded.step)}: the recorded LLM call has no "
                "completion to put text into; pass a whole completion instead"
            )
        text, value = value, copy.deepcopy(old)
        value["choices"][0]["message"]["content"] = text
    return value


def diverged_event_body(point: DivergencePoint) -> pb.Diverged:
    return pb.Diverged(
        reason=point.reason.proto, kind=point.kind,
        recorded=point.recorded, actual=point.actual,
    )


@contextmanager
def replay(
    source_run_id: str,
    store: EventStore | None = None,
    run_id: str | None = None,
    *,
    strict: bool = True,
    on_divergence: OnDivergence = "raise",
) -> Iterator[Session]:
    """Replay ``source_run_id``: every intercepted call is answered from its log.

    The replay is itself logged as a new run (``run_id``, by default
    ``<source>.replay-<n>``) so it can be compared with the recording.

    With ``on_divergence="raise"`` a call that doesn't match the recording
    raises ReplayMismatch, and (with ``strict``) leaving the block with
    recorded calls that were never made raises ReplayDivergence.

    With ``on_divergence="live"`` the first mismatch and everything
    downstream of it run live instead (see the module docstring), and
    nothing raises. ``session.divergences`` lists where the replay diverged,
    including recorded calls it skipped.
    """
    with replay_session(
        source_run_id, store, run_id,
        mode=Mode.REPLAY, strict=strict, on_divergence=on_divergence,
    ) as session:
        yield session


@contextmanager
def replay_session(
    source_run_id: str,
    store: EventStore | None,
    run_id: str | None,
    *,
    mode: Mode,
    strict: bool = True,
    on_divergence: OnDivergence = "raise",
    forks: Mapping[str, Any] | None = None,
) -> Iterator[Session]:
    """A replay or fork session; ``replay`` and ``fork`` are the public forms."""
    owns_store = store is None
    store = store or EventStore()
    try:
        index = ReplayIndex(
            store.events(source_run_id), on_divergence=on_divergence, forks=forks
        )
        if not index.calls and index.input is None:
            raise KeyError(f"no recorded run {source_run_id!r} in {store.path}")
        if run_id is None:
            prefix = f"{source_run_id}.{mode.value}-"
            n = sum(1 for r in store.run_ids() if r.startswith(prefix))
            run_id = f"{prefix}{n + 1}"
        session = Session(
            store, run_id, mode, source_run_id=source_run_id, replay_index=index
        )
        with activate(session):
            yield session
        _check_exit(index, source_run_id, strict=strict)
    finally:
        if owns_store:
            store.close()


def _check_exit(index: ReplayIndex, source_run_id: str, *, strict: bool) -> None:
    """Report (live) or raise (strict) on recorded calls the run never made."""
    unreached = index.unapplied_forks()
    skipped = index.skipped()
    if index.on_divergence == "live":
        for key in sorted(set(skipped) | set(unreached)):
            rec = index.calls[key]
            index.divergences.append(
                DivergencePoint(
                    step=key, reason=DivergenceReason.SKIPPED_CALL,
                    kind=body_kind(rec), recorded=_recorded_match(rec)[0], actual="",
                )
            )
        return
    if unreached:
        raise ReplayDivergence(
            f"fork of {source_run_id} never reached step {unreached[0]}"
            + (f" (and {len(unreached) - 1} more)" if len(unreached) > 1 else "")
        )
    # Without divergence points (a plain strict replay) skipped() is every
    # unconsumed call; a fork leaves out what runs live downstream of it.
    if strict and skipped:
        raise ReplayDivergence(
            f"replay of {source_run_id} skipped {len(skipped)} recorded call(s), "
            f"first at {skipped[0]}"
        )
