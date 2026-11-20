"""Fork: replay a recorded run with chosen calls edited or rerun live.

    for s in ledger.steps(run_id):            # pick a step
        print(s)

    with ledger.fork(run_id, at=s.key, edit="a different answer") as session:
        graph.invoke(inputs)                  # same input as the recording

At each fork step the call gets the edited outcome (``edit``) or, with the
default ``edit=ledger.RERUN``, runs live again. Every call downstream of a
fork step runs live, by the rule in ``ledger.replay.engine``; the rest of the
run replays from the recording, so a counterfactual pays only for what it
changes.

``edit`` is the call's new outcome, or a callable that takes the recorded
outcome (None if the call raised) and returns the new one. For an LLM call a
string replaces the first choice's text and keeps the rest of the recorded
completion. The fork's request at that step does not have to match the
recording: the edit replaces the outcome whatever was asked.

Upstream of the fork steps the run must still match its recording, as in a
strict replay: a mismatch raises ReplayMismatch, and a fork step the run
never reaches raises ReplayDivergence. Pass ``on_divergence="live"`` to let
code changes diverge too; ``session.divergences`` then reports both.

The fork is logged as its own run (``<source>.fork-<n>``, mode MODE_FORK).
Its log holds the edited outcomes, so it replays strictly like a recording.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from ledger.core.events import body_kind, load_payload, pb
from ledger.core.session import Mode, Session
from ledger.core.store import EventStore, step_key
from ledger.replay.engine import CALL_KINDS, OUTCOME_FIELD, RERUN, OnDivergence, replay_session

__all__ = ["RERUN", "Step", "StepInfo", "fork", "steps"]


@dataclass(frozen=True)
class StepInfo:
    """One recorded call, as ``steps()`` lists it."""

    index: int  # position of the call's event in the run's log
    key: str  # step key, what ``fork(at=...)`` matches on
    kind: str  # "llm_call", "tool_call", "nondet" or "effect"
    node: str
    superstep: int
    seq: int
    label: str  # model, tool or effect name, or nondet source
    summary: str  # the outcome, shortened
    from_log: bool

    def __str__(self) -> str:
        return f"#{self.index:<4} {self.kind:<9} {self.label:<20} {self.key}  {self.summary}"


# A step: its key, the index of its event in the source run's log, its
# StepRef, or a StepInfo from ``steps()``.
Step = str | int | pb.StepRef | StepInfo


def _summary(event: pb.Event, width: int = 60) -> str:
    kind = body_kind(event)
    body = getattr(event, kind)
    if kind == "effect" and body.phase == pb.EffectCall.PHASE_PENDING:
        text = "in doubt" + (f" after {body.error.type}" if body.HasField("error") else "")
    elif kind != "nondet" and body.HasField("error"):
        text = f"!{body.error.type}: {body.error.message}"
    else:
        field = OUTCOME_FIELD[kind]
        value = load_payload(getattr(body, field))
        if kind == "llm_call" and isinstance(value, dict) and value.get("choices"):
            value = value["choices"][0].get("message", {}).get("content") or ""
        text = value if isinstance(value, str) else repr(value)
    text = " ".join(str(text).split())
    return text if len(text) <= width else text[: width - 1] + "…"


def _label(event: pb.Event) -> str:
    kind = body_kind(event)
    if kind == "llm_call":
        return event.llm_call.model
    if kind == "tool_call":
        return event.tool_call.name
    if kind == "effect":
        return event.effect.name
    return event.nondet.source


def steps(run_id: str, store: EventStore | None = None) -> list[StepInfo]:
    """Every intercepted call of a run, in log order.

    An effect is listed once, at its outcome; one left in doubt is listed at
    its last PENDING record.
    """
    owns_store = store is None
    store = store or EventStore()
    try:
        found: dict[str, StepInfo] = {}
        for ev in store.events(run_id):
            if body_kind(ev) not in CALL_KINDS:
                continue
            found[step_key(ev.step)] = (
                StepInfo(
                    index=ev.index, key=step_key(ev.step), kind=body_kind(ev),
                    node=ev.step.node, superstep=ev.step.superstep, seq=ev.step.seq,
                    label=_label(ev), summary=_summary(ev), from_log=ev.from_log,
                )
            )
        out = sorted(found.values(), key=lambda s: s.index)
        if not out and not any(True for _ in store.events(run_id)):
            raise KeyError(f"no recorded run {run_id!r} in {store.path}")
        return out
    finally:
        if owns_store:
            store.close()


def _resolve(at: Step, by_index: dict[int, str]) -> str:
    if isinstance(at, StepInfo):
        return at.key
    if isinstance(at, pb.StepRef):
        return step_key(at)
    if isinstance(at, bool):
        raise TypeError("a step is a key, a log index, a StepRef or a StepInfo")
    if isinstance(at, int):
        if at not in by_index:
            raise KeyError(f"event #{at} of the recording is not a call")
        return by_index[at]
    if isinstance(at, str):
        return at
    raise TypeError("a step is a key, a log index, a StepRef or a StepInfo")


@contextmanager
def fork(
    source_run_id: str,
    at: Step | None = None,
    edit: Any = RERUN,
    *,
    edits: Mapping[Step, Any] | None = None,
    store: EventStore | None = None,
    run_id: str | None = None,
    on_divergence: OnDivergence = "raise",
    live_effects: bool = False,
) -> Iterator[Session]:
    """Replay ``source_run_id`` with the call at ``at`` edited or rerun live.

    Give one step with ``at`` and ``edit``, or several with ``edits``
    (``{step: edit}``). See the module docstring for what an edit can be.

    An effect downstream of the fork steps, or rerun by one, would run live
    and raises ``LiveEffectRefused`` unless ``live_effects`` is true. Editing
    an effect's outcome never calls it.
    """
    if (at is None) == (edits is None):
        raise TypeError("give either at= (with edit=) or edits=")
    wanted: Mapping[Step, Any] = {at: edit} if edits is None else edits
    if not wanted:
        raise ValueError("a fork needs at least one step")
    owns_store = store is None
    store = store or EventStore()
    try:
        by_index = {s.index: s.key for s in steps(source_run_id, store)}
        forks: dict[str, Any] = {}
        for step, value in wanted.items():
            key = _resolve(step, by_index)
            if key in forks:
                raise ValueError(f"step {key} is given twice")
            forks[key] = value
        with replay_session(
            source_run_id, store, run_id,
            mode=Mode.FORK, on_divergence=on_divergence, forks=forks,
            live_effects=live_effects,
        ) as session:
            yield session
    finally:
        if owns_store:
            store.close()
