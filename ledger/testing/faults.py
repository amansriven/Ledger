"""Fault injection: corrupt one call of a recording and remember which one.

    fault = faults.Fault(node="research_web", kind="llm_call",
                         corrupt=faults.replace_text("a wrong summary"))
    with ledger.record(store) as rec, faults.inject(fault):
        graph.invoke(inputs)
    fault.step          # key of the call that was corrupted: the ground truth

A fault fires at the first live call that matches every field given
(``node``, ``kind``, ``superstep``, ``seq``, ``key``) and only once. The call
itself runs as usual; ``corrupt`` then gets its outcome (a ChatCompletion for
an LLM call, the tool's result, the clock, uuid or seed value) and returns
the one the graph sees instead. If ``corrupt`` raises, the exception is the
call's outcome, as if the call had failed.

The corrupted outcome is logged as the call's own outcome, so the recording
looks like any other: nothing in the log says a fault was injected, and a
blame method reading it can't find the answer there. The ground truth lives
only in the Fault object (``fault.truth()`` for JSON). Replaying the
recording serves the corrupted outcome again; rerunning that call live (a
fork with ``RERUN``) makes it afresh, without the fault.

Faults apply to recordings only. Effects (``ledger.effect``) can't be
faulted.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from ledger.core.events import pb
from ledger.core.session import Mode, current_session
from ledger.core.store import step_key

__all__ = ["Fault", "inject", "replace_text"]

FAULT_KINDS = ("llm_call", "tool_call", "nondet")


@dataclass
class Fault:
    """One corrupted call. Fields left as None match anything."""

    corrupt: Callable[[Any], Any]
    node: str | None = None
    kind: str | None = None
    superstep: int | None = None
    seq: int | None = None
    key: str | None = None
    # Filled in when the fault fires.
    step: str | None = field(default=None, init=False)
    ref: pb.StepRef | None = field(default=None, init=False, repr=False)
    fired_kind: str | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.kind is not None and self.kind not in FAULT_KINDS:
            raise ValueError(f"kind must be one of {FAULT_KINDS}, not {self.kind!r}")

    @property
    def fired(self) -> bool:
        return self.step is not None

    def matches(self, step: pb.StepRef, kind: str) -> bool:
        return (
            (self.node is None or step.node == self.node)
            and (self.kind is None or kind == self.kind)
            and (self.superstep is None or step.superstep == self.superstep)
            and (self.seq is None or step.seq == self.seq)
            and (self.key is None or step_key(step) == self.key)
        )

    def apply(self, step: pb.StepRef, kind: str, value: Any) -> Any:
        """``value``, corrupted if this is the call the fault is for."""
        with self._lock:
            if self.fired or not self.matches(step, kind):
                return value
            self.step = step_key(step)
            self.ref = pb.StepRef()
            self.ref.CopyFrom(step)
            self.fired_kind = kind
        return self.corrupt(value)

    def truth(self) -> dict[str, Any]:
        """Where the fault fired, as JSON; ``{"step": None}`` if it never did."""
        if self.ref is None:
            return {"step": None}
        return {
            "step": self.step, "kind": self.fired_kind, "node": self.ref.node,
            "superstep": self.ref.superstep, "seq": self.ref.seq,
        }


@contextmanager
def inject(*faults: Fault) -> Iterator[list[Fault]]:
    """Arm ``faults`` on the recording that is active now."""
    session = current_session()
    if session is None or session.mode is not Mode.RECORD:
        raise RuntimeError("faults.inject needs an active ledger.record session")
    previous = session.fault

    def hook(step: pb.StepRef, kind: str, value: Any) -> Any:
        for f in faults:
            value = f.apply(step, kind, value)
        return previous(step, kind, value) if previous else value

    session.fault = hook
    try:
        yield list(faults)
    finally:
        session.fault = previous


def replace_text(new: str | Callable[[str], str]) -> Callable[[Any], Any]:
    """A corrupt function for LLM calls: change the first choice's text.

    ``new`` is the text, or a function of the old text.
    """

    def corrupt(completion: Any) -> Any:
        if isinstance(completion, dict):
            out = copy.deepcopy(completion)
            message = out["choices"][0]["message"]
            old = message.get("content") or ""
            message["content"] = new(old) if callable(new) else new
            return out
        out = completion.model_copy(deep=True)
        old = out.choices[0].message.content or ""
        out.choices[0].message.content = new(old) if callable(new) else new
        return out

    return corrupt
