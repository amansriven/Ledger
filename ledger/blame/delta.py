"""Blame: find the step that broke a failed run by rerunning versions of it.

    diagnosis = ledger.diagnose(run_id, graph, check=lambda out: out["ok"])
    diagnosis.step        # StepInfo of the decisive step, or None
    diagnosis.forks       # fork runs it took

``check`` says whether a run's output is a success. The failed run is the
recording ``run_id``; each counterfactual is a fork of it (``ledger.fork``)
in which chosen calls run live again (``RERUN``) instead of being replayed,
along with everything downstream of them. The calls before them come from
the log, so a counterfactual pays only for what it reruns. A run that raises
counts as failed.

**Rerunning from step k** reruns every call at or after k in log order. If
that fixes the run and rerunning from the next step doesn't, k's recorded
outcome is needed for the failure: keep it and the run fails even with every
later call redrawn; redraw it too and the run passes. That makes k the
decisive step in Who&When's sense, the earliest step whose correction turns
the failure into a success.

With a single bad step f, rerunning from k fixes the run for every k up to f
and for none after it, so a binary search over the log finds f. It takes one
fork to check that rerunning everything fixes the run, about log2(n) to find
f among n calls, and one more to check that rerunning f alone (with what
depends on it) is enough. The recording itself is the "rerun nothing" end
and costs nothing.

**Several bad steps.** If rerunning the first step found alone doesn't fix
the run, another step is needed too: the search goes on over the steps after
it, with the ones found so far always rerun, until rerunning just those
fixes the run. ``diagnosis.steps`` lists them in log order;
``diagnosis.step`` is the first.

**No step.** If rerunning everything still fails, no recorded outcome is to
blame: the cause is in the code or the input, or the failure comes back
whenever its step is redrawn (a model that makes the same mistake every
time). ``diagnosis.step`` is then None.

Each counterfactual runs ``repeats`` times and counts as a fix when more
than half of them pass.

Effects are not candidates: rerunning one would repeat it. A counterfactual
that would run an effect live raises ``LiveEffectRefused`` unless
``live_effects`` is true.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ledger.core.events import body_kind, load_payload
from ledger.core.store import EventStore
from ledger.effects import LiveEffectRefused
from ledger.replay.engine import RERUN
from ledger.replay.fork import StepInfo, fork, steps

__all__ = ["CANDIDATE_KINDS", "Counterfactual", "Diagnosis", "RunNotFailed", "diagnose"]

CANDIDATE_KINDS = ("llm_call", "tool_call", "nondet")


class RunNotFailed(ValueError):
    """The run passes its check, so there is nothing to blame."""


@dataclass(frozen=True)
class Counterfactual:
    """One counterfactual: the failed run with some calls rerun live."""

    # Keys of the calls given RERUN; whatever depends on them reruns too.
    reran: tuple[str, ...]
    # "from <key>": every call from that step on; "only <keys>": those alone.
    label: str
    passed: int
    trials: int
    run_ids: tuple[str, ...]
    # LLM and tool calls made live, over all trials.
    live_calls: int

    @property
    def fixed(self) -> bool:
        return 2 * self.passed > self.trials


@dataclass
class Diagnosis:
    run_id: str
    # Decisive steps in log order; empty if no rerun fixes the run.
    steps: list[StepInfo]
    # How many recorded calls were candidates.
    candidates: int
    counterfactuals: list[Counterfactual] = field(default_factory=list)
    # Whether rerunning ``steps`` alone was seen to fix the run.
    complete: bool = False

    @property
    def step(self) -> StepInfo | None:
        return self.steps[0] if self.steps else None

    @property
    def forks(self) -> int:
        return sum(c.trials for c in self.counterfactuals)

    @property
    def live_calls(self) -> int:
        return sum(c.live_calls for c in self.counterfactuals)

    def __str__(self) -> str:
        found = ", ".join(s.key for s in self.steps) or "no step"
        return (f"{self.run_id}: {found} ({self.forks} forks, {self.live_calls} live calls, "
                f"{self.candidates} candidates)")


def _recording(store: EventStore, run_id: str) -> tuple[Any, bool, Any]:
    """(input, finished without error, output) of the recorded run."""
    graph_input, ok, output = None, False, None
    for ev in store.events(run_id):
        kind = body_kind(ev)
        if kind == "run_started":
            graph_input = load_payload(ev.run_started.input)
        elif kind == "run_finished":
            ok = ev.run_finished.HasField("output")
            output = load_payload(ev.run_finished.output) if ok else None
    return graph_input, ok, output


def diagnose(
    run_id: str,
    graph: Any,
    check: Callable[[Any], bool],
    *,
    store: EventStore | None = None,
    inputs: Any = None,
    kinds: Sequence[str] = CANDIDATE_KINDS,
    repeats: int = 1,
    live_effects: bool = False,
) -> Diagnosis:
    """Name the step(s) of the failed run ``run_id`` that caused the failure.

    ``graph`` is the instrumented graph (anything with ``invoke``) or a
    function of the input; it is run with the recording's input unless
    ``inputs`` is given. ``check(output)`` is true for a successful run; for
    the recording it gets the output as logged (JSON). ``kinds`` are the
    call kinds that can be blamed. Raises RunNotFailed if the recording
    passes ``check``.
    """
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    invoke = graph.invoke if hasattr(graph, "invoke") else graph
    owns_store = store is None
    store = store or EventStore()
    try:
        candidates = [s for s in steps(run_id, store) if s.kind in kinds]
        recorded_input, finished, output = _recording(store, run_id)
        if inputs is None:
            inputs = recorded_input
        if finished and check(output):
            raise RunNotFailed(f"run {run_id} passes its check: nothing to blame")
        diagnosis = Diagnosis(run_id, [], len(candidates))
        if not candidates:
            return diagnosis

        def counterfactual(reran: list[str], label: str) -> Counterfactual:
            passed, run_ids, live = 0, [], 0
            for _ in range(repeats):
                with fork(
                    run_id, edits={k: RERUN for k in reran}, store=store,
                    on_divergence="live", live_effects=live_effects,
                ) as session:
                    try:
                        out, ok = invoke(inputs), True
                    except LiveEffectRefused:
                        raise
                    except Exception:
                        out, ok = None, False
                passed += ok and bool(check(out))
                run_ids.append(session.run_id)
                live += session.stats.live_calls
            c = Counterfactual(tuple(reran), label, passed, repeats, tuple(run_ids), live)
            diagnosis.counterfactuals.append(c)
            return c

        keys = [s.key for s in candidates]

        def from_(i: int, forced: list[int]) -> bool:
            reran = [keys[j] for j in forced] + keys[i:]
            return counterfactual(reran, f"from {keys[i]}").fixed

        if not from_(0, []):
            return diagnosis
        found: list[int] = []
        lo, n = 0, len(candidates)
        # Invariant: rerunning found + keys[lo:] fixes the run; found + keys[hi:]
        # doesn't (hi == n: the found steps alone, or the recording itself).
        while True:
            hi = n
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if from_(mid, found):
                    lo = mid
                else:
                    hi = mid
            found.append(lo)
            reran = [keys[j] for j in found]
            if counterfactual(reran, "only " + ", ".join(reran)).fixed:
                diagnosis.complete = True
                break
            lo += 1
            if lo >= n:
                break  # contradicts an earlier fork: the runs are noisy
        diagnosis.steps = [candidates[j] for j in found]
        return diagnosis
    finally:
        if owns_store:
            store.close()
