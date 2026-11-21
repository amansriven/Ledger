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
and for none after it, so f can be searched for. The search gallops back from
the end, rerunning the last call, then the last 2, 4, 8 and so on, until a
rerun fixes the run, then bisects inside the last gap. Live calls are what a
counterfactual costs, and the first forks rerun only the end of the run, so
a step d calls from the end takes about 2 log2(d) forks, none of them
rerunning much more than 2d calls. One more fork checks that rerunning f
alone (with what depends on it) is enough, unless every call after f depends
on f, in which case the fork that found f already did. Rerunning everything is only
tried when nothing shorter fixes the run.

**Several bad steps.** If rerunning the first step found alone doesn't fix
the run, another step is needed too: the search goes on over the steps after
it, with the ones found so far always rerun, until rerunning just those
fixes the run. ``diagnosis.steps`` lists them in log order;
``diagnosis.step`` is the first.

**No step.** If rerunning everything still fails, no recorded outcome is to
blame: the cause is in the code or the input, or the failure comes back
whenever its step is redrawn (a model that makes the same mistake every
time). ``diagnosis.step`` is then None.

**Noise.** Each counterfactual runs up to ``repeats`` times and counts as a
fix when more than ``threshold`` of them pass. It stops early once that is
settled, so a fork that passes or fails every time costs about half of
``repeats`` with the default threshold of 0.5 (a majority vote). Each
reports its pass rate with a Wilson interval.

The right threshold depends on where the noise is. A majority vote suits
runs that sometimes pass even with the fault kept. When they essentially
never do, and noise only makes good runs fail (a flaky call later on, a
model that repeats its mistake when redrawn), a real fix can pass less than
half the time; then ``threshold=0``, one pass out of ``repeats``, is right,
and stops at the first pass. Bisection
assumes the answers are consistent; when noise makes them contradict each
other the diagnosis ends ``"inconsistent"`` with the steps found so far.

**Budget.** ``budget`` caps forks, live calls or dollars (see
``ledger.blame.budget``). A diagnosis that runs out ends ``"budget"``: the
steps found so far, and in ``narrowed`` the candidates the step being
searched for must be among.

Effects are not candidates: rerunning one would repeat it. A counterfactual
that would run an effect live raises ``LiveEffectRefused`` unless
``live_effects`` is true.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ledger.blame.budget import Budget, OutOfBudget, Pricing, Spend, usage, wilson
from ledger.core.events import body_kind, load_payload
from ledger.core.store import EventStore, step_key
from ledger.effects import LiveEffectRefused
from ledger.replay.engine import CALL_KINDS, RERUN, is_downstream
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
    # Runs it was allowed (``repeats``); ``trials`` can stop short of it.
    repeats: int = 1
    # It fixes the run when more than this share of ``repeats`` pass.
    threshold: float = 0.5
    tokens_in: int = 0
    tokens_out: int = 0
    dollars: float = 0.0

    @property
    def fixed(self) -> bool:
        return self.passed > self.threshold * self.repeats

    @property
    def rate(self) -> float:
        return self.passed / self.trials if self.trials else 0.0

    @property
    def interval(self) -> tuple[float, float]:
        """95% Wilson interval for the pass rate."""
        return wilson(self.passed, self.trials)


@dataclass
class Diagnosis:
    run_id: str
    # Decisive steps in log order; empty if no rerun fixes the run.
    steps: list[StepInfo]
    # How many recorded calls were candidates.
    candidates: int
    counterfactuals: list[Counterfactual] = field(default_factory=list)
    # "found": rerunning ``steps`` alone fixed the run. "no_step": rerunning
    # everything didn't. "budget": the budget ran out. "inconsistent": noisy
    # forks contradicted each other. "" until the diagnosis ends.
    status: str = ""
    # With "budget": the candidates the step being searched for is among.
    narrowed: list[StepInfo] = field(default_factory=list)
    spent: Spend = field(default_factory=Spend)

    @property
    def complete(self) -> bool:
        return self.status == "found"

    @property
    def step(self) -> StepInfo | None:
        """The first step found or, if the budget ran out before any, the
        first candidate it was narrowed to (a best guess)."""
        if self.steps:
            return self.steps[0]
        return self.narrowed[0] if self.narrowed else None

    @property
    def forks(self) -> int:
        return self.spent.forks

    @property
    def live_calls(self) -> int:
        return self.spent.live_calls

    @property
    def dollars(self) -> float:
        return self.spent.dollars

    def __str__(self) -> str:
        found = ", ".join(s.key for s in self.steps) or "no step"
        return (f"{self.run_id}: {self.status} {found} ({self.forks} forks, "
                f"{self.live_calls} live calls, ${self.dollars:.4f}, "
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
    threshold: float = 0.5,
    budget: Budget | None = None,
    pricing: Pricing | None = None,
    live_effects: bool = False,
) -> Diagnosis:
    """Name the step(s) of the failed run ``run_id`` that caused the failure.

    ``graph`` is the instrumented graph (anything with ``invoke``) or a
    function of the input; it is run with the recording's input unless
    ``inputs`` is given. ``check(output)`` is true for a successful run; for
    the recording it gets the output as logged (JSON). ``kinds`` are the
    call kinds that can be blamed. ``repeats``, ``threshold``, ``budget``
    and ``pricing`` are described in the module docstring. Raises RunNotFailed if the
    recording passes ``check``.
    """
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    if not 0 <= threshold < 1:
        raise ValueError("threshold must be in [0, 1)")
    need = threshold * repeats  # passes must exceed this
    if budget is not None and budget.dollars is not None and pricing is None:
        raise ValueError("a dollar budget needs pricing")
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
            diagnosis.status = "no_step"
            return diagnosis
        spent = diagnosis.spent

        def counterfactual(reran: list[str], label: str) -> Counterfactual:
            passed, run_ids, live, t_in, t_out, cost = 0, [], 0, 0, 0, 0.0
            for trial in range(repeats):
                if passed > need or passed + (repeats - trial) <= need:
                    break  # settled either way
                if budget is not None and budget.exhausted(spent):
                    if trial == 0:
                        raise OutOfBudget
                    break
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
                tin, tout = usage(store, session.run_id)
                dollars = pricing.dollars(tin, tout) if pricing else 0.0
                t_in, t_out, cost = t_in + tin, t_out + tout, cost + dollars
                spent.forks += 1
                spent.live_calls += session.stats.live_calls
                spent.tokens_in += tin
                spent.tokens_out += tout
                spent.dollars += dollars
            c = Counterfactual(
                tuple(reran), label, passed, len(run_ids), tuple(run_ids), live,
                repeats=repeats, threshold=threshold, tokens_in=t_in, tokens_out=t_out, dollars=cost,
            )
            diagnosis.counterfactuals.append(c)
            return c

        keys = [s.key for s in candidates]
        n = len(keys)
        refs = {step_key(ev.step): ev.step for ev in store.events(run_id, hydrate=False)
                if body_kind(ev) in CALL_KINDS}

        def covered(found: list[int], lo: int) -> bool:
            """Whether every candidate after ``lo`` depends on a found step, so
            rerunning the found steps alone reruns what found + keys[lo:] did."""
            return all(any(is_downstream(refs[keys[k]], refs[keys[g]]) for g in found)
                       for k in range(lo + 1, n))

        def from_(i: int, forced: list[int]) -> bool:
            reran = [keys[j] for j in forced] + keys[i:]
            return counterfactual(reran, f"from {keys[i]}").fixed

        found: list[int] = []
        lo, hi = 0, n
        searching = True  # between finding one step and checking it alone: False
        try:
            # Each round finds the last k >= lo with found + keys[k:] fixing
            # the run; found + keys[n:] (the found steps alone, or in the
            # first round the recording itself) is known not to. In later
            # rounds found + keys[lo:] is known to fix it.
            known = False
            while True:
                hi, width = n, 1
                while True:  # gallop back from the end
                    k = max(lo, n - width)
                    if k == lo and known:
                        break
                    if from_(k, found):
                        lo = k
                        break
                    hi = k
                    if k == lo:
                        diagnosis.status = "no_step"
                        return diagnosis
                    width *= 2
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if from_(mid, found):
                        lo = mid
                    else:
                        hi = mid
                found.append(lo)
                searching = False
                reran = [keys[j] for j in found]
                # found + keys[lo:] is known to fix the run; if that reran
                # nothing beyond what the found steps alone rerun, it was
                # already this check.
                if covered(found, lo) or counterfactual(reran, "only " + ", ".join(reran)).fixed:
                    diagnosis.status = "found"
                    break
                lo += 1
                searching, known = True, True
                if lo >= n:
                    diagnosis.status = "inconsistent"
                    break
        except OutOfBudget:
            diagnosis.status = "budget"
            if searching:
                diagnosis.narrowed = candidates[lo:hi]
        diagnosis.steps = [candidates[j] for j in found]
        return diagnosis
    finally:
        if owns_store:
            store.close()
