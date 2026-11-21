"""Blame: bisection over forked replays finds the injected fault."""

from __future__ import annotations

import math

import pytest

import ledger
from ledger.testing import faults
from tests.test_divergence import tool_graph
from tests.test_fork import TOPIC

# The 8 calls of tool_graph: plan, two per branch, join.
CALLS = [("plan", 0), ("a", 0), ("a", 1), ("b", 0), ("b", 1), ("c", 0), ("c", 1), ("join", 0)]


def no_bad(out) -> bool:
    return "BAD" not in out["out"]


def record_faulty(store, *fs, graph_kw=None):
    graph, runs = tool_graph(**(graph_kw or {}))
    with ledger.record(store) as rec, faults.inject(*fs):
        try:
            graph.invoke(TOPIC)
        except Exception:
            pass
    return rec.run_id, graph, runs


@pytest.mark.parametrize("node,seq", CALLS)
def test_a_single_fault_is_found_wherever_it_is(store, node, seq):
    fault = faults.Fault(node=node, seq=seq, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, fault)
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert [s.key for s in d.steps] == [fault.step]
    assert d.complete and d.candidates == 8
    # Gallop back from the end, bisect the last gap, then the step alone.
    assert d.forks <= 2 * math.ceil(math.log2(8)) + 1
    assert d.counterfactuals[0].label.startswith("from /3/join/")  # the last call only


def test_each_fork_replays_the_prefix(store):
    fault = faults.Fault(node="join", corrupt=lambda v: "BAD")
    run_id, graph, runs = record_faulty(store, fault)
    runs.clear()
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert d.step.node == "join"
    # Rerunning the last call fixes it; nothing comes after it, so that was
    # also the check that it alone is enough. The other 7 calls are replayed.
    assert (d.forks, d.live_calls) == (1, 1)
    assert sum(runs.values()) == d.live_calls == runs["join"]
    assert all(r.startswith(f"{run_id}.fork-") for c in d.counterfactuals for r in c.run_ids)


def test_two_faults_are_both_needed(store):
    a = faults.Fault(node="a", seq=1, corrupt=lambda v: "BAD")
    c = faults.Fault(node="c", seq=0, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, a, c)
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert {s.key for s in d.steps} == {a.step, c.step}
    assert d.step.index == min(s.index for s in d.steps)  # the earlier one first
    assert d.complete
    alone = [c for c in d.counterfactuals if c.label.startswith("only ")]
    assert not alone[0].fixed  # the first step alone wasn't enough


def test_a_fault_downstream_of_another_is_covered_by_it(store):
    # Rerunning plan reruns everything after it, so plan alone is enough.
    p = faults.Fault(node="plan", corrupt=lambda v: "BAD")
    b = faults.Fault(node="b", seq=1, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, p, b)
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert [s.node for s in d.steps] == ["plan"] and d.complete


def test_a_failure_in_the_code_has_no_step(store):
    # Node b always passes BAD to its tool: no rerun can fix that.
    run_id, graph, _ = record_faulty(store, graph_kw={"edits": {"b": "BAD"}})
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert d.step is None and d.steps == [] and not d.complete
    assert d.status == "no_step"
    # Rerun the last 1, 2, 4 calls, then everything.
    assert [len(c.reran) for c in d.counterfactuals] == [1, 2, 4, 8]


def test_a_raised_error_is_a_failure(store):
    def boom(value):
        raise TimeoutError("timed out")

    fault = faults.Fault(node="b", seq=1, corrupt=boom)
    run_id, graph, _ = record_faulty(store, fault)
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert [s.key for s in d.steps] == [fault.step]


def test_a_passing_run_has_nothing_to_blame(store):
    run_id, graph, _ = record_faulty(store)
    with pytest.raises(ledger.RunNotFailed):
        ledger.diagnose(run_id, graph, no_bad, store=store)


def test_repeats_run_each_counterfactual_several_times(store):
    fault = faults.Fault(node="c", seq=1, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, fault)
    d = ledger.diagnose(run_id, graph, no_bad, store=store, repeats=3)
    assert d.step.key == fault.step
    # Every fork here passes or fails every time: 2 of 3 settle the majority.
    assert all(c.trials == 2 and len(c.run_ids) == 2 for c in d.counterfactuals)
    assert all(c.rate in (0.0, 1.0) and c.repeats == 3 for c in d.counterfactuals)
    assert d.forks == 2 * len(d.counterfactuals)
    with pytest.raises(ValueError):
        ledger.diagnose(run_id, graph, no_bad, store=store, repeats=0)


def test_a_plain_function_works_as_the_graph(store):
    fault = faults.Fault(node="a", seq=0, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, fault)
    d = ledger.diagnose(run_id, lambda inputs: graph.invoke(inputs), no_bad, store=store)
    assert d.step.key == fault.step


@pytest.mark.parametrize("node", ["plan", "research_web", "research_code", "write"])
def test_an_llm_fault_in_the_toy_pipeline(fake_server, toy_graph, store, node):
    fault = faults.Fault(node=node, kind="llm_call",
                         corrupt=faults.replace_text("CORRUPT"))
    with ledger.record(store) as rec, faults.inject(fault):
        toy_graph.invoke(TOPIC)

    def check(out) -> bool:
        return not any("CORRUPT" in v for v in [out["plan"], out["draft"], *out["notes"]])

    d = ledger.diagnose(rec.run_id, toy_graph, check, store=store)
    assert [s.key for s in d.steps] == [fault.step]
    assert d.step.kind == "llm_call" and d.complete
    # 11 candidates: 5 LLM calls, 3 searches, 3 clock, uuid and rng reads.
    assert d.candidates == 11
    assert d.forks <= 2 * math.ceil(math.log2(11)) + 1


def flaky_graph(fail_rate: float, seed: int = 0):
    """tool_graph whose join tool spuriously returns BAD at ``fail_rate``."""
    import random

    rnd = random.Random(seed)
    graph, runs = tool_graph()
    inner = graph.invoke

    def invoke(inputs):
        out = inner(inputs)
        if rnd.random() < fail_rate:
            out = {**out, "out": out["out"] + " BAD"}
        return out

    return invoke


def test_a_budget_stops_the_search_and_narrows_it(store):
    fault = faults.Fault(node="a", seq=0, corrupt=lambda v: "BAD")  # early: a long search
    run_id, graph, _ = record_faulty(store, fault)
    d = ledger.diagnose(run_id, graph, no_bad, store=store,
                        budget=ledger.blame.Budget(forks=2))
    assert d.status == "budget" and d.forks == 2 and d.steps == []
    assert fault.step in [s.key for s in d.narrowed]
    assert 1 < len(d.narrowed) < 8
    assert d.step == d.narrowed[0]  # best guess
    # Out of budget before the first fork: nothing is narrowed out.
    d0 = ledger.diagnose(run_id, graph, no_bad, store=store,
                         budget=ledger.blame.Budget(forks=0))
    assert d0.status == "budget" and len(d0.narrowed) == 8
    # Enough budget: the same answer as without one.
    d9 = ledger.diagnose(run_id, graph, no_bad, store=store,
                         budget=ledger.blame.Budget(forks=9, live_calls=100))
    assert d9.status == "found" and d9.step.key == fault.step


def test_dollars_come_from_token_usage(fake_server, toy_graph, store):
    fault = faults.Fault(node="write", kind="llm_call", corrupt=faults.replace_text("CORRUPT"))
    with ledger.record(store) as rec, faults.inject(fault):
        toy_graph.invoke(TOPIC)
    pricing = ledger.blame.Pricing(input_per_mtok=1.0, output_per_mtok=2.0)
    d = ledger.diagnose(rec.run_id, toy_graph, lambda out: "CORRUPT" not in out["draft"],
                        store=store, pricing=pricing)
    assert d.step.node == "write"
    spent = d.spent
    assert spent.tokens_in > 0 and spent.tokens_out > 0
    assert spent.dollars == pytest.approx((spent.tokens_in + 2 * spent.tokens_out) / 1e6)
    assert spent.dollars == pytest.approx(sum(c.dollars for c in d.counterfactuals))
    with pytest.raises(ValueError, match="pricing"):
        ledger.diagnose(rec.run_id, toy_graph, bool, store=store,
                        budget=ledger.blame.Budget(dollars=1.0))


def test_repeats_outvote_spurious_failures(store):
    fault = faults.Fault(node="b", seq=0, corrupt=lambda v: "BAD")
    run_id, _, _ = record_faulty(store, fault)
    # One fork in five fails for no reason. A single run per fork is misled
    # some of the time; five, with the majority, almost never.
    hits1 = hits5 = 0
    for seed in range(20):
        d1 = ledger.diagnose(run_id, flaky_graph(0.2, seed), no_bad, store=store)
        d5 = ledger.diagnose(run_id, flaky_graph(0.2, seed), no_bad, store=store, repeats=5)
        hits1 += d1.step is not None and d1.step.key == fault.step
        hits5 += d5.step is not None and d5.step.key == fault.step
    assert hits5 >= 18 and hits5 > hits1


def test_wilson_interval():
    from ledger.blame import wilson

    assert wilson(0, 0) == (0.0, 1.0)
    lo, hi = wilson(5, 5)
    assert hi == 1.0 and 0.5 < lo < 0.6
    lo, hi = wilson(50, 100)
    assert lo == pytest.approx(0.4038, abs=1e-3) and hi == pytest.approx(0.5962, abs=1e-3)
