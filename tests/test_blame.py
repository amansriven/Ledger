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
    # Rerun everything, bisect over 8 calls, then the step alone.
    assert d.forks <= 1 + math.ceil(math.log2(8)) + 1
    assert d.counterfactuals[0].label.startswith("from /1/plan/")
    assert d.counterfactuals[-1].label == f"only {fault.step}"


def test_each_fork_replays_the_prefix(store):
    fault = faults.Fault(node="join", corrupt=lambda v: "BAD")
    run_id, graph, runs = record_faulty(store, fault)
    runs.clear()
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert d.step.node == "join"
    # Only the first fork reran everything; the rest replayed most calls.
    assert d.counterfactuals[0].live_calls == 8
    assert d.live_calls < 8 * d.forks
    assert sum(runs.values()) == d.live_calls
    assert all(r.startswith(f"{run_id}.fork-") for c in d.counterfactuals for r in c.run_ids)


def test_two_faults_are_both_needed(store):
    a = faults.Fault(node="a", seq=1, corrupt=lambda v: "BAD")
    c = faults.Fault(node="c", seq=0, corrupt=lambda v: "BAD")
    run_id, graph, _ = record_faulty(store, a, c)
    d = ledger.diagnose(run_id, graph, no_bad, store=store)
    assert {s.key for s in d.steps} == {a.step, c.step}
    assert d.step.index == min(s.index for s in d.steps)  # the earlier one first
    assert d.complete
    assert not d.counterfactuals[-2].fixed  # the first step alone wasn't enough


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
    assert d.forks == 1


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
    assert all(c.trials == 3 and len(c.run_ids) == 3 for c in d.counterfactuals)
    assert d.forks == 3 * len(d.counterfactuals)
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
    assert d.forks <= 1 + math.ceil(math.log2(11)) + 1
