"""Fault injection: one corrupted call per recording, with the step as ground truth."""

from __future__ import annotations

import pytest

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.testing import faults
from tests.test_divergence import tool_graph
from tests.test_fork import B0, TOPIC


def record_with(store, *fs, graph_kw=None):
    graph, runs = tool_graph(**(graph_kw or {}))
    with ledger.record(store) as rec, faults.inject(*fs):
        try:
            out = graph.invoke(TOPIC)
        except Exception as exc:
            out = exc
    return rec.run_id, out, graph


def test_the_fault_fires_once_and_names_its_step(store):
    fault = faults.Fault(node="b", kind="tool_call", corrupt=lambda v: "BAD")
    run_id, out, _ = record_with(store, fault)
    assert fault.step == B0
    assert fault.truth() == {"step": B0, "kind": "tool_call", "node": "b",
                             "superstep": 2, "seq": 0}
    b_note = next(n for n in out["notes"] if n.startswith("BAD"))
    assert "second BAD" in b_note  # b's second call saw the corrupted first one
    assert out["out"].count("BAD") == 2


def test_the_log_looks_like_any_recording(store):
    fault = faults.Fault(node="b", seq=0, corrupt=lambda v: "BAD")
    run_id, out, graph = record_with(store, fault)
    events = list(store.events(run_id))
    assert events[0].run_started.mode == pb.MODE_RECORD
    assert "diverged" not in {body_kind(ev) for ev in events}
    at_b0 = next(ev for ev in events if ev.step.node == "b" and ev.step.seq == 0)
    assert load_payload(at_b0.tool_call.result) == "BAD"
    with ledger.replay(run_id, store) as rep:  # replays strictly, fault included
        assert graph.invoke(TOPIC) == out
    assert rep.stats.live_calls == 0


def test_a_fault_that_matches_nothing_never_fires(store):
    fault = faults.Fault(node="nowhere", corrupt=lambda v: "BAD")
    run_id, out, _ = record_with(store, fault)
    assert not fault.fired and fault.truth() == {"step": None}
    assert "BAD" not in out["out"]


def test_a_raising_fault_is_the_calls_error(store):
    def boom(value):
        raise TimeoutError("search timed out")

    fault = faults.Fault(node="join", corrupt=boom)
    run_id, out, _ = record_with(store, fault)
    assert isinstance(out, TimeoutError)
    at_join = next(ev for ev in store.events(run_id) if ev.step.node == "join")
    assert at_join.tool_call.error.type.endswith("TimeoutError")
    last = list(store.events(run_id))[-1]
    assert last.run_finished.HasField("error")


def test_two_faults_in_one_recording(store):
    a = faults.Fault(node="a", seq=1, corrupt=lambda v: "BAD-a")
    c = faults.Fault(node="c", seq=0, corrupt=lambda v: "BAD-c")
    _, out, _ = record_with(store, a, c)
    assert a.fired and c.fired
    assert "BAD-a" in out["out"] and "BAD-c" in out["out"]


def test_faults_need_a_recording(store):
    fault = faults.Fault(corrupt=lambda v: v)
    with pytest.raises(RuntimeError, match="ledger.record"), faults.inject(fault):
        pass
    with pytest.raises(ValueError, match="kind"):
        faults.Fault(kind="effect", corrupt=lambda v: v)


def test_replace_text_on_an_llm_call(fake_server, toy_graph, store):
    fault = faults.Fault(node="research_docs", kind="llm_call",
                         corrupt=faults.replace_text(lambda old: "WRONG " + old))
    with ledger.record(store) as rec, faults.inject(fault):
        out = toy_graph.invoke(TOPIC)
    docs = next(n for n in out["notes"] if n.startswith("docs:"))
    assert docs.startswith("docs: WRONG [fake-model]")
    assert fault.step.startswith("/2/research_docs/") and fault.step.endswith("#1")
    assert faults.replace_text("x")({"choices": [{"message": {"content": "y"}}]}) == {
        "choices": [{"message": {"content": "x"}}]
    }
