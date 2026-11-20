"""Fork: replay with chosen calls edited or rerun, everything downstream live."""

from __future__ import annotations

from collections import Counter

import pytest

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.session import SessionStats
from ledger.replay.engine import DivergenceReason
from tests.test_divergence import record, tool_graph

TOPIC = {"topic": "fencing tokens"}
PLAN = "/1/plan/(__pregel_pull,plan)#0"
B0 = "/2/b/(__pregel_pull,b)#0"
A1 = "/2/a/(__pregel_pull,a)#1"
C0 = "/2/c/(__pregel_pull,c)#0"
JOIN = "/3/join/(__pregel_pull,join)#0"


def fork(store, run_id, *, graph_kw=None, **kw):
    graph, runs = tool_graph(**(graph_kw or {}))
    with ledger.fork(run_id, store=store, **kw) as session:
        out = graph.invoke(TOPIC)
    return session, out, runs


def test_steps_lists_every_call_in_log_order(store):
    run_id, _ = record(store)
    listed = ledger.steps(run_id, store)
    assert len(listed) == 8
    assert [s.index for s in listed] == sorted(s.index for s in listed)
    assert {s.key for s in listed} >= {PLAN, B0, JOIN}
    assert all(s.kind == "tool_call" and s.label == "work" for s in listed)
    plan = next(s for s in listed if s.key == PLAN)
    assert plan.summary.startswith("plan(fencing tokens)#")
    assert "work" in str(plan) and PLAN in str(plan)
    with pytest.raises(KeyError):
        ledger.steps("nope", store)


def test_rerun_runs_the_step_and_downstream_live(store):
    run_id, out = record(store)
    session, forked, runs = fork(store, run_id, at=B0)
    assert session.run_id == f"{run_id}.fork-1"
    assert runs == Counter({"b": 2, "join": 1})
    assert session.stats == SessionStats(live_calls=3, replayed_calls=5)
    [point] = session.divergences
    assert (point.reason, point.step) == (DivergenceReason.RERUN, B0)
    first = next(iter(store.events(session.run_id)))
    assert first.run_started.mode == pb.MODE_FORK
    assert first.run_started.source_run_id == run_id
    assert forked["plan"] == out["plan"]


def test_edit_replaces_the_outcome(store):
    run_id, out = record(store)
    session, forked, runs = fork(store, run_id, at=B0, edit="EDITED")
    # b's first call is not made; its second, built from the edit, is live.
    assert runs == Counter({"b": 1, "join": 1})
    assert session.stats == SessionStats(live_calls=2, replayed_calls=5, edited=1)
    b_note = next(n for n in forked["notes"] if n.startswith("EDITED"))
    assert b_note.split(" ", 1)[1].startswith("b(second EDITED)#")
    [point] = session.divergences
    assert point.reason is DivergenceReason.EDITED


def test_edit_is_logged_after_a_diverged_event(store):
    run_id, _ = record(store)
    session, _, _ = fork(store, run_id, at=B0, edit="EDITED")
    at_b0 = [ev for ev in store.events(session.run_id) if ev.step.seq == 0 and ev.step.node == "b"]
    assert [body_kind(ev) for ev in at_b0] == ["diverged", "tool_call"]
    diverged, call = at_b0
    assert diverged.diverged.reason == pb.DIVERGENCE_REASON_EDITED
    assert load_payload(call.tool_call.result) == "EDITED"
    assert not call.from_log


def test_callable_edit_gets_the_recorded_outcome(store):
    run_id, out = record(store)
    session, forked, runs = fork(store, run_id, at=PLAN, edit=lambda old: old.upper())
    assert forked["plan"] == out["plan"].upper()
    assert runs == Counter({"a": 2, "b": 2, "c": 2, "join": 1})
    assert session.stats.replayed_calls == 0


def test_step_by_index_or_step_info(store):
    run_id, _ = record(store)
    info = next(s for s in ledger.steps(run_id, store) if s.key == B0)
    for at in (info, info.index):
        _, _, runs = fork(store, run_id, at=at, edit="x")
        assert runs == Counter({"b": 1, "join": 1})


def test_bad_steps(store):
    run_id, _ = record(store)
    with pytest.raises(KeyError, match="no call at step"):
        fork(store, run_id, at="/9/nowhere/()#0")
    run_started_index = 0
    with pytest.raises(KeyError, match="not a call"):
        fork(store, run_id, at=run_started_index)
    with pytest.raises(TypeError):
        fork(store, run_id, at=B0, edits={B0: "x"})
    with pytest.raises(TypeError):
        fork(store, run_id)
    with pytest.raises(ValueError, match="given twice"):
        info = next(s for s in ledger.steps(run_id, store) if s.key == B0)
        fork(store, run_id, edits={B0: "x", info.index: "y"})


def test_several_edits_apply_even_downstream_of_each_other(store):
    run_id, _ = record(store)
    session, forked, runs = fork(store, run_id, edits={PLAN: "new plan", JOIN: "new out"})
    assert forked["out"] == "new out"
    assert runs == Counter({"a": 2, "b": 2, "c": 2})  # join was edited, not run
    assert session.stats.edited == 2
    assert {p.step for p in session.divergences} == {PLAN, JOIN}


def test_a_fork_replays_strictly_like_a_recording(store):
    run_id, _ = record(store)
    session, forked, _ = fork(store, run_id, at=B0, edit="EDITED")
    graph, runs = tool_graph()  # unedited code: the edit is in the fork's log
    with ledger.replay(session.run_id, store) as again:
        assert graph.invoke(TOPIC) == forked
    assert not runs
    assert again.divergences == []


def test_unreached_fork_step(store):
    run_id, _ = record(store)
    # Without a's second call join's request changes too, which strict mode
    # reports first; rerun join so the unreached step is all that's left.
    with pytest.raises(ledger.ReplayMismatch, match="step /3/join/"):
        fork(store, run_id, at=A1, graph_kw={"extra": "-a"})
    with pytest.raises(ledger.ReplayDivergence, match="never reached step .*a"):
        fork(store, run_id, edits={A1: ledger.RERUN, JOIN: ledger.RERUN}, graph_kw={"extra": "-a"})
    session, _, _ = fork(
        store, run_id, at=A1, graph_kw={"extra": "-a"}, on_divergence="live"
    )
    assert (DivergenceReason.SKIPPED_CALL, A1) in {(p.reason, p.step) for p in session.divergences}


def test_prefix_must_match_unless_live(store):
    run_id, _ = record(store)
    # a is a sibling of the fork step c, so it is replayed and must match.
    with pytest.raises(ledger.ReplayMismatch, match="step /2/a/"):
        fork(store, run_id, at=C0, graph_kw={"edits": {"a": "changed"}})
    session, _, runs = fork(
        store, run_id, at=C0, graph_kw={"edits": {"a": "changed"}}, on_divergence="live"
    )
    assert {p.reason for p in session.divergences} == {
        DivergenceReason.RERUN,
        DivergenceReason.CHANGED_REQUEST,
    }
    assert runs == Counter({"a": 2, "c": 2, "join": 1})


def test_llm_text_edit_reaches_the_next_prompt(fake_server, toy_graph, store):
    with ledger.record(store) as rec:
        recorded = toy_graph.invoke(TOPIC)
    web = next(
        s for s in ledger.steps(rec.run_id, store)
        if s.node == "research_web" and s.kind == "llm_call"
    )
    before = fake_server.stats()
    with ledger.fork(rec.run_id, at=web, edit="EDITED NOTE", store=store) as session:
        forked = toy_graph.invoke(TOPIC)

    after = fake_server.stats()
    assert (after["chat"] - before["chat"], after["search"] - before["search"]) == (1, 0)
    assert session.stats == SessionStats(
        live_calls=1, replayed_calls=6, live_nondet=2, replayed_nondet=1, edited=1
    )
    assert "web: EDITED NOTE" in forked["notes"]
    edited = next(
        ev for ev in store.events(session.run_id)
        if body_kind(ev) == "llm_call" and ev.step.node == "research_web"
    )
    completion = load_payload(edited.llm_call.response)
    assert completion["choices"][0]["message"]["content"] == "EDITED NOTE"
    assert edited.llm_call.model == "fake-model"
    write = next(
        ev for ev in store.events(session.run_id)
        if body_kind(ev) == "llm_call" and ev.step.node == "write"
    )
    assert "EDITED NOTE" in load_payload(write.llm_call.request)["messages"][0]["content"]
    assert forked["draft"] != recorded["draft"]


def test_text_edit_of_a_failed_llm_call_needs_a_whole_completion(store):
    from ledger.replay.engine import apply_edit

    ev = pb.Event(step=pb.StepRef(node="n"))
    ev.llm_call.error.CopyFrom(pb.Error(type="X", message="boom"))
    with pytest.raises(ValueError, match="whole completion"):
        apply_edit(ev, "text")
    assert apply_edit(ev, {"choices": []}) == {"choices": []}
