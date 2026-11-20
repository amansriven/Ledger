"""Divergence detection: replay with on_divergence="live" goes live from the
first mismatch, and only downstream of it."""

from __future__ import annotations

import operator
import random
import time
from collections import Counter
from typing import Annotated, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.session import SessionStats
from ledger.replay.engine import DivergenceReason, is_downstream
from tests.conftest import make_toy_graph

BRANCHES = ("a", "b", "c")


class S(TypedDict, total=False):
    topic: str
    plan: str
    notes: Annotated[list[str], operator.add]
    out: str


def tool_graph(edits: dict[str, str] | None = None, *, jitter: bool = False, extra: str = ""):
    """plan -> (a | b | c) -> join, every step a local tool call.

    ``edits`` replaces the argument a node passes to its tool, by node name.
    ``extra`` names a node that makes one more tool call than usual, or, with
    a leading "-", one fewer. Returns (graph, executions counter).
    """
    edits = edits or {}
    runs: Counter[str] = Counter()

    @ledger.tool(name="work")
    def work(node: str, arg: str) -> str:
        runs[node] += 1
        return f"{node}({arg})#{random.randint(0, 10**9)}"

    def plan(state: S) -> S:
        return {"plan": work("plan", edits.get("plan", state["topic"]))}

    def branch(name: str):
        def node(state: S) -> S:
            if jitter:
                time.sleep(random.uniform(0, 0.01))
            first = work(name, edits.get(name, state["plan"]))
            notes = [first]
            if extra != f"-{name}":
                notes.append(work(name, f"second {first}"))
            if extra == name:
                notes.append(work(name, "one more"))
            return {"notes": [" ".join(notes)]}

        return node

    def join(state: S) -> S:
        return {"out": work("join", edits.get("join", "|".join(sorted(state["notes"]))))}

    b = StateGraph(S)
    b.add_node("plan", plan)
    b.add_node("join", join)
    b.add_edge(START, "plan")
    for name in BRANCHES:
        b.add_node(name, branch(name))
        b.add_edge("plan", name)
        b.add_edge(name, "join")
    b.add_edge("join", END)
    return ledger.instrument(b.compile()), runs


def record(store, **kw):
    graph, runs = tool_graph(**kw)
    with ledger.record(store) as rec:
        out = graph.invoke({"topic": "fencing tokens"})
    return rec.run_id, out


def replay_live(store, run_id, **kw):
    graph, runs = tool_graph(**kw)
    with ledger.replay(run_id, store, on_divergence="live") as rep:
        out = graph.invoke({"topic": "fencing tokens"})
    return rep, out, runs


def calls(store, run_id):
    """(node, from_log) of every call event in a run's log."""
    return [
        (ev.step.node, ev.from_log)
        for ev in store.events(run_id)
        if body_kind(ev) in ("llm_call", "tool_call", "nondet")
    ]


def test_unchanged_replay_has_no_divergence(store):
    run_id, out = record(store)
    rep, replayed, runs = replay_live(store, run_id)
    assert replayed == out
    assert rep.divergences == []
    assert not runs  # nothing ran live
    assert rep.stats == SessionStats(live_calls=0, replayed_calls=8)
    assert all(from_log for _, from_log in calls(store, rep.run_id))


def test_edited_branch_goes_live_and_its_siblings_keep_replaying(store):
    run_id, out = record(store)
    rep, replayed, runs = replay_live(store, run_id, edits={"b": "edited"})

    [point] = rep.divergences
    assert point.reason is DivergenceReason.CHANGED_REQUEST
    assert point.step == "/2/b/(__pregel_pull,b)#0"
    assert point.kind == "tool_call"
    # b's two calls and join (a later superstep) ran; plan, a and c did not.
    assert runs == Counter({"b": 2, "join": 1})
    assert rep.stats == SessionStats(live_calls=3, replayed_calls=5)
    assert replayed["out"] != out["out"]
    assert replayed["plan"] == out["plan"]


def test_which_calls_go_live_does_not_depend_on_branch_timing(store):
    run_id, _ = record(store, jitter=True)
    seen = set()
    for _ in range(8):
        rep, _, runs = replay_live(store, run_id, edits={"a": "edited"}, jitter=True)
        seen.add((frozenset(runs.items()), tuple(sorted(calls(store, rep.run_id)))))
    assert len(seen) == 1
    [(live, _)] = seen
    assert dict(live) == {"a": 2, "join": 1}


def test_two_edited_branches_are_two_divergence_points(store):
    run_id, _ = record(store)
    rep, _, runs = replay_live(store, run_id, edits={"a": "x", "c": "y"})
    assert sorted(p.step.split("/")[2] for p in rep.divergences) == ["a", "c"]
    assert runs == Counter({"a": 2, "c": 2, "join": 1})


def test_divergence_in_the_first_step_runs_everything_live(store):
    run_id, _ = record(store)
    rep, _, runs = replay_live(store, run_id, edits={"plan": "something else"})
    assert [p.step for p in rep.divergences] == ["/1/plan/(__pregel_pull,plan)#0"]
    assert runs == Counter({"plan": 1, "a": 2, "b": 2, "c": 2, "join": 1})
    assert rep.stats.replayed_calls == 0


def test_later_calls_of_the_diverged_task_go_live_even_if_they_match(store):
    run_id, _ = record(store)
    # join's request depends on every note, so only edit join itself: one call.
    rep, _, runs = replay_live(store, run_id, edits={"join": "edited"})
    assert runs == Counter({"join": 1})
    assert rep.stats == SessionStats(live_calls=1, replayed_calls=7)

    # b's first call matches, its second (built from the first's live,
    # random result) would not; both of b's calls from #0 on are live.
    rep, _, runs = replay_live(store, run_id, edits={"b": "edited"})
    assert [e for e in calls(store, rep.run_id) if e[0] == "b"] == [("b", False), ("b", False)]


def test_extra_call_is_an_unrecorded_divergence(store):
    run_id, _ = record(store)
    rep, _, runs = replay_live(store, run_id, extra="c")
    [point] = rep.divergences
    assert point.reason is DivergenceReason.UNRECORDED_CALL
    assert point.step == "/2/c/(__pregel_pull,c)#2"
    assert point.recorded == ""
    # c's two recorded calls replay, the extra one and join run live.
    assert runs == Counter({"c": 1, "join": 1})


def test_skipped_call_is_reported_not_raised(store):
    run_id, _ = record(store)
    rep, _, runs = replay_live(store, run_id, extra="-a")
    # a's notes change, so join's request changes and goes live; a's
    # missing second call is reported as skipped.
    reasons = {(p.reason, p.step.split("/")[2]) for p in rep.divergences}
    assert reasons == {
        (DivergenceReason.CHANGED_REQUEST, "join"),
        (DivergenceReason.SKIPPED_CALL, "a"),
    }
    assert runs == Counter({"join": 1})


def test_changed_kind_is_a_divergence(store):
    llm_like = []

    @ledger.tool(name="t")
    def t(x: int) -> int:
        llm_like.append(x)
        return x

    with ledger.record(store) as rec:
        t(1)
        ledger.time()
    with ledger.replay(rec.run_id, store, on_divergence="live") as rep:
        t(1)
        t(2)  # recorded a nondet read at this step
    [point] = rep.divergences
    assert point.reason is DivergenceReason.CHANGED_KIND
    assert (point.recorded, point.actual) == ("nondet", "tool_call")
    assert llm_like == [1, 2]


def test_diverged_event_precedes_the_live_call(store):
    run_id, _ = record(store)
    rep, _, _ = replay_live(store, run_id, edits={"b": "edited"})
    events = list(store.events(rep.run_id))
    i = next(i for i, ev in enumerate(events) if body_kind(ev) == "diverged")
    diverged = events[i]
    # Siblings run in parallel and may log in between; take the next event
    # at the same step.
    live = next(ev for ev in events[i + 1 :] if ev.step == diverged.step)
    assert diverged.diverged.reason == pb.DIVERGENCE_REASON_CHANGED_REQUEST
    assert diverged.diverged.kind == "tool_call"
    assert diverged.diverged.recorded and diverged.diverged.actual
    assert diverged.diverged.recorded != diverged.diverged.actual
    assert body_kind(live) == "tool_call" and not live.from_log
    assert live.step == diverged.step
    assert sum(body_kind(ev) == "diverged" for ev in events) == 1


def test_a_diverged_replay_replays_strictly(store):
    """The replay's own log is a complete recording of what it did."""
    run_id, _ = record(store)
    rep, out, _ = replay_live(store, run_id, edits={"b": "edited"})
    graph, runs = tool_graph(edits={"b": "edited"})
    with ledger.replay(rep.run_id, store) as again:
        assert graph.invoke({"topic": "fencing tokens"}) == out
    assert not runs
    assert again.divergences == []


def test_strict_replay_still_raises(store):
    run_id, _ = record(store)
    graph, _ = tool_graph(edits={"b": "edited"})
    with pytest.raises(ledger.ReplayMismatch) as exc:
        with ledger.replay(run_id, store):
            graph.invoke({"topic": "fencing tokens"})
    assert exc.value.reason is DivergenceReason.CHANGED_REQUEST


def test_bad_on_divergence_value(store):
    run_id, _ = record(store)
    with pytest.raises(ValueError, match="on_divergence"):
        with ledger.replay(run_id, store, on_divergence="ignore"):  # type: ignore[arg-type]
            pass


def test_edited_model_runs_the_toy_pipeline_live_from_plan(fake_server, toy_graph, store):
    with ledger.record(store) as rec:
        recorded = toy_graph.invoke({"topic": "fencing tokens"})
    before = fake_server.stats()
    edited = make_toy_graph(fake_server.base_url, model="another-model")
    with ledger.replay(rec.run_id, store, on_divergence="live") as rep:
        replayed = edited.invoke(load_payload(rep.replay_index.input))
    # plan's rng seed comes from the log; its LLM call is the divergence.
    [point] = rep.divergences
    assert point.step == "/1/plan/(__pregel_pull,plan)#1"
    assert rep.stats == SessionStats(
        live_calls=8, replayed_calls=0, live_nondet=2, replayed_nondet=1
    )
    after = fake_server.stats()
    assert (after["chat"] - before["chat"], after["search"] - before["search"]) == (5, 3)
    assert replayed["draft"] != recorded["draft"]


def _ref(ns="", step=1, node="n", path="p", seq=0):
    return pb.StepRef(namespace=ns, superstep=step, node=node, path=path, seq=seq)


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (_ref(seq=0), True),  # the divergent call itself
        (_ref(seq=3), True),  # later in the same task
        (_ref(step=2, node="m"), True),  # later superstep
        (_ref(node="m"), False),  # parallel task, same superstep
        (_ref(path="q"), False),  # same node, different task (Send)
        (_ref(step=0), False),  # earlier superstep
        (_ref(ns="sub", step=0), True),  # other namespace: conservative
    ],
)
def test_is_downstream(call, expected):
    assert is_downstream(call, _ref(seq=0)) is expected


def test_is_downstream_earlier_seq_in_same_task():
    assert is_downstream(_ref(seq=1), _ref(seq=2)) is False
