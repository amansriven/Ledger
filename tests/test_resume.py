"""ledger.resume: crash a run partway, resume it in the same run, check it."""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

import ledger
from examples import refund_agent
from ledger import cli
from ledger.core.events import body_kind, pb
from ledger.core.store import step_key
from tests.conftest import crash_after, make_toy_graph, replays_strictly


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr("ledger.effects.manager._delay", lambda backoff, attempt: 0)


def ticket(charge):
    return {"ticket_id": "T-9", "charge_id": charge["id"], "message": "twice", "reason": "duplicate"}


class Out(TypedDict, total=False):
    out: Any


def one_node(fn):
    """A graph whose single node calls ``fn``; resume needs an instrumented run."""
    b = StateGraph(Out)
    b.add_node("pay", lambda s: {"out": fn()})
    b.add_edge(START, "pay")
    b.add_edge("pay", END)
    return ledger.instrument(b.compile())


def test_without_reconcile_the_effect_is_called_again_with_its_key(store):
    keys: list[str] = []
    applied: dict[str, int] = {}

    @ledger.effect(name="pay", backoff=0)
    def pay(amount, *, idempotency_key):
        keys.append(idempotency_key)
        applied.setdefault(idempotency_key, amount)  # an idempotent API
        return {"paid": applied[idempotency_key]}

    graph = one_node(lambda: pay(5))
    with crash_after(store, 2):  # run_started and PENDING are logged, the outcome isn't
        with ledger.record(store) as rec:
            graph.invoke({})
    with ledger.resume(rec.run_id, store) as res:
        assert graph.invoke({}) == {"out": {"paid": 5}}
    assert len(keys) == 2 and keys[0] == keys[1]
    assert len(applied) == 1
    settled = [ev.effect for ev in store.events(rec.run_id) if body_kind(ev) == "effect"][-1]
    assert settled.phase == pb.EffectCall.PHASE_CONFIRMED and settled.attempts == 1
    assert not settled.reconciled
    assert res.stats.live_calls == 1


def test_reconcile_finding_nothing_falls_back_to_calling(store):
    calls = []

    def find(amount, *, idempotency_key):
        return None

    @ledger.effect(name="pay", backoff=0, reconcile=find)
    def pay(amount, *, idempotency_key):
        calls.append(idempotency_key)
        return amount

    graph = one_node(lambda: pay(3))
    with crash_after(store, 2):
        with ledger.record(store) as rec:
            graph.invoke({})
    with ledger.resume(rec.run_id, store):
        assert graph.invoke({}) == {"out": 3}
    assert len(calls) == 2


def test_a_crash_during_the_resume_is_resumed_again(agent, stripe_server, store):
    charge = stripe_server.create_charge(1200)
    with crash_after(store, 2):
        with ledger.record(store) as rec:
            agent.invoke(ticket(charge))
    with crash_after(store, 2):  # run_resumed, the claim's PENDING; dies before CONFIRMED
        with ledger.resume(rec.run_id, store):
            agent.invoke(ticket(charge))
    with ledger.resume(rec.run_id, store) as again:
        out = agent.invoke(ticket(charge))
    assert again.resume_attempt == 2
    assert len(stripe_server.refunds_of(charge["id"])) == 1
    replays_strictly(store, rec.run_id, agent, out)


@pytest.mark.parametrize("n", [1, 3, 6, 9, 12])
def test_parallel_branches_resume_from_any_point(fake_server, store, n):
    graph = make_toy_graph(fake_server.base_url)
    with crash_after(store, n):
        with ledger.record(store) as rec:
            graph.invoke({"topic": "fencing tokens"})
    prefix = {step_key(e.step): e for e in store.events(rec.run_id) if e.HasField("step")}

    with ledger.resume(rec.run_id, store) as res:
        out = graph.invoke({"topic": "fencing tokens"})
    final = {step_key(e.step): e for e in store.events(rec.run_id) if e.HasField("step")}
    assert all(final[k] == e for k, e in prefix.items())  # logged calls kept their answers
    assert res.stats.replayed_calls == sum(
        body_kind(e) in ("llm_call", "tool_call") for e in prefix.values()
    )
    assert res.stats.replayed_calls + res.stats.live_calls == 8
    replays_strictly(store, rec.run_id, graph, out)


def test_what_cannot_be_resumed(agent, stripe_server, store):
    charge = stripe_server.create_charge(100)
    with ledger.record(store) as rec:
        agent.invoke(ticket(charge))
    with pytest.raises(ledger.RunAlreadyFinished):
        with ledger.resume(rec.run_id, store):
            pass
    with ledger.replay(rec.run_id, store) as rep:
        agent.invoke(ticket(charge))
    with pytest.raises(ValueError, match="only recordings"):
        with ledger.resume(rep.run_id, store):
            pass
    with pytest.raises(KeyError):
        with ledger.resume("nope", store):
            pass


def test_resume_needs_the_same_input(agent, stripe_server, store):
    charge = stripe_server.create_charge(100)
    with crash_after(store, 2):
        with ledger.record(store) as rec:
            agent.invoke(ticket(charge))
    with pytest.raises(ValueError, match="input the run started with"):
        with ledger.resume(rec.run_id, store):
            agent.invoke({**ticket(charge), "message": "something else"})


def test_cli_resumes_every_unfinished_run(fake_server, stripe_server, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("STRIPE_API_BASE", stripe_server.base_url)
    monkeypatch.setenv("OPENAI_BASE_URL", f"{fake_server.base_url}/v1")
    db = str(tmp_path / "cli.db")
    graph = refund_agent.graph_from_env()
    charges = [stripe_server.create_charge(300) for _ in range(2)]
    with ledger.EventStore(db) as store:
        for charge, n in zip(charges, (3, 5), strict=True):
            with crash_after(store, n):
                with ledger.record(store) as rec:
                    rec.graph_ref = "examples.refund_agent:graph_from_env"
                    graph.invoke(ticket(charge))

    assert cli.main(["--db", db, "resume", "--all"]) == 0
    out = capsys.readouterr().out
    assert out.count("OK ") == 2 and "resume 1" in out
    assert cli.main(["--db", db, "resume", "--all"]) == 0
    assert "nothing to resume" in capsys.readouterr().out
    for charge in charges:
        assert len(stripe_server.refunds_of(charge["id"])) == 1
    cli.main(["--db", db, "runs"])
    assert capsys.readouterr().out.count("ok  calls=4 live=4 resumed=1") == 2
