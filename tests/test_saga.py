"""Sagas: confirmed effects are compensated, newest first, when a run fails for good."""

from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.store import step_key
from ledger.effects.saga import compensation_step
from tests.conftest import crash_after

CONFIRMED = pb.EffectCall.PHASE_CONFIRMED


class S(TypedDict, total=False):
    done: Annotated[list[Any], operator.add]


class World:
    """An idempotent API: what was booked, and every request with its key."""

    def __init__(self, refuse: set[str] | None = None, lose: dict[str, int] | None = None):
        self.booked: dict[str, int] = {}
        self.requests: list[tuple[str, str]] = []
        self.applied: dict[str, Any] = {}
        self.refuse = refuse or set()
        self.lose = dict(lose or {})  # op -> answers to drop (applied, then lost)

    def call(self, op: str, key: str, apply) -> Any:
        self.requests.append((op, key))
        if op in self.refuse:
            raise ValueError(f"{op} refused")
        if key not in self.applied:
            self.applied[key] = apply()
        if self.lose.get(op, 0) > 0:
            self.lose[op] -= 1
            raise ConnectionError(f"{op}: connection dropped")
        return self.applied[key]


def booking_graph(world: World, *, fail_at_end: bool = True, use_async: bool = False):
    """book hotel -> book car -> note (no compensation) -> pay (refused)."""

    def unbook(booked, item, price, *, idempotency_key):
        return world.call(f"unbook {item}", idempotency_key, lambda: world.booked.pop(item))

    @ledger.effect(name="book", compensate=unbook, backoff=0)
    def book(item, price, *, idempotency_key):
        return world.call(f"book {item}", idempotency_key,
                          lambda: world.booked.setdefault(item, price))

    @ledger.effect(name="note", backoff=0)
    def note(text, *, idempotency_key):
        return world.call("note", idempotency_key, lambda: text)

    @ledger.effect(name="pay", backoff=0)
    def pay(total, *, idempotency_key):
        return world.call("pay", idempotency_key, lambda: total)

    async def aunbook(booked, item, price, *, idempotency_key):
        await asyncio.sleep(0)
        return unbook(booked, item, price, idempotency_key=idempotency_key)

    @ledger.effect(name="book", compensate=aunbook, backoff=0)
    async def abook(item, price, *, idempotency_key):
        await asyncio.sleep(0)
        return book.__wrapped__(item, price, idempotency_key=idempotency_key)

    def trip(state: S) -> S:
        hotel = book("hotel", 300)
        car = book("car", 80)
        note("booked")
        if fail_at_end:
            pay(hotel + car)
        return {"done": [hotel, car]}

    async def atrip(state: S) -> S:
        hotel = await abook("hotel", 300)
        car = await abook("car", 80)
        note("booked")
        if fail_at_end:
            pay(hotel + car)
        return {"done": [hotel, car]}

    b = StateGraph(S)
    b.add_node("trip", atrip if use_async else trip)
    b.add_edge(START, "trip")
    b.add_edge("trip", END)
    return ledger.instrument(b.compile())


def effects(store, run_id):
    return [
        (ev.effect.name, ev.effect.phase, step_key(ev.step))
        for ev in store.events(run_id) if body_kind(ev) == "effect"
    ]


def outcomes(store, run_id):
    """name -> phases, for the outcome events (not PENDING)."""
    return [
        (name, phase) for name, phase, _ in effects(store, run_id)
        if phase != pb.EffectCall.PHASE_PENDING
    ]


def test_a_run_that_fails_for_good_undoes_its_effects_newest_first(store):
    world = World(refuse={"pay"})
    graph = booking_graph(world)
    with ledger.record(store) as rec:
        with pytest.raises(ValueError, match="pay refused"):
            graph.invoke({})

    assert world.booked == {}
    ops = [op for op, _ in world.requests]
    assert ops == ["book hotel", "book car", "note", "pay", "unbook car", "unbook hotel"]
    assert outcomes(store, rec.run_id) == [
        ("book", CONFIRMED), ("book", CONFIRMED), ("note", CONFIRMED),
        ("pay", pb.EffectCall.PHASE_FAILED),
        ("book:compensate", CONFIRMED), ("book:compensate", CONFIRMED),
    ]
    # Each compensation has its own step and key, and knows what it undoes.
    keys = [key for _, key in world.requests]
    assert len(set(keys)) == len(keys)
    comps = [ev for ev in store.events(rec.run_id)
             if body_kind(ev) == "effect" and ev.effect.name == "book:compensate"]
    assert comps[-1].step.node == "trip:compensate"
    assert load_payload(comps[-1].effect.args) == {
        "args": ["hotel", 300], "kwargs": {}, "undoes": 300,
    }
    last = list(store.events(rec.run_id))[-1]
    assert last.run_finished.error.type == "builtins.ValueError"
    assert ledger.resumable_runs(store) == []


def test_a_run_that_succeeds_keeps_its_effects(store):
    world = World()
    with ledger.record(store):
        assert booking_graph(world).invoke({}) == {"done": [300, 80]}
    assert world.booked == {"hotel": 300, "car": 80}


def test_an_effect_in_doubt_is_not_a_failure_for_good(store):
    world = World(lose={"pay": 99})
    with ledger.record(store) as rec:
        with pytest.raises(ledger.EffectInDoubt):
            booking_graph(world).invoke({})
    assert world.booked == {"hotel": 300, "car": 80}  # nothing undone
    assert ledger.resumable_runs(store) == [rec.run_id]


def test_a_refused_compensation_makes_the_saga_incomplete(store):
    world = World(refuse={"pay", "unbook car"})
    with ledger.record(store) as rec:
        with pytest.raises(ledger.SagaIncomplete, match="1 compensation") as info:
            booking_graph(world).invoke({})
    assert isinstance(info.value.__cause__, ValueError)
    assert world.booked == {"car": 80}  # the hotel was still undone
    last = list(store.events(rec.run_id))[-1]
    assert last.run_finished.error.type == "ledger.effects.saga.SagaIncomplete"
    assert ledger.resumable_runs(store) == []  # refused: a person has to look


def test_a_compensation_in_doubt_is_settled_by_resume(store):
    world = World(refuse={"pay"}, lose={"unbook hotel": 3})
    graph = booking_graph(world)
    with ledger.record(store) as rec:
        with pytest.raises(ledger.SagaIncomplete):
            graph.invoke({})
    assert world.booked == {}  # applied, but Ledger never heard back
    assert ledger.resumable_runs(store) == [rec.run_id]

    before = len(world.requests)
    with ledger.resume(rec.run_id, store):
        with pytest.raises(ValueError, match="pay refused"):
            graph.invoke({})
    # Only the compensation in doubt was asked again, with its own key.
    [(op, key)] = world.requests[before:]
    assert op == "unbook hotel" and key == world.requests[-2][1]
    assert ledger.resumable_runs(store) == []


# run_started and 4 effects x (PENDING, outcome) are 9 events; then each
# compensation's PENDING and CONFIRMED (events 10-13) and run_finished (14).
# Crash after each of events 9 to 13.
@pytest.mark.parametrize("n", range(9, 14))
def test_a_crash_during_compensation_is_resumed(store, n):
    world = World(refuse={"pay"})
    graph = booking_graph(world)
    with crash_after(store, n):
        with ledger.record(store) as rec:
            graph.invoke({})
    with ledger.resume(rec.run_id, store):
        with pytest.raises(ValueError, match="pay refused"):
            graph.invoke({})
    assert world.booked == {}
    unbooks = [op for op, _ in world.requests if op.startswith("unbook")]
    # Asked at most once more than once, and only when the answer was lost.
    assert set(unbooks) == {"unbook car", "unbook hotel"} and len(unbooks) <= 3
    assert sorted(outcomes(store, rec.run_id)).count(("book:compensate", CONFIRMED)) == 2
    assert ledger.resumable_runs(store) == []


def test_a_replay_answers_the_compensations_from_the_log(store):
    world = World(refuse={"pay"})
    graph = booking_graph(world)
    with ledger.record(store) as rec:
        with pytest.raises(ValueError):
            graph.invoke({})
    asked = len(world.requests)
    with ledger.replay(rec.run_id, store) as rep:
        with pytest.raises(ValueError, match="pay refused"):
            graph.invoke({})
    assert len(world.requests) == asked
    assert rep.stats.live_calls == 0 and rep.stats.replayed_calls == 6


def test_compensation_steps_are_derived_from_the_effect_step():
    step = pb.StepRef(namespace="", superstep=2, node="refund", path="(x)", seq=1)
    comp = compensation_step(step)
    assert (comp.node, comp.superstep, comp.path, comp.seq) == ("refund:compensate", 2, "(x)", 1)
    assert step.node == "refund"


def test_async_compensations(store):
    world = World(refuse={"pay"})
    graph = booking_graph(world, use_async=True)
    with ledger.record(store):
        with pytest.raises(ValueError):
            asyncio.run(graph.ainvoke({}))
    assert world.booked == {}
    assert [op for op, _ in world.requests][-2:] == ["unbook car", "unbook hotel"]
