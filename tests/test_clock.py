from __future__ import annotations

import operator
import uuid
from datetime import UTC, datetime
from typing import Annotated, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

import ledger
from ledger.core.events import body_kind


def test_outside_a_session_they_are_the_stdlib():
    assert isinstance(ledger.time(), float)
    assert isinstance(ledger.now(), datetime)
    assert isinstance(ledger.uuid4(), uuid.UUID)
    assert ledger.uuid4() != ledger.uuid4()
    assert 0 <= ledger.rng().random() < 1


def _read_all():
    return (
        ledger.time(),
        ledger.now(UTC),
        ledger.uuid4(),
        [ledger.rng().random() for _ in range(3)],
        ledger.rng().randint(0, 10**9),
    )


def test_values_replay_exactly(store):
    with ledger.record(store) as rec:
        recorded = _read_all()
    with ledger.replay(rec.run_id, store) as rep:
        replayed = _read_all()
    assert replayed == recorded
    # time, time (inside now), uuid4, one seed for the rng: four reads.
    assert rec.stats.live_nondet == 4
    assert rep.stats.replayed_nondet == 4
    assert rec.stats.live_calls == rep.stats.live_calls == 0

    sources = [e.nondet.source for e in store.events(rec.run_id) if body_kind(e) == "nondet"]
    assert sources == ["time", "time", "uuid4", "rng_seed"]


def test_reading_in_a_different_order_is_a_mismatch(store):
    with ledger.record(store) as rec:
        ledger.time()
        ledger.uuid4()
    with pytest.raises(ledger.ReplayMismatch, match="changed"):
        with ledger.replay(rec.run_id, store):
            ledger.uuid4()


def test_a_skipped_read_is_a_divergence(store):
    with ledger.record(store) as rec:
        ledger.time()
        ledger.time()
    with pytest.raises(ledger.ReplayDivergence, match="skipped 1"):
        with ledger.replay(rec.run_id, store):
            ledger.time()


class S(TypedDict, total=False):
    draws: Annotated[list[tuple[str, float]], operator.add]


def test_each_parallel_task_gets_its_own_replayed_generator(store):
    def branch(tag: str):
        def node(state: S) -> S:
            r = ledger.rng()
            assert ledger.rng() is r
            return {"draws": [(tag, r.random()), (tag, r.random())]}

        return node

    b = StateGraph(S)
    for tag in ("a", "b", "c"):
        b.add_node(tag, branch(tag))
        b.add_edge(START, tag)
        b.add_edge(tag, END)
    graph = ledger.instrument(b.compile())

    with ledger.record(store) as rec:
        recorded = graph.invoke({})
    with ledger.replay(rec.run_id, store) as rep:
        replayed = graph.invoke({})
    assert replayed == recorded
    assert rec.stats.live_nondet == 3  # one seed per task
    assert len({v for _, v in recorded["draws"]}) == 6
