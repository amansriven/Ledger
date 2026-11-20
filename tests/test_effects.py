"""ledger.effect: write-ahead records, idempotency keys, retries, replay."""

from __future__ import annotations

import asyncio
import operator
from typing import Annotated, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.store import step_key
from ledger.effects import idempotency_key

PENDING = pb.EffectCall.PHASE_PENDING
CONFIRMED = pb.EffectCall.PHASE_CONFIRMED
FAILED = pb.EffectCall.PHASE_FAILED


def effects(store, run_id):
    return [ev for ev in store.events(run_id) if body_kind(ev) == "effect"]


class Flaky:
    """An effect body that fails ``fail`` times with ``exc``, then succeeds."""

    def __init__(self, fail=0, exc=ConnectionError):
        self.keys: list[str] = []
        self.fail = fail
        self.exc = exc

    def __call__(self, amount, *, idempotency_key):
        self.keys.append(idempotency_key)
        if len(self.keys) <= self.fail:
            raise self.exc(f"attempt {len(self.keys)}")
        return {"refunded": amount, "key": idempotency_key}


def make(body, **kw):
    return ledger.effect(name="refund", backoff=0, **kw)(body)


def test_records_pending_then_confirmed_with_a_derived_key(store):
    body = Flaky()
    refund = make(body)
    with ledger.record(store) as rec:
        assert refund(500)["refunded"] == 500

    pending, confirmed = effects(store, rec.run_id)
    assert (pending.effect.phase, confirmed.effect.phase) == (PENDING, CONFIRMED)
    assert pending.index < confirmed.index and pending.step == confirmed.step
    assert not pending.effect.HasField("result")
    key = idempotency_key(rec.run_id, pending.step)
    assert body.keys == [key] == [pending.effect.idempotency_key]
    assert load_payload(pending.effect.args) == {"args": [500], "kwargs": {}}
    assert load_payload(confirmed.effect.result) == {"refunded": 500, "key": key}
    assert confirmed.effect.attempts == 1
    assert rec.stats.live_calls == 1


def test_keys_differ_by_run_and_step_and_repeat_for_the_same_pair():
    a = pb.StepRef(node="refund", superstep=3, path="(x)", seq=0)
    b = pb.StepRef(node="refund", superstep=3, path="(x)", seq=1)
    assert idempotency_key("r1", a) == idempotency_key("r1", a)
    assert len({idempotency_key("r1", a), idempotency_key("r1", b), idempotency_key("r2", a)}) == 3
    assert len(idempotency_key("r1", a)) <= 255  # Stripe's limit


def test_retries_reuse_the_key(store):
    body = Flaky(fail=2)
    with ledger.record(store) as rec:
        make(body)(100)
    assert len(body.keys) == 3 and len(set(body.keys)) == 1
    _, confirmed = effects(store, rec.run_id)
    assert confirmed.effect.attempts == 3


def test_an_error_outside_retry_on_is_a_definite_failure(store):
    body = Flaky(fail=1, exc=ValueError)
    with ledger.record(store) as rec:
        with pytest.raises(ValueError, match="attempt 1"):
            make(body)(100)
    assert len(body.keys) == 1
    _, failed = effects(store, rec.run_id)
    assert failed.effect.phase == FAILED
    assert failed.effect.error.type == "builtins.ValueError"


def test_running_out_of_retries_leaves_the_effect_in_doubt(store):
    body = Flaky(fail=99)
    with ledger.record(store) as rec:
        with pytest.raises(ledger.EffectInDoubt) as info:
            make(body, attempts=4)(100)
    assert len(body.keys) == 4
    assert info.value.idempotency_key == body.keys[0]
    assert isinstance(info.value.__cause__, ConnectionError)
    first, last = effects(store, rec.run_id)
    assert first.effect.phase == last.effect.phase == PENDING
    assert last.effect.attempts == 4 and last.effect.error.type == "builtins.ConnectionError"
    [step] = ledger.steps(rec.run_id, store)
    assert step.kind == "effect" and step.summary.startswith("in doubt")


def test_replay_answers_from_the_log_and_never_calls(store):
    body = Flaky(fail=1)
    refund = make(body)
    with ledger.record(store) as rec:
        recorded = refund(250)
    calls = len(body.keys)
    with ledger.replay(rec.run_id, store) as rep:
        assert refund(250) == recorded
    assert len(body.keys) == calls
    [served] = effects(store, rep.run_id)
    assert served.from_log and served.effect.phase == CONFIRMED
    assert rep.stats.live_calls == 0 and rep.stats.replayed_calls == 1


def test_replay_raises_a_recorded_failure_again(store):
    refund = make(Flaky(fail=1, exc=ValueError))
    with ledger.record(store) as rec:
        with pytest.raises(ValueError):
            refund(1)
    with ledger.replay(rec.run_id, store):
        with pytest.raises(ValueError, match="attempt 1"):
            refund(1)


def test_replay_of_an_effect_left_in_doubt_raises(store):
    refund = make(Flaky(fail=99), attempts=2)
    with ledger.record(store) as rec:
        with pytest.raises(ledger.EffectInDoubt):
            refund(1)
    with ledger.replay(rec.run_id, store):
        with pytest.raises(ledger.EffectInDoubt, match="in doubt after 2"):
            refund(1)


def test_the_key_is_injected_not_passed():
    with pytest.raises(TypeError, match="must accept an idempotency_key"):
        ledger.effect(lambda amount: amount)
    body = Flaky()
    refund = make(body)
    with ledger.record(ledger.EventStore(":memory:")):
        with pytest.raises(TypeError, match="don't pass one"):
            refund(1, idempotency_key="mine")
    # Outside a session: a fresh key, or the caller's own.
    refund(1)
    refund(1, idempotency_key="mine")
    assert len(body.keys[0]) == 32 and body.keys[1] == "mine"


def test_async_effects(store):
    keys = []

    @ledger.effect(name="arefund", backoff=0)
    async def arefund(amount, *, idempotency_key):
        keys.append(idempotency_key)
        await asyncio.sleep(0)
        if len(keys) == 1:
            raise TimeoutError("slow")
        return amount

    async def run():
        return await arefund(7)

    with ledger.record(store) as rec:
        assert asyncio.run(run()) == 7
    with ledger.replay(rec.run_id, store):
        assert asyncio.run(run()) == 7
    assert len(keys) == 2 and keys[0] == keys[1]


# In a graph: a tool upstream of an effect, to diverge and fork on.


class S(TypedDict, total=False):
    amount: int
    quote: int
    receipts: Annotated[list[dict], operator.add]


def graph(body, extra=0):
    """``extra`` edits the price node: it asks for a different quote."""

    @ledger.tool(name="quote")
    def quote(amount: int) -> int:
        return amount

    refund = make(body)
    b = StateGraph(S)
    b.add_node("price", lambda s: {"quote": quote(s["amount"] + extra)})
    b.add_node("pay", lambda s: {"receipts": [refund(s["quote"])]})
    b.add_edge(START, "price")
    b.add_edge("price", "pay")
    b.add_edge("pay", END)
    return ledger.instrument(b.compile())


def test_a_replay_that_diverges_upstream_refuses_to_run_the_effect(store):
    body = Flaky()
    with ledger.record(store) as rec:
        graph(body).invoke({"amount": 10})
    edited = graph(body, extra=5)
    with pytest.raises(ledger.LiveEffectRefused, match="live_effects=True"):
        with ledger.replay(rec.run_id, store, on_divergence="live"):
            edited.invoke({"amount": 10})
    assert len(body.keys) == 1

    with ledger.replay(rec.run_id, store, on_divergence="live", live_effects=True) as rep:
        out = edited.invoke({"amount": 10})
    assert out["receipts"][0]["refunded"] == 15
    # A live effect in a replay is a new run's effect, with that run's key.
    assert body.keys[1] != body.keys[0]
    assert body.keys[1] == idempotency_key(rep.run_id, effects(store, rep.run_id)[0].step)


def test_fork_edits_an_effect_without_calling_it_and_refuses_a_rerun(store):
    body = Flaky()
    with ledger.record(store) as rec:
        graph(body).invoke({"amount": 10})
    step = next(s for s in ledger.steps(rec.run_id, store) if s.kind == "effect")

    with ledger.fork(rec.run_id, at=step, edit={"refunded": 0, "key": "edited"}, store=store) as fk:
        out = graph(body).invoke({"amount": 10})
    assert out["receipts"] == [{"refunded": 0, "key": "edited"}]
    assert len(body.keys) == 1
    [edited] = [e for e in effects(store, fk.run_id) if step_key(e.step) == step.key]
    assert edited.effect.phase == CONFIRMED

    with pytest.raises(ledger.LiveEffectRefused):
        with ledger.fork(rec.run_id, at=step, store=store):
            graph(body).invoke({"amount": 10})
    assert len(body.keys) == 1


def test_steps_lists_an_effect_once_at_its_outcome(store):
    with ledger.record(store) as rec:
        graph(Flaky()).invoke({"amount": 3})
    kinds = [(s.kind, s.label) for s in ledger.steps(rec.run_id, store)]
    assert kinds == [("tool_call", "quote"), ("effect", "refund")]
    _, confirmed = effects(store, rec.run_id)
    assert ledger.steps(rec.run_id, store)[1].index == confirmed.index
