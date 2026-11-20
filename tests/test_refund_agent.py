"""The refund agent against the fake Stripe server, with faults injected."""

from __future__ import annotations

import pytest

import ledger
from examples import refund_agent
from ledger.core.events import body_kind, load_payload, pb


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr("ledger.effects.manager._delay", lambda backoff, attempt: 0)


def ticket(charge, **extra):
    return {
        "ticket_id": "T-1",
        "charge_id": charge["id"],
        "message": "charged twice",
        "reason": "duplicate",
        **extra,
    }


def effect_events(store, run_id):
    return [ev.effect for ev in store.events(run_id) if body_kind(ev) == "effect"]


def test_one_ticket_one_refund_keyed_by_run_and_step(agent, stripe_server, store):
    charge = stripe_server.create_charge(4200)
    with ledger.record(store) as rec:
        out = agent.invoke(ticket(charge))

    [refund] = stripe_server.refunds_of(charge["id"])
    assert out["refund"]["id"] == refund["id"] and refund["amount"] == 4200
    pending, confirmed = effect_events(store, rec.run_id)
    assert refund["metadata"] == {"ticket_id": "T-1", "ledger_key": pending.idempotency_key}
    assert confirmed.phase == pb.EffectCall.PHASE_CONFIRMED
    assert out["reply"]


@pytest.mark.parametrize("mode", ["drop_after", "drop_before", "error_500"])
def test_a_lost_or_failed_attempt_is_retried_into_exactly_one_refund(
    agent, stripe_server, store, mode
):
    charge = stripe_server.create_charge(1000)
    stripe_server.add_fault("/v1/refunds", mode)
    with ledger.record(store) as rec:
        out = agent.invoke(ticket(charge))

    refunds = stripe_server.refunds_of(charge["id"])
    assert len(refunds) == 1 and out["refund"]["id"] == refunds[0]["id"]
    assert stripe_server.state()["requests"]["POST /v1/refunds"] == 2
    _, confirmed = effect_events(store, rec.run_id)
    assert confirmed.attempts == 2


def test_a_refund_lost_every_time_is_in_doubt_not_repeated(agent, stripe_server, store):
    charge = stripe_server.create_charge(1000)
    stripe_server.add_fault("/v1/refunds", "drop_after", count=3)
    with ledger.record(store) as rec:
        with pytest.raises(ledger.EffectInDoubt) as info:
            agent.invoke(ticket(charge))

    # Stripe made the refund on the first attempt; the repeats were answered
    # from its idempotency cache, so there is still only one.
    [refund] = stripe_server.refunds_of(charge["id"])
    assert refund["metadata"]["ledger_key"] == info.value.idempotency_key
    phases = [e.phase for e in effect_events(store, rec.run_id)]
    assert phases == [pb.EffectCall.PHASE_PENDING] * 2


def test_a_refused_refund_is_a_definite_failure(agent, stripe_server, store, monkeypatch):
    import stripe

    charge = stripe_server.create_charge(1000)
    decide = refund_agent.decide

    def decide_then_race(ticket, charge_seen):
        # Someone else refunds the charge after the agent looked it up.
        stripe_server.post("/v1/refunds", {"charge": charge_seen["id"]}, None)
        return decide(ticket, charge_seen)

    monkeypatch.setattr(refund_agent, "decide", decide_then_race)
    with ledger.record(store) as rec:
        with pytest.raises(stripe.InvalidRequestError, match="already been refunded"):
            agent.invoke(ticket(charge))

    pending, failed = effect_events(store, rec.run_id)
    assert failed.phase == pb.EffectCall.PHASE_FAILED and failed.attempts == 1
    assert failed.error.type == "stripe._error.InvalidRequestError"
    assert len(stripe_server.refunds_of(charge["id"])) == 1  # only the other one


def test_replay_with_stripe_gone_makes_no_request(agent, stripe_server, store, request):
    charge = stripe_server.create_charge(3000)
    stripe_server.add_fault("/v1/refunds", "drop_after")
    with ledger.record(store) as rec:
        recorded = agent.invoke(ticket(charge))
    before = stripe_server.state()["requests"]

    stripe_server.stop()
    request.getfixturevalue("network_off")
    with ledger.replay(rec.run_id, store) as rep:
        replayed = agent.invoke(load_payload(rep.replay_index.input))
    assert replayed == recorded
    assert rep.stats.live_calls == 0 and rep.stats.replayed_calls == 3
    assert stripe_server.state()["requests"] == before
    assert len(stripe_server.refunds_of(charge["id"])) == 1


def test_policy_sends_big_refunds_to_a_human(agent, stripe_server, store):
    charge = stripe_server.create_charge(90_000)
    with ledger.record(store):
        out = agent.invoke(ticket(charge))
    assert out["refund"] is None and "human" in out["decision"]["why"]
    assert stripe_server.refunds_of(charge["id"]) == []


def test_live_keys_are_refused():
    with pytest.raises(ValueError, match="test-mode"):
        refund_agent.stripe_client("sk_live_abc")


def test_the_real_key_is_read_only_for_stripe_itself(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text('OTHER=1\nexport STRIPE_SECRET_KEY="sk_test_from_file"\n')
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_API_BASE", raising=False)
    # The fake server, by default: it gets a fake key, never the real one.
    assert refund_agent.stripe_settings(env) == ("sk_test_fake", refund_agent.FAKE_STRIPE)

    monkeypatch.setenv("STRIPE_API_BASE", "")
    assert refund_agent.stripe_settings(env) == ("sk_test_from_file", None)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_from_env")
    assert refund_agent.stripe_settings(env) == ("sk_test_from_env", None)
    monkeypatch.delenv("STRIPE_SECRET_KEY")
    with pytest.raises(RuntimeError, match="STRIPE_SECRET_KEY"):
        refund_agent.stripe_settings(tmp_path / "missing.env")
    assert "OTHER" not in __import__("os").environ

