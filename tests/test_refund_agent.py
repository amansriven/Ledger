"""The refund agent against the fake Stripe server, with faults injected."""

from __future__ import annotations

import pytest
from openai import OpenAI

import ledger
from examples import refund_agent
from ledger.core.events import body_kind, load_payload, pb
from tests.conftest import crash_after, replays_strictly


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


REFUND = "stripe.refunds.create"
CLAIM = "stripe.charges.claim"


def effect_events(store, run_id, name=REFUND):
    """The log's events for one effect, by name (the refund by default)."""
    return [
        ev.effect for ev in store.events(run_id)
        if body_kind(ev) == "effect" and ev.effect.name == name
    ]


def test_one_ticket_one_refund_keyed_by_run_and_step(agent, stripe_server, store):
    charge = stripe_server.create_charge(4200)
    with ledger.record(store) as rec:
        out = agent.invoke(ticket(charge))

    [refund] = stripe_server.refunds_of(charge["id"])
    assert out["refund"]["id"] == refund["id"] and refund["amount"] == 4200
    pending, confirmed = effect_events(store, rec.run_id)
    assert refund["metadata"] == {"ticket_id": "T-1", "ledger_key": pending.idempotency_key}
    assert confirmed.phase == pb.EffectCall.PHASE_CONFIRMED
    # The charge stays claimed by the ticket that refunded it.
    assert stripe_server.charge(charge["id"])["metadata"] == {"refund_claim": "T-1"}
    assert [e.phase for e in effect_events(store, rec.run_id, CLAIM)] == [
        pb.EffectCall.PHASE_PENDING, pb.EffectCall.PHASE_CONFIRMED,
    ]
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
    # In doubt is not a failure for good: the claim stays, for the resume.
    assert stripe_server.charge(charge["id"])["metadata"] == {"refund_claim": "T-1"}
    assert effect_events(store, rec.run_id, CLAIM + ":compensate") == []


def test_a_refused_refund_fails_and_releases_the_claim(agent, stripe_server, store, monkeypatch):
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
    # The saga released the claim, with an effect of its own.
    assert stripe_server.charge(charge["id"])["metadata"] == {}
    _, released = effect_events(store, rec.run_id, CLAIM + ":compensate")
    assert released.phase == pb.EffectCall.PHASE_CONFIRMED
    assert ledger.resumable_runs(store) == []  # failed for good: nothing to resume

    # A replay fails the same way, and answers the release from the log.
    monkeypatch.setattr(refund_agent, "decide", decide)  # no more racing refunds
    posts = stripe_server.state()["requests"]
    with ledger.replay(rec.run_id, store) as rep:
        with pytest.raises(stripe.InvalidRequestError):
            agent.invoke(load_payload(rep.replay_index.input))
    assert stripe_server.state()["requests"] == posts
    assert rep.stats.live_calls == 0


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
    assert rep.stats.live_calls == 0 and rep.stats.replayed_calls == 4
    assert stripe_server.state()["requests"] == before
    assert len(stripe_server.refunds_of(charge["id"])) == 1


def test_a_charge_claimed_by_another_ticket_is_left_alone(agent, stripe_server, store):
    charge = stripe_server.create_charge(1000, metadata={"refund_claim": "T-7"})
    with ledger.record(store):
        out = agent.invoke(ticket(charge))
    assert out["refund"] is None and "T-7" in out["decision"]["why"]
    assert stripe_server.state()["requests"].get("POST /v1/refunds", 0) == 0


def test_policy_sends_big_refunds_to_a_human(agent, stripe_server, store):
    charge = stripe_server.create_charge(90_000)
    with ledger.record(store):
        out = agent.invoke(ticket(charge))
    assert out["refund"] is None and "human" in out["decision"]["why"]
    assert stripe_server.refunds_of(charge["id"]) == []


def test_a_lookup_cut_off_is_retried_by_the_node(agent, stripe_server, store):
    charge = stripe_server.create_charge(1000)
    stripe_server.add_fault("/v1/charges/*", "drop_before", count=2)
    assert stripe_server.state()["faults_pending"] == 2
    with ledger.record(store):
        out = agent.invoke(ticket(charge))

    assert stripe_server.state()["faults_pending"] == 0
    assert len(stripe_server.refunds_of(charge["id"])) == 1 and out["reply"]


def test_without_ledger_a_node_retry_refunds_twice(fake_server, stripe_server):
    """Why ``refund`` gets no node retry: the chaos baseline's failure, in one run."""
    llm = OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0)
    client = refund_agent.stripe_client("sk_test_fake", stripe_server.base_url)
    app = refund_agent.build_builder(llm, client, model="fake-model", retry_refund=True).compile()
    charge = stripe_server.create_charge(3000)
    stripe_server.add_fault("/v1/refunds", "drop_after")  # applied, answer lost
    app.invoke(ticket(charge, amount=1000))

    assert len(stripe_server.refunds_of(charge["id"])) == 2


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


# Crashes: kill the agent partway (every later write to the log fails, like
# kill -9), then resume it.


def kinds(store, run_id):
    return [body_kind(ev) for ev in store.events(run_id)]


# A clean run of the agent logs 8 events: run_started, the charge lookup,
# the claim's PENDING and CONFIRMED, the refund's PENDING and CONFIRMED, the
# reply, run_finished. Crash after each of the first seven (after the eighth
# there is nothing left to lose).
@pytest.mark.parametrize("n", range(1, 8))
def test_a_crash_anywhere_resumes_to_exactly_one_refund(agent, stripe_server, store, n):
    charge = stripe_server.create_charge(2500)
    with crash_after(store, n):
        with ledger.record(store) as rec:
            agent.invoke(ticket(charge))
    run_id = rec.run_id
    assert ledger.resumable_runs(store) == [run_id]
    before = list(store.events(run_id))

    with ledger.resume(run_id, store) as res:
        out = agent.invoke(ticket(charge))

    [refund] = stripe_server.refunds_of(charge["id"])
    assert out["refund"]["id"] == refund["id"] and refund["amount"] == 2500
    after = list(store.events(run_id))
    assert after[:n] == before  # the prefix is untouched
    assert body_kind(after[n]) == "run_resumed" and after[n].run_resumed.attempt == 1
    assert kinds(store, run_id)[-1] == "run_finished"
    assert ledger.resumable_runs(store) == []
    # Only what wasn't logged ran: the refund is never asked for twice when
    # its outcome was logged, and at most twice (same key) when it wasn't.
    posts = stripe_server.state()["requests"].get("POST /v1/refunds", 0)
    assert posts == 1  # at n=5 Stripe made it before the crash and reconcile found it
    assert stripe_server.charge(charge["id"])["metadata"] == {"refund_claim": "T-1"}
    assert res.stats.replayed_calls + res.stats.live_calls == 4
    replays_strictly(store, run_id, agent, out)


def test_the_crash_that_matters_is_settled_by_reconcile(agent, stripe_server, store):
    """Killed after Stripe made the refund, before CONFIRMED was logged."""
    charge = stripe_server.create_charge(700)
    with crash_after(store, 5):  # run_started, lookup, the claim, the refund's PENDING
        with ledger.record(store) as rec:
            agent.invoke(ticket(charge))
    # The crash hit the CONFIRMED append, so Stripe has the refund.
    assert len(stripe_server.refunds_of(charge["id"])) == 1

    with ledger.resume(rec.run_id, store):
        agent.invoke(ticket(charge))
    assert stripe_server.state()["requests"]["POST /v1/refunds"] == 1  # not asked again
    settled = effect_events(store, rec.run_id)[-1]
    assert settled.phase == pb.EffectCall.PHASE_CONFIRMED and settled.reconciled
    assert len(stripe_server.refunds_of(charge["id"])) == 1


def test_an_effect_in_doubt_is_settled_on_resume(agent, stripe_server, store):
    charge = stripe_server.create_charge(900)
    stripe_server.add_fault("/v1/refunds", "drop_after", count=3)
    with ledger.record(store) as rec:
        with pytest.raises(ledger.EffectInDoubt):
            agent.invoke(ticket(charge))
    assert ledger.resumable_runs(store) == [rec.run_id]

    with ledger.resume(rec.run_id, store) as res:
        out = agent.invoke(ticket(charge))
    assert res.previous_error is not None and "EffectInDoubt" in res.previous_error.type
    [refund] = stripe_server.refunds_of(charge["id"])
    assert out["refund"]["id"] == refund["id"]
    assert stripe_server.state()["requests"]["POST /v1/refunds"] == 3  # none from the resume
    replays_strictly(store, rec.run_id, agent, out)
