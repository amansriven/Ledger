"""The chaos harness's pieces: invariants, the worker, and a short local session."""

from __future__ import annotations

import json

import pytest

import ledger
from chaos import harness, invariants, worker
from ledger.testing.fake_stripe import FakeStripeServer


def tickets_for(server: FakeStripeServer, amounts: list[int]) -> list[dict]:
    out = []
    for i, amount in enumerate(amounts):
        charge = server.create_charge(amount * 3)
        out.append({"ticket_id": f"T{i}", "charge_id": charge["id"], "amount": amount,
                    "message": "refund the shipping", "reason": "requested_by_customer"})
    return out


def refund(server: FakeStripeServer, ticket: dict, key: str | None = None) -> dict:
    status, body, _ = server.post(
        "/v1/refunds",
        {"charge": ticket["charge_id"], "amount": str(ticket["amount"]),
         "metadata": {"ledger_key": key or ""}},
        key,
    )
    assert status == 200
    return body


def test_duplicates_and_missing_refunds_are_counted():
    with FakeStripeServer() as server:
        ok, twice, none, over_limit = tickets_for(server, [1000, 2000, 3000, 70_000])
        refund(server, ok)
        refund(server, twice)
        refund(server, twice)
        report = invariants.check_stripe("x", [ok, twice, none, over_limit], server.state())
    assert (report.refunds, report.duplicate, report.missing) == (3, 1, 1)
    assert set(report.examples) == {"T1", "T2"}
    assert not report.ok


def test_a_refund_over_the_limit_is_a_duplicate():
    with FakeStripeServer() as server:
        [big] = tickets_for(server, [70_000])
        refund(server, big)
        report = invariants.check_stripe("x", [big], server.state())
    assert report.duplicate == 1


def test_the_ledger_log_is_reconciled_against_stripe(agent, stripe_server, store, monkeypatch):
    monkeypatch.setattr("ledger.effects.manager._delay", lambda backoff, attempt: 0)
    a, b = tickets_for(stripe_server, [1000, 2000])
    for t in (a, b):
        with ledger.record(store, run_id=worker.run_id_for(t)):
            agent.invoke(t)
    report = invariants.check_ledger_log(
        invariants.check_stripe("ledger", [a, b], stripe_server.state()),
        [a, b], stripe_server.state(), store,
    )
    assert report.ok, report.examples

    refund(stripe_server, b)  # a refund nobody logged
    state = stripe_server.state()
    stripe_server.refunds.pop(state["refunds"][0]["id"])  # and one that vanished
    report = invariants.check_ledger_log(
        invariants.check_stripe("ledger", [a, b], stripe_server.state()),
        [a, b], stripe_server.state(), store,
    )
    # b now has two refunds (one unlogged); a has none left (its logged one is a phantom).
    assert (report.unlogged, report.phantom, report.duplicate, report.missing) == (1, 1, 1, 1)


def test_resume_times_are_measured_from_each_restart(tmp_path):
    events = tmp_path / "x-events.jsonl"
    lines = [{"t": 10.5, "event": "resumed"}, {"t": 10.9, "event": "resumed"},
             {"t": 20.2, "event": "resumed"}]
    events.write_text("".join(json.dumps(line) + "\n" for line in lines))
    assert harness.resume_times(events, [10.0, 15.0, 20.0]) == pytest.approx([0.5, 0.2])


@pytest.mark.parametrize("system", ["ledger", "langgraph"])
def test_a_short_local_session_settles_every_ticket(tmp_path, monkeypatch, system):
    monkeypatch.setattr(harness, "WORK", tmp_path / "work")
    out = tmp_path / "result.json"
    code = harness.main([
        "--mode", "local", "--system", system, "--runs", "40", "--seed", "3",
        "--kill-after", "1.2", "2.0", "--out", str(out),
    ])
    [session] = json.loads(out.read_text())["sessions"]
    report = session["report"]
    assert report["missing"] == 0
    assert report["refunds"] >= 30
    if system == "ledger":
        assert code == 0 and report["ok"]
        assert report["duplicate"] == report["unlogged"] == report["unfinished"] == 0
