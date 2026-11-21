"""Invariants checked after a chaos session, against what Stripe actually holds.

Each ticket asks for a partial refund of its own fresh charge (so Stripe
would accept a second one), and the expected outcome is known in advance:
one refund if the amount is within the agent's automatic limit, none if it
goes to a person. Per ticket:

duplicate
    every refund beyond the one expected (or any refund when none was).
missing
    an expected refund that isn't at Stripe once the worker is done.

For Ledger, the event log is reconciled against Stripe as well:

unlogged
    a refund at Stripe whose ``ledger_key`` no confirmed refund in the
    ticket's run carries.
phantom
    a refund the log shows as confirmed that Stripe doesn't have.
unfinished
    a run that didn't settle: no RunFinished, or an effect still pending.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from examples.refund_agent import AUTO_REFUND_LIMIT

REFUND = "stripe.refunds.create"


def expected_refunds(ticket: dict[str, Any]) -> int:
    return 1 if ticket["amount"] <= AUTO_REFUND_LIMIT else 0


@dataclass
class Report:
    system: str
    tickets: int
    refunds: int = 0
    duplicate: int = 0
    missing: int = 0
    unlogged: int = 0
    phantom: int = 0
    unfinished: int = 0
    # ticket id -> what was wrong, for the first few
    examples: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not (self.duplicate or self.missing or self.unlogged or self.phantom
                    or self.unfinished)

    def note(self, ticket_id: str, what: str) -> None:
        if len(self.examples) < 20:
            self.examples[ticket_id] = what

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "ok": self.ok}


def refunds_by_charge(stripe_state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for refund in stripe_state["refunds"]:
        out.setdefault(refund["charge"], []).append(refund)
    return out


def check_stripe(system: str, tickets: list[dict[str, Any]], stripe_state: dict[str, Any]) -> Report:
    """Duplicate and missing refunds, from Stripe's side alone."""
    by_charge = refunds_by_charge(stripe_state)
    report = Report(system, len(tickets))
    for ticket in tickets:
        got = len(by_charge.get(ticket["charge_id"], []))
        want = expected_refunds(ticket)
        report.refunds += got
        if got > want:
            report.duplicate += got - want
            report.note(ticket["ticket_id"], f"{got} refunds, expected {want}")
        elif got < want:
            report.missing += want - got
            report.note(ticket["ticket_id"], f"{got} refunds, expected {want}")
    return report


def check_ledger_log(report: Report, tickets: list[dict[str, Any]],
                     stripe_state: dict[str, Any], store: Any) -> Report:
    """Reconcile each ticket's Ledger run against the refunds at Stripe."""
    from chaos.worker import _settled, run_id_for
    from ledger.core.events import body_kind, load_payload, pb

    by_charge = refunds_by_charge(stripe_state)
    for ticket in tickets:
        log = list(store.events(run_id_for(ticket)))
        if not log or not _settled(log):
            report.unfinished += 1
            report.note(ticket["ticket_id"], "run not settled")
        confirmed: Counter[str] = Counter()
        for ev in log:
            if body_kind(ev) == "effect" and ev.effect.name == REFUND:
                if ev.effect.phase == pb.EffectCall.PHASE_CONFIRMED:
                    result = load_payload(ev.effect.result) or {}
                    confirmed[result.get("id", "")] += 1
        at_stripe = {r["id"]: r for r in by_charge.get(ticket["charge_id"], [])}
        for refund_id, refund in at_stripe.items():
            if refund_id not in confirmed:
                report.unlogged += 1
                report.note(ticket["ticket_id"], f"refund {refund_id} at Stripe, not in the log")
        for refund_id in confirmed:
            if refund_id not in at_stripe:
                report.phantom += 1
                report.note(ticket["ticket_id"], f"refund {refund_id} in the log, not at Stripe")
    return report
