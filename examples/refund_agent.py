"""Refund agent: a support ticket in, a Stripe refund and a reply out.

    lookup -> decide -> refund -> reply

``lookup`` reads the charge from Stripe (a ``ledger.tool``: reading twice is
harmless). ``decide`` applies the refund policy in code, and leaves a charge
another ticket has claimed to that ticket. ``refund`` first claims the charge
for this ticket (an effect writing the charge's metadata), then creates the
refund. If the refund fails for good (Stripe refuses it), the run's saga
releases the claim again, so another ticket or a person can take over. Both
are ``ledger.effect`` calls, so each is made once per run whatever fails
around it. For the refund: every attempt carries the same idempotency key, derived
from the run id and the step, and the key also goes in the refund's
metadata. After a crash, ``ledger.resume`` looks the refund up by that
metadata before trying again. ``reply`` has the LLM draft the
answer to the customer.

The same code runs against the fake Stripe server
(``ledger.testing.fake_stripe``) and against Stripe in test mode: set
``STRIPE_API_BASE`` to an empty string, and put a test key in
``STRIPE_SECRET_KEY`` (the environment, or ``.env`` at the repo root, which
git ignores). Live keys are refused, and the fake server never gets the key.

    python -m ledger.testing.fake_stripe --port 12111 &
    python -m ledger.testing.fake_llm --port 8787 &
    python -m examples.refund_agent          # seeds a charge, records one refund
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import Any, TypedDict

import openai
import stripe
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy
from openai import OpenAI

import ledger
from ledger.intercept.langgraph import InstrumentedGraph

# Refunds above this many cents go to a human instead.
AUTO_REFUND_LIMIT = 50_000
REASONS = ("duplicate", "fraudulent", "requested_by_customer")
FAKE_STRIPE = "http://127.0.0.1:12111"
# Charge metadata key naming the ticket that is refunding the charge.
CLAIM = "refund_claim"
ENV_FILE = Path(__file__).resolve().parents[1] / ".env"

# Stripe errors after which a refund may or may not have been made: retrying
# with the same idempotency key is safe. Anything else (a declined or invalid
# request) means it was not made.
IN_DOUBT = (stripe.APIConnectionError, stripe.APIError, stripe.RateLimitError)

# Errors worth retrying a read-only node for: the charge lookup and the reply.
TRANSIENT = IN_DOUBT + (
    openai.APIConnectionError, openai.APITimeoutError, openai.InternalServerError,
    openai.RateLimitError,
)
READ_RETRY = RetryPolicy(initial_interval=0.05, max_attempts=4, retry_on=TRANSIENT)


class Ticket(TypedDict, total=False):
    ticket_id: str
    charge_id: str
    message: str
    reason: str  # one of REASONS
    amount: int  # cents; absent means everything not yet refunded
    charge: dict[str, Any]
    decision: dict[str, Any]
    refund: dict[str, Any] | None
    reply: str


def stripe_client(api_key: str, api_base: str | None = None) -> stripe.StripeClient:
    """A Stripe client for test keys only. Ledger owns the retries."""
    if not api_key.startswith(("sk_test_", "rk_test_")):
        raise ValueError("the refund agent only runs with Stripe test-mode keys")
    kwargs: dict[str, Any] = {"max_network_retries": 0}
    if api_base:
        kwargs["base_addresses"] = {"api": api_base}
    return stripe.StripeClient(api_key, **kwargs)


def _from_env_file(name: str, path: Path) -> str | None:
    """``name``'s value in a ``KEY=value`` file, if it's there. Nothing else is loaded."""
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return None
    for line in lines:
        key, sep, value = line.strip().removeprefix("export ").partition("=")
        if sep and key.strip() == name:
            return value.strip().strip("'\"") or None
    return None


def stripe_settings(env_file: Path = ENV_FILE) -> tuple[str, str | None]:
    """(api key, api base) from the environment.

    ``STRIPE_API_BASE`` defaults to the fake server, which gets a fake key.
    Set it to an empty string for Stripe itself; the key then comes from
    ``STRIPE_SECRET_KEY`` in the environment or in ``env_file``.
    """
    base = os.environ.get("STRIPE_API_BASE", FAKE_STRIPE)
    if base:
        return "sk_test_fake", base
    key = os.environ.get("STRIPE_SECRET_KEY") or _from_env_file("STRIPE_SECRET_KEY", env_file)
    if not key:
        raise RuntimeError(f"set STRIPE_SECRET_KEY (in the environment or {env_file}) to a test key")
    return key, None


def decide(ticket: Ticket, charge: dict[str, Any]) -> dict[str, Any]:
    """The refund policy: what's asked for, capped at what's left, up to a limit."""
    left = charge["amount"] - charge["amount_refunded"]
    amount = min(int(ticket.get("amount") or left), left)
    if charge.get("status") != "succeeded":
        return {"refund": False, "amount": 0, "why": f"charge is {charge.get('status')}"}
    if left == 0:
        return {"refund": False, "amount": 0, "why": "already fully refunded"}
    claimed_by = (charge.get("metadata") or {}).get(CLAIM)
    if claimed_by and claimed_by != ticket.get("ticket_id"):
        return {"refund": False, "amount": 0, "why": f"ticket {claimed_by} is refunding it"}
    if amount > AUTO_REFUND_LIMIT:
        return {"refund": False, "amount": amount, "why": "over the automatic limit; sent to a human"}
    if ticket.get("reason", "requested_by_customer") not in REASONS:
        return {"refund": False, "amount": amount, "why": f"unknown reason {ticket.get('reason')!r}"}
    return {"refund": True, "amount": amount, "why": "within policy"}


def build_builder(
    llm: Any, client: stripe.StripeClient, *, model: str, retry_refund: bool = False
) -> StateGraph:
    """The agent, uncompiled. ``llm`` should be wrapped with ``ledger.wrap``.

    The read-only nodes (``lookup``, ``reply``) are retried on transient
    errors. ``refund`` is not: its effects retry with their idempotency
    keys, and rerunning the node would make new ones. ``retry_refund`` gives
    it the same node retry anyway, the naive setup the chaos benchmark's
    baseline uses (run without Ledger, each attempt gets a fresh key).
    """

    @ledger.tool(name="stripe.charges.retrieve")
    def get_charge(charge_id: str) -> dict[str, Any]:
        return client.v1.charges.retrieve(charge_id).to_dict()

    def find_refund(
        charge_id: str, amount: int, reason: str, ticket_id: str, *, idempotency_key: str
    ) -> dict[str, Any] | None:
        """The refund this key made, if Stripe has it: settles a refund left in
        doubt without relying on Stripe still remembering the key."""
        for refund in client.v1.refunds.list({"charge": charge_id, "limit": 100}).auto_paging_iter():
            found = refund.to_dict()
            if (found.get("metadata") or {}).get("ledger_key") == idempotency_key:
                return found
        return None

    def release_claim(
        claimed: dict[str, Any], charge_id: str, ticket_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        """Compensation for claim_charge: drop the claim (Stripe removes a
        metadata key set to an empty string)."""
        return client.v1.charges.update(
            charge_id, {"metadata": {CLAIM: ""}}, {"idempotency_key": idempotency_key}
        ).to_dict()

    @ledger.effect(name="stripe.charges.claim", retry_on=IN_DOUBT, compensate=release_claim)
    def claim_charge(charge_id: str, ticket_id: str, *, idempotency_key: str) -> dict[str, Any]:
        return client.v1.charges.update(
            charge_id, {"metadata": {CLAIM: ticket_id}}, {"idempotency_key": idempotency_key}
        ).to_dict()

    @ledger.effect(name="stripe.refunds.create", retry_on=IN_DOUBT, reconcile=find_refund)
    def create_refund(
        charge_id: str, amount: int, reason: str, ticket_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        return client.v1.refunds.create(
            {
                "charge": charge_id,
                "amount": amount,
                "reason": reason,
                "metadata": {"ticket_id": ticket_id, "ledger_key": idempotency_key},
            },
            {"idempotency_key": idempotency_key},
        ).to_dict()

    def lookup(state: Ticket) -> Ticket:
        return {"charge": get_charge(state["charge_id"])}

    def decide_node(state: Ticket) -> Ticket:
        return {"decision": decide(state, state["charge"])}

    def refund(state: Ticket) -> Ticket:
        decision = state["decision"]
        if not decision["refund"]:
            return {"refund": None}
        claim_charge(state["charge_id"], state["ticket_id"])
        return {
            "refund": create_refund(
                state["charge_id"],
                decision["amount"],
                state.get("reason", "requested_by_customer"),
                state["ticket_id"],
            )
        }

    def reply(state: Ticket) -> Ticket:
        refund = state.get("refund")
        outcome = (
            f"refunded {refund['amount'] / 100:.2f} {refund['currency'].upper()} ({refund['id']})"
            if refund
            else f"no refund: {state['decision']['why']}"
        )
        resp = llm.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You answer customer support tickets briefly."},
                {"role": "user", "content": f"Ticket: {state.get('message', '')}\nOutcome: {outcome}"},
            ],
            temperature=0.3,
        )
        return {"reply": resp.choices[0].message.content or ""}

    b = StateGraph(Ticket)
    b.add_node("lookup", lookup, retry_policy=READ_RETRY)
    b.add_node("decide", decide_node)
    b.add_node("refund", refund, retry_policy=READ_RETRY if retry_refund else None)
    b.add_node("reply", reply, retry_policy=READ_RETRY)
    b.add_edge(START, "lookup")
    b.add_edge("lookup", "decide")
    b.add_edge("decide", "refund")
    b.add_edge("refund", "reply")
    b.add_edge("reply", END)
    return b


def build_graph(llm: Any, client: stripe.StripeClient, *, model: str) -> InstrumentedGraph:
    return ledger.instrument(build_builder(llm, client, model=model).compile())


def graph_from_env() -> InstrumentedGraph:
    """The agent wired from the environment.

    Stripe as ``stripe_settings`` reads it (the fake server on :12111 unless
    ``STRIPE_API_BASE`` is empty), plus the LLM settings the toy pipeline uses.
    """
    return ledger.instrument(builder_from_env().compile())


def builder_from_env(*, retry_refund: bool = False) -> StateGraph:
    """``build_builder`` wired from the environment, as ``graph_from_env`` reads it."""
    llm = ledger.wrap(
        OpenAI(
            base_url=os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:8787/v1"),
            api_key=os.environ.get("OPENAI_API_KEY", "fake"),
            max_retries=0,
        )
    )
    client = stripe_client(*stripe_settings())
    return build_builder(
        llm, client, model=os.environ.get("LEDGER_MODEL", "fake-model"), retry_refund=retry_refund
    )


def main() -> int:
    """Seed a charge on the fake Stripe server and record one ticket."""
    base = os.environ.get("STRIPE_API_BASE", FAKE_STRIPE)
    if not base:
        print("the demo seeds a charge on the fake server; it doesn't run against Stripe", file=sys.stderr)
        return 2
    req = urllib.request.Request(
        f"{base}/_charges", data=json.dumps({"amount": 4200}).encode(), method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        charge = json.load(resp)
    ticket = {
        "ticket_id": "T-1001",
        "charge_id": charge["id"],
        "message": "I was charged twice for the same order.",
        "reason": "duplicate",
    }
    with ledger.record() as session:
        out = graph_from_env().invoke(ticket)
    print(f"recorded {session.run_id}: {out['decision']['why']}, refund {out['refund']['id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
