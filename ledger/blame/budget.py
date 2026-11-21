"""What a diagnosis may spend, what it did spend, and how sure each fork is.

    budget = Budget(forks=20, dollars=0.50)
    pricing = Pricing(input_per_mtok=0.15, output_per_mtok=0.60)
    d = ledger.diagnose(run_id, graph, check, repeats=5, budget=budget, pricing=pricing)

A Budget caps forks, live LLM and tool calls, or dollars per diagnosis;
``None`` leaves a cap off. It is checked before each fork, so the fork that
crosses a cap still finishes and the total can go over by one fork's cost.
Dollars need a Pricing, applied to the token usage the live LLM calls
report.

``wilson`` is the Wilson score interval for a success rate, which stays
inside [0, 1] and is sensible for the handful of trials a counterfactual
gets.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ledger.core.events import body_kind, load_payload
from ledger.core.store import EventStore

__all__ = ["Budget", "OutOfBudget", "Pricing", "Spend", "usage", "wilson"]


def wilson(passed: int, trials: int, z: float = 1.96) -> tuple[float, float]:
    """95% (by default) Wilson score interval for ``passed`` out of ``trials``."""
    if trials <= 0:
        return (0.0, 1.0)
    p = passed / trials
    denom = 1 + z * z / trials
    centre = (p + z * z / (2 * trials)) / denom
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


@dataclass(frozen=True)
class Pricing:
    """Dollars per million input and output tokens."""

    input_per_mtok: float
    output_per_mtok: float

    def dollars(self, tokens_in: int, tokens_out: int) -> float:
        return (tokens_in * self.input_per_mtok + tokens_out * self.output_per_mtok) / 1e6


@dataclass
class Spend:
    forks: int = 0
    live_calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    dollars: float = 0.0


class OutOfBudget(Exception):
    """Raised inside a diagnosis when the next fork would start over budget."""


@dataclass(frozen=True)
class Budget:
    forks: int | None = None
    live_calls: int | None = None
    dollars: float | None = None

    def exhausted(self, spent: Spend) -> bool:
        return (
            (self.forks is not None and spent.forks >= self.forks)
            or (self.live_calls is not None and spent.live_calls >= self.live_calls)
            or (self.dollars is not None and spent.dollars >= self.dollars)
        )


def usage(store: EventStore, run_id: str) -> tuple[int, int]:
    """(prompt, completion) tokens of the LLM calls a run made live."""
    tokens_in = tokens_out = 0
    for ev in store.events(run_id):
        if body_kind(ev) != "llm_call" or ev.from_log or ev.llm_call.HasField("error"):
            continue
        resp = load_payload(ev.llm_call.response)
        u = (resp or {}).get("usage") or {} if isinstance(resp, dict) else {}
        tokens_in += int(u.get("prompt_tokens") or 0)
        tokens_out += int(u.get("completion_tokens") or 0)
    return tokens_in, tokens_out
