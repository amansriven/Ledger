"""Order-total pipeline: a small multi-agent workload whose answer can be checked.

    intake -> (pricing | tax | coupon) -> accountant -> writer

A customer asks, in plain words, for some items shipped to a state with a
coupon code. ``intake`` (LLM) turns the request into a JSON order. Three
agents then work in parallel: ``pricing`` looks each item up in the catalog
(a tool call per item) and has the LLM add up the subtotal, ``tax`` looks up
the state's sales tax rate and ``coupon`` looks up the code's discount (tool
calls). ``accountant`` (LLM) applies the discount and then the tax, and
``writer`` (LLM) answers the customer with the total.

The right total follows from the catalog, so a run succeeds when the reply
quotes it to the cent (``expected_total``, ``reply_is_right``). The model
does the arithmetic, so a real model can get it wrong on its own; injected
faults (``perturb``) change a number in one call's outcome, the kind of
mistake a lookup or a model makes, so that the trace still reads plausibly.
"""

from __future__ import annotations

import json
import random
import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

import ledger
from ledger.intercept.langgraph import InstrumentedGraph

CATALOG = {
    "blue mug": 12.50, "notebook": 4.75, "desk lamp": 38.00, "water bottle": 18.25,
    "phone stand": 15.40, "wool socks": 9.99, "backpack": 64.00, "pen set": 7.30,
}
TAX = {"Oregon": 0.0, "Texas": 0.0625, "California": 0.0725, "New York": 0.04,
       "Washington": 0.065, "Florida": 0.06}
COUPONS = {"SAVE10": 10, "SAVE20": 20, "WELCOME5": 5}
_NUM = re.compile(r"\d+(?:\.\d+)?")


class State(TypedDict, total=False):
    request: str
    order: dict[str, Any]
    subtotal: float
    tax_rate: float
    percent_off: float
    total: float
    reply: str


def cents(x: float | Decimal) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def expected_total(order: dict[str, Any]) -> Decimal:
    """Subtotal, less the coupon, plus tax on the discounted amount, to the cent."""
    subtotal = sum(Decimal(str(CATALOG[i["name"]])) * i["qty"] for i in order["items"])
    off = Decimal(COUPONS.get(order.get("coupon") or "", 0)) / 100
    rate = Decimal(str(TAX[order["state"]]))
    return cents(subtotal * (1 - off) * (1 + rate))


def make_order(rnd: random.Random) -> tuple[str, dict[str, Any]]:
    """A random order and the customer's request for it."""
    names = rnd.sample(sorted(CATALOG), rnd.randint(1, 3))
    items = [{"name": n, "qty": rnd.randint(1, 4)} for n in names]
    state = rnd.choice(sorted(TAX))
    coupon = rnd.choice([*sorted(COUPONS), None])
    parts = [f"{i['qty']} {i['name']}{'s' if i['qty'] > 1 and not i['name'].endswith('s') else ''}"
             for i in items]
    wanted = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]
    opener = rnd.choice(["Hi! I'd like to order", "Hello, please send me", "Can I get"])
    code = f" I have the code {coupon}." if coupon else ""
    request = f"{opener} {wanted}, shipped to {state}.{code} What will it cost in total?"
    return request, {"items": items, "state": state, "coupon": coupon}


def reply_is_right(reply: str, order: dict[str, Any]) -> bool:
    """Whether the reply quotes the right total (and no other dollar amount)."""
    want = expected_total(order)
    amounts = {cents(m.replace(",", "")) for m in re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", reply)}
    return amounts == {want}


def _json(text: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        raise ValueError(f"no JSON in {text[:200]!r}")
    return json.loads(m.group(0))


def perturb(rnd: random.Random) -> Any:
    """A corrupt function that changes one number in a call's outcome.

    Quantities (small whole numbers) go up by 1 or 2; amounts and rates are
    scaled by 1.15 to 1.6 (a rate of 0 becomes 0.05). For an LLM call the
    number is changed in the text, for a tool call in its result.
    """

    def bump(x: float, whole: bool) -> float:
        if whole and x < 10:
            return x + rnd.choice([1, 2])
        if x == 0:
            return 0.05
        return round(x * rnd.uniform(1.15, 1.6), 2 if x >= 1 else 4)

    def in_text(text: str) -> str:
        found = list(_NUM.finditer(text))
        if not found:
            return text + " (total $0.01)"
        m = rnd.choice(found)
        whole = "." not in m.group(0)
        new = bump(float(m.group(0)), whole)
        s = str(int(new)) if whole and float(new).is_integer() else f"{new:.2f}" if new >= 1 else str(new)
        return text[: m.start()] + s + text[m.end():]

    def corrupt(value: Any) -> Any:
        if isinstance(value, dict) and "choices" not in value:  # a tool result
            out = dict(value)
            keys = [k for k, v in out.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]
            if keys:
                k = rnd.choice(keys)
                out[k] = bump(out[k], isinstance(out[k], int))
            return out
        from ledger.testing import faults

        return faults.replace_text(in_text)(value)

    return corrupt


def build_builder(llm: Any, *, model: str, reasoning_effort: str | None = "low") -> StateGraph:
    """The pipeline, uncompiled. ``llm`` should already be wrapped with ``ledger.wrap``."""
    extra = {"reasoning_effort": reasoning_effort} if reasoning_effort else {}

    def ask(prompt: str) -> str:
        resp = llm.chat.completions.create(
            model=model, messages=[{"role": "user", "content": prompt}], **extra,
        )
        return resp.choices[0].message.content or ""

    @ledger.tool(name="lookup_price")
    def lookup_price(item: str) -> dict[str, Any]:
        if item not in CATALOG:
            return {"item": item, "error": "not in the catalog"}
        return {"item": item, "unit_price": CATALOG[item]}

    @ledger.tool(name="tax_rate")
    def tax_rate(state: str) -> dict[str, Any]:
        return {"state": state, "rate": TAX.get(state, 0.0)}

    @ledger.tool(name="coupon")
    def coupon(code: str | None) -> dict[str, Any]:
        return {"code": code, "percent_off": COUPONS.get(code or "", 0)}

    def intake(state: State) -> State:
        text = ask(
            "Turn this customer request into a JSON order. Item names must be one of: "
            + ", ".join(sorted(CATALOG)) + ". The state is a US state name; the coupon "
            'is the code or null. Answer with JSON only: {"items": [{"name": ..., "qty": ...}], '
            '"state": ..., "coupon": ...}\n\nRequest: ' + state["request"]
        )
        return {"order": _json(text)}

    def pricing(state: State) -> State:
        lines = []
        for item in state["order"]["items"]:
            found = lookup_price(item["name"])
            lines.append(f"{item['qty']} x {item['name']} at ${found.get('unit_price')} each")
        text = ask("Add up the subtotal of these order lines (quantity times unit price, "
                   'summed). Answer with JSON only: {"subtotal": <number>}\n\n' + "\n".join(lines))
        return {"subtotal": float(_json(text)["subtotal"])}

    def tax(state: State) -> State:
        return {"tax_rate": float(tax_rate(state["order"]["state"])["rate"])}

    def discount(state: State) -> State:
        return {"percent_off": float(coupon(state["order"].get("coupon"))["percent_off"])}

    def accountant(state: State) -> State:
        text = ask(
            f"Subtotal: ${state['subtotal']}. Coupon: {state['percent_off']}% off the subtotal. "
            f"Sales tax: {state['tax_rate'] * 100:g}%, charged on the discounted amount. "
            'Compute the order total, rounded to the cent. Answer with JSON only: {"total": <number>}'
        )
        return {"total": float(_json(text)["total"])}

    def writer(state: State) -> State:
        reply = ask(
            "Write a two-sentence reply to this customer confirming their order and quoting "
            f"the total of ${state['total']:.2f}. Mention no other dollar amount.\n\n"
            f"Customer: {state['request']}"
        )
        return {"reply": reply}

    b = StateGraph(State)
    for name, fn in [("intake", intake), ("pricing", pricing), ("tax", tax),
                     ("coupon", discount), ("accountant", accountant), ("writer", writer)]:
        b.add_node(name, fn)
    b.add_edge(START, "intake")
    for name in ("pricing", "tax", "coupon"):
        b.add_edge("intake", name)
        b.add_edge(name, "accountant")
    b.add_edge("accountant", "writer")
    b.add_edge("writer", END)
    return b


def build_graph(llm: Any, *, model: str, reasoning_effort: str | None = "low") -> InstrumentedGraph:
    return ledger.instrument(build_builder(llm, model=model,
                                           reasoning_effort=reasoning_effort).compile())
