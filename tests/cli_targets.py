"""Graphs for the CLI tests to load by module:attr."""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

import ledger


class S(TypedDict, total=False):
    x: int


@ledger.tool(name="bump")
def bump(x: int) -> int:
    return x + 1


def _step(state: S) -> S:
    if state["x"] < 0:
        raise ValueError("x must not be negative")
    return {"x": bump(state["x"])}


def _build():
    b = StateGraph(S)
    b.add_node("step", _step)
    b.add_edge(START, "step")
    b.add_edge("step", END)
    return b.compile()


compiled = _build()  # a plain compiled graph, not instrumented
instrumented = ledger.instrument(_build())
not_a_graph = 42


def make():
    return _build()
