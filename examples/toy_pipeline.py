"""Toy 5-node research pipeline with a parallel fan-out.

    plan -> (research_web | research_docs | research_code) -> write

Each research branch calls a search tool and the LLM, and the three run in
parallel in one superstep. The fake LLM server answers after a random delay,
so the branches finish in a different order from run to run. The `write`
node's prompt includes every note in state order, so if that order drifted,
replay would see a changed request and stop.

`plan` picks a tone with ``ledger.rng()`` and `write` stamps the report with
``ledger.uuid4()`` and ``ledger.now()``, so replay also depends on recorded
randomness, ids and clock reads.

Usage (fake server on :8787, see ledger.testing.fake_llm):

    python -m examples.toy_pipeline record --runs 5
    python -m examples.toy_pipeline replay --all
    python -m examples.toy_pipeline list
"""

from __future__ import annotations

import argparse
import operator
import os
import random
import sys
from datetime import UTC
from typing import Annotated, Any, TypedDict

import httpx
from langgraph.graph import END, START, StateGraph
from openai import OpenAI

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.intercept.langgraph import InstrumentedGraph

ANGLES = ("web", "docs", "code")
TONES = ("brief", "detailed", "skeptical")
TOPICS = (
    "vector clocks",
    "write-ahead logging",
    "idempotency keys",
    "saga compensation",
    "delta debugging",
    "speculative execution",
    "consistent hashing",
    "fencing tokens",
)


class State(TypedDict, total=False):
    topic: str
    plan: str
    notes: Annotated[list[str], operator.add]
    draft: str


def _text(resp: Any) -> str:
    return resp.choices[0].message.content or ""


def build_graph(llm: Any, *, tools_url: str, model: str) -> InstrumentedGraph:
    """Build the pipeline. ``llm`` should already be wrapped with ``ledger.wrap``."""

    @ledger.tool(name="search")
    def search(query: str) -> dict[str, Any]:
        r = httpx.get(f"{tools_url}/search", params={"q": query}, timeout=10)
        r.raise_for_status()
        return r.json()

    def ask(prompt: str) -> str:
        return _text(
            llm.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )
        )

    def plan(state: State) -> State:
        tone = ledger.rng().choice(TONES)
        return {"plan": ask(f"In a {tone} tone, list three research angles for: {state['topic']}")}

    def research(angle: str):
        def node(state: State) -> State:
            hits = search(f"{state['topic']} {angle}")
            summary = ask(
                f"Summarize the {angle} findings on {state['topic']}: "
                + "; ".join(hits["results"])
            )
            return {"notes": [f"{angle}: {summary}"]}

        node.__name__ = f"research_{angle}"
        return node

    def write(state: State) -> State:
        notes = "\n".join(state["notes"])
        header = f"Report {ledger.uuid4()} ({ledger.now(UTC):%Y-%m-%d %H:%M} UTC)"
        draft = ask(f"Write a short report on {state['topic']} from:\n{notes}")
        return {"draft": f"{header}\n{draft}"}

    builder = StateGraph(State)
    builder.add_node("plan", plan)
    builder.add_node("write", write)
    builder.add_edge(START, "plan")
    for angle in ANGLES:
        name = f"research_{angle}"
        builder.add_node(name, research(angle))
        builder.add_edge("plan", name)
        builder.add_edge(name, "write")
    builder.add_edge("write", END)
    return ledger.instrument(builder.compile())


def graph_from_env() -> InstrumentedGraph:
    base_url = os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:8787/v1")
    llm = ledger.wrap(
        OpenAI(base_url=base_url, api_key=os.environ.get("OPENAI_API_KEY", "fake"), max_retries=0)
    )
    return build_graph(
        llm,
        tools_url=os.environ.get("TOOLS_URL", "http://127.0.0.1:8787/tools"),
        model=os.environ.get("LEDGER_MODEL", "fake-model"),
    )


def recorded_runs(store: ledger.EventStore) -> list[str]:
    """Run ids that were recorded (not replays), oldest first."""
    out = []
    for run_id in store.run_ids():
        first = next(iter(store.events(run_id)), None)
        if first is not None and body_kind(first) == "run_started":
            if first.run_started.mode == pb.MODE_RECORD:
                out.append(run_id)
    return out


def replay_run(graph: Any, store: ledger.EventStore, run_id: str) -> tuple[bool, str]:
    """Replay one run. Returns (ok, one-line report)."""
    try:
        with ledger.replay(run_id, store) as session:
            assert session.replay_index is not None and session.replay_index.input is not None
            graph.invoke(load_payload(session.replay_index.input))
    except Exception as exc:  # report and keep going
        return False, f"FAIL {run_id}  {type(exc).__name__}: {exc}"
    matched = session.output_sha256 == session.replay_index.output_sha256
    ok = matched and session.stats.live_calls == 0
    status = "OK  " if ok else "FAIL"
    return ok, (
        f"{status} {run_id}  replayed={session.stats.replayed_calls} "
        f"live={session.stats.live_calls} output={'match' if matched else 'MISMATCH'}"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="toy_pipeline", description="Record and replay the toy pipeline.")
    sub = p.add_subparsers(dest="cmd", required=True)
    rec = sub.add_parser("record", help="record new runs against the live (fake) endpoints")
    rec.add_argument("--runs", type=int, default=1)
    rec.add_argument("--topic", help="fixed topic (default: random per run)")
    rp = sub.add_parser("replay", help="replay recorded runs from the log")
    rp.add_argument("run_ids", nargs="*")
    rp.add_argument("--all", action="store_true", help="replay every recorded run")
    sub.add_parser("list", help="list recorded runs")
    args = p.parse_args(argv)

    store = ledger.EventStore()
    graph = graph_from_env()

    if args.cmd == "record":
        for _ in range(args.runs):
            topic = args.topic or random.choice(TOPICS)
            with ledger.record(store) as session:
                graph.invoke({"topic": topic})
            print(f"recorded {session.run_id}  topic={topic!r} calls={session.stats.live_calls}")
        return 0

    if args.cmd == "list":
        for run_id in recorded_runs(store):
            n = sum(1 for _ in store.events(run_id))
            print(f"{run_id}  events={n}")
        return 0

    run_ids = recorded_runs(store) if args.all else args.run_ids
    if not run_ids:
        if args.all:
            print(f"no recorded runs in {store.path}", file=sys.stderr)
            return 1
        p.error("give run ids or --all")
    failures = 0
    for run_id in run_ids:
        ok, line = replay_run(graph, store, run_id)
        failures += not ok
        print(line)
    print(f"{len(run_ids) - failures}/{len(run_ids)} replayed identically")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
