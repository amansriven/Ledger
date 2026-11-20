"""Property tests: replay, fork and divergence on randomly generated graphs.

Hypothesis builds layered LangGraph graphs: each layer is a superstep of 1-3
parallel nodes, each node wired from a random subset of the layer before.
Every node runs a random list of operations, each one an intercepted call:

    tool   a tool returning a fresh random number
    fail   a tool that raises ValueError, caught by the node
    time   ledger.time()
    uuid   ledger.uuid4()
    rng    a draw from ledger.rng() (only the task's first draw is logged)

Each call's request is built from the node's input state and the values it
has read so far, so a value served wrong anywhere changes later requests and
the output. Nodes sleep a random moment first, so branches finish in a
different order from run to run, and half the graphs run async.

The expected live/replayed split for forks and divergences is computed from
the graph's layers, not from ``ledger.replay.engine.is_downstream``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import operator
import os
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from langgraph.graph import END, START, StateGraph

import ledger
from ledger.core.events import body_kind
from ledger.core.store import step_key
from ledger.replay.engine import DivergenceReason

OPS = ("tool", "fail", "time", "uuid", "rng")
# 60 examples per property by default; HYPOTHESIS_PROFILE=thorough for 1000.
settings.register_profile(
    "default", max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)
settings.register_profile("thorough", settings.get_profile("default"), max_examples=1000)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))
SETTINGS = settings()


@dataclass(frozen=True)
class Spec:
    # layers[l][i] is the op list of node "n{l}_{i}", which runs in superstep l + 1.
    layers: tuple[tuple[tuple[str, ...], ...], ...]
    # preds[l][i]: indices in layer l - 1 that have an edge to node (l, i).
    preds: tuple[tuple[tuple[int, ...], ...], ...]
    use_async: bool

    def nodes(self):
        for layer, nodes in enumerate(self.layers):
            for i, ops in enumerate(nodes):
                yield layer, i, f"n{layer}_{i}", ops

    def layer_of(self, node: str) -> int:
        return int(node[1:].split("_")[0])

    def ops_of(self, node: str) -> tuple[str, ...]:
        layer, i = map(int, node[1:].split("_"))
        return self.layers[layer][i]


@st.composite
def specs(draw) -> Spec:
    node = st.lists(st.sampled_from(OPS), max_size=3).map(tuple)
    layers = draw(
        st.lists(st.lists(node, min_size=1, max_size=3).map(tuple), min_size=1, max_size=4)
    )
    preds: list[tuple[tuple[int, ...], ...]] = [tuple(() for _ in layers[0])]
    for layer in range(1, len(layers)):
        n_prev = len(layers[layer - 1])
        node_preds = []
        for _ in layers[layer]:
            # A subset of the previous layer, never empty: a mask plus one
            # index that is always in, so nothing gets rejected.
            mask = draw(st.lists(st.booleans(), min_size=n_prev, max_size=n_prev))
            always = draw(st.integers(0, n_prev - 1))
            node_preds.append(tuple(j for j in range(n_prev) if mask[j] or j == always))
        preds.append(tuple(node_preds))
    return Spec(tuple(layers), tuple(preds), draw(st.booleans()))


class S(TypedDict, total=False):
    seed: int
    notes: Annotated[list[str], operator.add]


def _digest(*parts: Any) -> str:
    return hashlib.sha256(json.dumps(parts, default=str).encode()).hexdigest()[:10]


_jitter = random.Random()


def build(spec: Spec, salt: dict[str, str] | None = None):
    """The graph for ``spec``, and a counter of tool bodies actually executed.

    ``salt`` appends a suffix to every tool argument of the named nodes, like
    an edit to those nodes' code.
    """
    salt = salt or {}
    executed: Counter[str] = Counter()

    @ledger.tool(name="work")
    def work(arg: str) -> float:
        executed["work"] += 1
        return random.random()

    @ledger.tool(name="fail")
    def fail(arg: str) -> None:
        executed["fail"] += 1
        raise ValueError(f"no {arg}")

    @ledger.tool(name="work")
    async def awork(arg: str) -> float:
        executed["work"] += 1
        await asyncio.sleep(0)
        return random.random()

    @ledger.tool(name="fail")
    async def afail(arg: str) -> None:
        executed["fail"] += 1
        raise ValueError(f"no {arg}")

    def arg_for(name: str, i: int, state: S, values: list[Any]) -> str:
        return f"{name}:{i}:{_digest(state['seed'], state.get('notes', []), values)}{salt.get(name, '')}"

    def read(op: str) -> Any:
        if op == "time":
            return ledger.time()
        if op == "uuid":
            return str(ledger.uuid4())
        return ledger.rng().random()

    def sync_node(name: str, ops: tuple[str, ...]):
        def node(state: S) -> S:
            time.sleep(_jitter.uniform(0, 0.002))
            values: list[Any] = []
            for i, op in enumerate(ops):
                if op == "tool":
                    values.append(work(arg_for(name, i, state, values)))
                elif op == "fail":
                    try:
                        fail(arg_for(name, i, state, values))
                    except ValueError as exc:
                        values.append(f"caught: {exc}")
                else:
                    values.append(read(op))
            return {"notes": [f"{name}={_digest(values)}"]}

        return node

    def async_node(name: str, ops: tuple[str, ...]):
        async def node(state: S) -> S:
            await asyncio.sleep(_jitter.uniform(0, 0.002))
            values: list[Any] = []
            for i, op in enumerate(ops):
                if op == "tool":
                    values.append(await awork(arg_for(name, i, state, values)))
                elif op == "fail":
                    try:
                        await afail(arg_for(name, i, state, values))
                    except ValueError as exc:
                        values.append(f"caught: {exc}")
                else:
                    values.append(read(op))
            return {"notes": [f"{name}={_digest(values)}"]}

        return node

    make = async_node if spec.use_async else sync_node
    b = StateGraph(S)
    has_successor: set[str] = set()
    for layer, i, name, ops in spec.nodes():
        b.add_node(name, make(name, ops))
        if layer == 0:
            b.add_edge(START, name)
        else:
            for p in spec.preds[layer][i]:
                pred = f"n{layer - 1}_{p}"
                b.add_edge(pred, name)
                has_successor.add(pred)
    for _, _, name, _ in spec.nodes():
        if name not in has_successor:
            b.add_edge(name, END)
    return ledger.instrument(b.compile()), executed


def invoke(spec: Spec, graph, seed: int) -> Any:
    if spec.use_async:
        return asyncio.run(graph.ainvoke({"seed": seed}))
    return graph.invoke({"seed": seed})


def calls(store: ledger.EventStore, run_id: str) -> dict[str, tuple[str, str, str, bool]]:
    """step key -> (kind, request match key, outcome digest, from_log)."""
    out = {}
    for ev in store.events(run_id):
        kind = body_kind(ev)
        if kind == "tool_call":
            body = ev.tool_call
            outcome = (
                f"!{body.error.type}:{body.error.message}"
                if body.HasField("error")
                else body.result.sha256
            )
            match = body.args_key
        elif kind == "nondet":
            outcome, match = ev.nondet.value.sha256, ev.nondet.source
        else:
            continue
        out[step_key(ev.step)] = (kind, match, outcome, ev.from_log)
    return out


def _parse(key: str) -> tuple[str, int]:
    """(node, seq) of a top-level step key, "/superstep/node/path#seq"."""
    return key.split("/")[2], int(key.rsplit("#", 1)[1])


def downstream(spec: Spec, key: str, of: tuple[str, int]) -> bool:
    """Whether the call at ``key`` comes after ``of`` = (node, seq), by layers."""
    node, seq = _parse(key)
    target_node, target_seq = of
    if node == target_node:
        return seq >= target_seq
    return spec.layer_of(node) > spec.layer_of(target_node)


def record(spec: Spec, seed: int, store: ledger.EventStore):
    graph, _ = build(spec)
    with ledger.record(store) as rec:
        out = invoke(spec, graph, seed)
    return rec.run_id, out


@SETTINGS
@given(spec=specs(), seed=st.integers(0, 2**31))
def test_replay_reproduces_every_call(spec, seed):
    with ledger.EventStore(":memory:") as store:
        run_id, recorded = record(spec, seed, store)

        graph, executed = build(spec)
        with ledger.replay(run_id, store) as rep:
            replayed = invoke(spec, graph, seed)

        assert replayed == recorded
        assert not executed, "a tool body ran during replay"
        assert rep.stats.live_calls == rep.stats.live_nondet == 0
        assert rep.divergences == []
        rec_calls, rep_calls = calls(store, run_id), calls(store, rep.run_id)
        assert {k: v[:3] for k, v in rep_calls.items()} == {k: v[:3] for k, v in rec_calls.items()}
        assert all(v[3] for v in rep_calls.values())


@SETTINGS
@given(spec=specs(), seed=st.integers(0, 2**31), data=st.data())
def test_fork_reruns_exactly_the_downstream_calls(spec, seed, data):
    with ledger.EventStore(":memory:") as store:
        run_id, _ = record(spec, seed, store)
        rec_calls = calls(store, run_id)
        if not rec_calls:
            return
        target = data.draw(st.sampled_from(sorted(rec_calls)), label="fork step")

        graph, _ = build(spec)
        with ledger.fork(run_id, at=target, store=store) as fk:
            forked = invoke(spec, graph, seed)

        fork_calls = calls(store, fk.run_id)
        assert set(fork_calls) == set(rec_calls)  # the same calls, live or not
        live = {k for k, v in fork_calls.items() if not v[3]}
        assert live == {k for k in rec_calls if downstream(spec, k, _parse(target))}
        for k in set(rec_calls) - live:
            assert fork_calls[k][:3] == rec_calls[k][:3]

        # The fork's log is a recording in its own right.
        graph, executed = build(spec)
        with ledger.replay(fk.run_id, store) as again:
            assert invoke(spec, graph, seed) == forked
        assert not executed


def _first_tool_seq(ops: tuple[str, ...]) -> int | None:
    """Seq of a node's first tool call: rng logs only the task's first draw."""
    seq, seeded = 0, False
    for op in ops:
        if op in ("tool", "fail"):
            return seq
        if op == "rng":
            if seeded:
                continue
            seeded = True
        seq += 1
    return None


@SETTINGS
@given(spec=specs(), seed=st.integers(0, 2**31), data=st.data())
def test_edited_node_goes_live_from_its_first_changed_call(spec, seed, data):
    with ledger.EventStore(":memory:") as store:
        run_id, recorded = record(spec, seed, store)
        rec_calls = calls(store, run_id)
        names = [name for _, _, name, _ in spec.nodes()]
        edited = data.draw(st.sampled_from(names), label="edited node")

        graph, _ = build(spec, salt={edited: "!"})
        with ledger.replay(run_id, store, on_divergence="live") as rep:
            replayed = invoke(spec, graph, seed)

        rep_calls = calls(store, rep.run_id)
        live = {k for k, v in rep_calls.items() if not v[3]}
        first = _first_tool_seq(spec.ops_of(edited))
        if first is None:  # the edit touches no request
            assert rep.divergences == []
            assert live == set()
            assert replayed == recorded
            return
        [point] = rep.divergences
        assert point.reason is DivergenceReason.CHANGED_REQUEST
        assert _parse(point.step) == (edited, first)
        assert live == {k for k in rec_calls if downstream(spec, k, (edited, first))}
        assert set(rep_calls) == set(rec_calls)


def test_specs_cover_the_interesting_shapes():
    """A smoke check that the strategy reaches fan-out, async and every op."""
    seen = {"fanout": False, "async": False, **{op: False for op in OPS}}

    @settings(max_examples=200, database=None)
    @given(spec=specs())
    def look(spec):
        seen["async"] |= spec.use_async
        seen["fanout"] |= any(len(layer) > 1 for layer in spec.layers)
        for _, _, _, ops in spec.nodes():
            for op in ops:
                seen[op] = True

    look()
    assert all(seen.values()), seen
