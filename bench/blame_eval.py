"""Blame benchmark: how often each method names the injected fault, and its cost.

Each trial records a clean run of a workload, picks one of its LLM or tool
calls at random, and records the run again with that call corrupted
(``ledger.testing.faults``). The guilty step and its node are the ground
truth. Every method then names a step of the failed run:

ledger
    ``ledger.diagnose``: bisection over forked replays (``repeats`` runs per
    counterfactual, majority vote).
scan
    The naive counterfactual: rerun one step at a time (with what depends on
    it), last step first, until a rerun fixes the run. Same answers as
    bisection on clean runs, at linear cost; here for the cost comparison.
random
    A uniformly random LLM or tool call of the failed run.
judge
    An LLM reading the whole trace and naming the step, as in Who&When's
    all-at-once method. Runs only with ``--judge-model`` and an
    OpenAI-compatible endpoint (``--judge-base-url``, key in
    ``LEDGER_JUDGE_API_KEY`` or ``OPENAI_API_KEY``).

Step-level accuracy counts the exact step; agent-level counts its node.

Workloads, on the fake LLM server and local tools:

toy
    The 5-node pipeline (``examples/toy_pipeline.py``); faults hit its LLM
    calls. The run fails if "CORRUPT" reaches the plan, a note or the draft.
chain-N
    N tool calls in sequence, each building on the last; the run fails if
    "BAD" reaches the end. Shows how forks grow with the step count.
orders
    The order-total pipeline (``examples/order_pipeline.py``) on a real
    model (``--model``, an OpenAI-compatible endpoint; key as for the
    judge). Faults change one number in a call's outcome, so the trace
    reads plausibly; the run fails if the reply quotes the wrong total. A
    trial whose clean run already fails is skipped, and so is one whose
    fault doesn't make the run fail. Reruns are as noisy as the model is.

Noise makes counterfactuals unreliable, as reruns with a real model are:
``--recur p`` makes the guilty call go wrong again with probability p when
it is rerun, and ``--flake q`` makes any live call go wrong with probability
q. Without them a rerun call is always clean, and accuracy on the fake
server says how the search behaves, not how often a real fault is found.

    python -m bench.blame_eval [--runs 100] [--recur 0.3] [--flake 0.05] [--repeats 5]
    python -m bench.blame_eval --workloads orders --model gpt-6-luna --price 0.10 0.50 \\
        --judge-model gpt-6.2-sol --judge-price 2 10 --runs 50 --max-dollars 5

``--max-dollars`` caps the whole run (models and judge): no new trial starts
once it is spent.

Writes ``bench/results/blame.json`` (or ``--out``) and prints a table.
"""

from __future__ import annotations

import argparse
import json
import operator
import os
import platform
import random
import re
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from openai import OpenAI

import ledger
from bench.replay_fidelity import _pcts
from examples import toy_pipeline as toy
from examples import order_pipeline as orders
from ledger.blame import Budget, Pricing, wilson
from ledger.blame.budget import usage
from ledger.core.events import body_kind, load_payload
from ledger.core.session import task_coordinates
from ledger.testing import faults
from ledger.testing.fake_llm import FakeLLMServer

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
METHODS = ("ledger", "scan", "random", "judge")
STEP_KINDS = ("llm_call", "tool_call")


@dataclass
class Noise:
    """Faults that hit live calls during counterfactuals (off while recording)."""

    recur: float = 0.0
    flake: float = 0.0
    seed: int = 0
    on: bool = False
    guilty_node: str | None = None
    rng: random.Random = field(default_factory=random.Random)

    def hit(self, node: str) -> bool:
        if not self.on:
            return False
        p = self.recur if node == self.guilty_node else 0.0
        p = 1 - (1 - p) * (1 - self.flake)
        return self.rng.random() < p


@dataclass
class Workload:
    name: str
    graph: Any
    inputs: Callable[[int], dict[str, Any]]
    check: Callable[[Any], bool]
    fault: Callable[[str, random.Random], Callable[[Any], Any]]  # (kind, rng) -> corrupt
    fault_kinds: tuple[str, ...]
    noise: Noise
    describe: str  # what failure means, for the judge
    trace_width: int = 400  # characters of each call's input and output the judge sees


# Workloads


class _NoisyCompletions:
    def __init__(self, inner: Any, noise: Noise) -> None:
        self._inner, self._noise = inner, noise

    def create(self, **kwargs: Any) -> Any:
        resp = self._inner.create(**kwargs)
        if self._noise.hit(task_coordinates()[2]):
            resp = faults.replace_text("CORRUPT " + secrets_hex())(resp)
        return resp


def secrets_hex() -> str:
    return f"{random.getrandbits(32):08x}"


def toy_workload(base_url: str, noise: Noise) -> Workload:
    raw = OpenAI(base_url=f"{base_url}/v1", api_key="fake", max_retries=0)
    noisy = SimpleNamespace(base_url=raw.base_url,
                            chat=SimpleNamespace(completions=_NoisyCompletions(raw.chat.completions,
                                                                               noise)))
    graph = toy.build_graph(ledger.wrap(noisy), tools_url=f"{base_url}/tools", model="fake-model")

    def check(out: Any) -> bool:
        return not any("CORRUPT" in v for v in [out["plan"], out["draft"], *out["notes"]])

    return Workload(
        "toy", graph, lambda i: {"topic": toy.TOPICS[i % len(toy.TOPICS)]}, check,
        lambda kind, rnd: faults.replace_text(lambda old: "CORRUPT " + old),
        ("llm_call",), noise,
        'The run failed because the text "CORRUPT" reached its plan, a note or its draft.',
    )


def orders_workload(client: Any, model: str, seed: int,
                    reasoning_effort: str | None = "low") -> Workload:
    graph = orders.build_graph(ledger.wrap(client), model=model,
                               reasoning_effort=reasoning_effort)
    by_request: dict[str, dict[str, Any]] = {}

    def inputs(i: int) -> dict[str, Any]:
        request, order = orders.make_order(random.Random(f"{seed}-{i}"))
        by_request[request] = order
        return {"request": request}

    def check(out: Any) -> bool:
        order = by_request.get((out or {}).get("request", ""))
        return order is not None and orders.reply_is_right(out.get("reply") or "", order)

    return Workload(
        "orders", graph, inputs, check, lambda kind, rnd: orders.perturb(rnd),
        ("llm_call", "tool_call"), Noise(),
        "The run failed because the reply to the customer quoted the wrong order total "
        "(the catalog prices, tax rates and coupon discounts the tools return are correct "
        "unless a step got them wrong).",
        trace_width=3000,
    )


class ChainState(TypedDict, total=False):
    seed: str
    trail: Annotated[list[str], operator.add]
    value: str


def chain_workload(n: int, noise: Noise) -> Workload:
    @ledger.tool(name="stage")
    def stage(i: int, prev: str) -> str:
        out = f"{prev}>{i}"
        return out + " BAD" if noise.hit(f"s{i}") else out

    def node(i: int):
        def run(state: ChainState) -> ChainState:
            v = stage(i, state.get("value") or state["seed"])
            return {"value": v, "trail": [v]}

        return run

    b = StateGraph(ChainState)
    prev = START
    for i in range(n):
        b.add_node(f"s{i}", node(i))
        b.add_edge(prev, f"s{i}")
        prev = f"s{i}"
    b.add_edge(prev, END)
    graph = ledger.instrument(b.compile())
    return Workload(
        f"chain-{n}", graph, lambda i: {"seed": f"x{i}"},
        lambda out: "BAD" not in out["value"],
        lambda kind, rnd: (lambda v: v + " BAD"), ("tool_call",), noise,
        'The run failed because "BAD" reached its final value.',
    )


# Methods


def scan(run_id: str, w: Workload, store: ledger.EventStore, repeats: int = 1,
         threshold: float = 0.5, pricing: Pricing | None = None) -> tuple[Any, int, int, float]:
    """Rerun one step at a time, last first, with the same repeats and threshold
    as ledger. Returns (step, forks, live calls, dollars)."""
    cands = [s for s in ledger.steps(run_id, store) if s.kind in ledger.blame.CANDIDATE_KINDS]
    inputs = w.inputs(0)
    for ev in store.events(run_id):
        if body_kind(ev) == "run_started":
            inputs = load_payload(ev.run_started.input)
            break
    forks = live = 0
    dollars = 0.0
    need = threshold * repeats
    for s in reversed(cands):
        passed = 0
        for trial in range(repeats):
            if passed > need or passed + (repeats - trial) <= need:
                break
            with ledger.fork(run_id, at=s.key, store=store, on_divergence="live") as session:
                try:
                    passed += bool(w.check(w.graph.invoke(inputs)))
                except Exception:
                    pass
            forks += 1
            live += session.stats.live_calls
            if pricing:
                dollars += pricing.dollars(*usage(store, session.run_id))
        if passed > need:
            return s, forks, live, dollars
    return None, forks, live, dollars


def trace_text(run_id: str, store: ledger.EventStore, width: int = 400) -> tuple[str, list[Any]]:
    """The run as numbered steps, for the judge; and the steps by number."""
    lines, numbered = [], []
    for s in ledger.steps(run_id, store):
        if s.kind not in STEP_KINDS:
            continue
        ev = next(e for e in store.events(run_id) if e.index == s.index)
        body = getattr(ev, s.kind)
        req = load_payload(body.request if s.kind == "llm_call" else body.args)
        if s.kind == "llm_call":
            req = (req.get("messages") or [{}])[-1].get("content", "")
        out = s.summary if s.kind != "llm_call" else (
            (load_payload(body.response) or {}).get("choices", [{}])[0]
            .get("message", {}).get("content", "")
        )
        numbered.append(s)
        lines.append(f"[{len(numbered)}] agent={s.node} {s.kind} {s.label}\n"
                     f"  input: {str(req)[:width]}\n  output: {str(out)[:width]}")
    return "\n".join(lines), numbered


JUDGE_PROMPT = """You are debugging a failed multi-agent run. {describe}
Below is every step of the run in order. Find the step whose output caused the
failure: the earliest step that, had it been correct, would have let the run succeed.

{trace}

Answer with JSON only: {{"step": <step number>, "agent": "<agent name>"}}"""


def judge_options(temperature: float | None, reasoning_effort: str | None) -> dict[str, Any]:
    """Sampling options for the judge call. Both are left out unless given:
    reasoning models such as the GPT-6.2 family reject a custom temperature."""
    out: dict[str, Any] = {}
    if temperature is not None:
        out["temperature"] = temperature
    if reasoning_effort is not None:
        out["reasoning_effort"] = reasoning_effort
    return out


def judge(run_id: str, w: Workload, store: ledger.EventStore, client: Any, model: str,
          pricing: Pricing | None, options: dict[str, Any] | None = None,
          ) -> tuple[Any, str | None, float]:
    """Ask the judge model. Returns (step or None, agent or None, dollars)."""
    trace, numbered = trace_text(run_id, store, w.trace_width)
    resp = client.chat.completions.create(
        model=model, **(options or {}),
        messages=[{"role": "user", "content": JUDGE_PROMPT.format(describe=w.describe,
                                                                   trace=trace)}],
    )
    text = resp.choices[0].message.content or ""
    cost = 0.0
    if pricing and resp.usage:
        cost = pricing.dollars(resp.usage.prompt_tokens, resp.usage.completion_tokens)
    m = re.search(r"\{.*\}", text, re.S)
    try:
        answer = json.loads(m.group(0)) if m else {}
    except json.JSONDecodeError:
        answer = {}
    step = None
    n = answer.get("step")
    if isinstance(n, int) and 1 <= n <= len(numbered):
        step = numbered[n - 1]
    agent = answer.get("agent") if isinstance(answer.get("agent"), str) else None
    return step, agent or (step.node if step else None), cost


# The benchmark


@dataclass
class Tally:
    step_hits: int = 0
    agent_hits: int = 0
    trials: int = 0
    forks: list[float] = field(default_factory=list)
    live_calls: list[float] = field(default_factory=list)
    dollars: list[float] = field(default_factory=list)
    status: dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, step_ok: bool, agent_ok: bool, forks: int = 0, live: int = 0,
            dollars: float = 0.0, status: str | None = None) -> None:
        with self._lock:
            self._add(step_ok, agent_ok, forks, live, dollars, status)

    def _add(self, step_ok: bool, agent_ok: bool, forks: int, live: int, dollars: float,
             status: str | None) -> None:
        self.trials += 1
        self.step_hits += step_ok
        self.agent_hits += agent_ok
        self.forks.append(forks)
        self.live_calls.append(live)
        self.dollars.append(dollars)
        if status:
            self.status[status] = self.status.get(status, 0) + 1

    def summary(self) -> dict[str, Any]:
        def acc(hits: int) -> dict[str, float]:
            lo, hi = wilson(hits, self.trials)
            return {"rate": round(hits / self.trials, 3) if self.trials else 0.0,
                    "ci95": [round(lo, 3), round(hi, 3)]}

        return {
            "trials": self.trials,
            "step_accuracy": acc(self.step_hits),
            "agent_accuracy": acc(self.agent_hits),
            "forks": _pcts(self.forks),
            "live_calls": _pcts(self.live_calls),
            "dollars": {"mean": round(sum(self.dollars) / max(1, self.trials), 6)},
            "status": dict(sorted(self.status.items())),
        }


@dataclass
class Spend:
    """Dollars a benchmark run has spent, by where they went."""

    pricing: Pricing | None = None  # the workload's model
    cap: float | None = None
    by: dict[str, float] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, what: str, dollars: float) -> None:
        with self._lock:
            self.by[what] = self.by.get(what, 0.0) + dollars

    def run(self, what: str, store: ledger.EventStore, run_id: str) -> None:
        if self.pricing:
            self.add(what, self.pricing.dollars(*usage(store, run_id)))

    @property
    def total(self) -> float:
        return sum(self.by.values())

    @property
    def exhausted(self) -> bool:
        return self.cap is not None and self.total >= self.cap


def in_order(steps: Iterable[Any]) -> list[Any]:
    """Calls sorted by superstep, node and seq. Log order isn't fixed: calls of
    nodes that run in parallel land in the log in whatever order their threads
    reach it, so a pick by position in the log could differ between reruns."""
    return sorted(steps, key=lambda s: (s.superstep, s.node, s.seq, s.key))


def run_workload(w: Workload, runs: int, seed: int, repeats: int, threshold: float,
                 budget: Budget, store: ledger.EventStore, judge_cfg: dict[str, Any] | None,
                 methods: tuple[str, ...], spend: Spend | None = None,
                 workers: int = 1) -> dict[str, Any]:
    """Run ``runs`` trials, ``workers`` at a time. Each trial draws its fault
    and its random pick from its own seeds, over calls in a fixed order (see
    ``in_order``), so neither depends on timing. Noise (``--recur``,
    ``--flake``) shares one generator, so it runs one trial at a time."""
    spend = spend or Spend()
    if w.noise.recur or w.noise.flake:
        workers = 1
    w.noise.rng.seed(seed + 1)
    tallies = {m: Tally() for m in methods}
    counts = {"attempted": 0, "clean_failed": 0, "fault_masked": 0, "done": 0}
    lock = threading.Lock()
    every = max(1, runs // 20)

    def count(key: str) -> None:
        with lock:
            counts[key] += 1

    def trial(i: int) -> None:
        if spend.exhausted:
            return
        count("attempted")
        rnd = random.Random(f"{seed}-{i}")
        inputs = w.inputs(i)
        w.noise.on = False
        with ledger.record(store) as clean:
            try:
                ok = w.check(w.graph.invoke(inputs))
            except Exception:
                ok = False
        spend.run("record", store, clean.run_id)
        if not ok:
            count("clean_failed")  # failed with no fault: no ground truth
            return
        eligible = in_order(s for s in ledger.steps(clean.run_id, store)
                            if s.kind in w.fault_kinds)
        target = rnd.choice(eligible)
        fault = faults.Fault(key=target.key, corrupt=w.fault(target.kind, rnd))
        with ledger.record(store) as rec, faults.inject(fault):
            try:
                out = w.graph.invoke(inputs)
            except Exception:
                out = None
        spend.run("record", store, rec.run_id)
        if not fault.fired or (out is not None and w.check(out)):
            count("fault_masked")  # the fault didn't make the run fail
            return
        truth = fault.truth()
        w.noise.guilty_node = truth["node"]
        w.noise.on = True

        def score(m: str, step: Any, agent: str | None, **cost: Any) -> None:
            tallies[m].add(step is not None and step.key == truth["step"],
                           agent == truth["node"], **cost)
            spend.add(m, cost.get("dollars", 0.0))

        if "ledger" in tallies:
            d = ledger.diagnose(rec.run_id, w.graph, w.check, store=store, repeats=repeats,
                                threshold=threshold, budget=budget, pricing=spend.pricing)
            score("ledger", d.step, d.step.node if d.step else None, forks=d.forks,
                  live=d.live_calls, dollars=d.dollars, status=d.status)
        if "scan" in tallies:
            step, forks, live, dollars = scan(rec.run_id, w, store, repeats, threshold,
                                              spend.pricing)
            score("scan", step, step.node if step else None, forks=forks, live=live,
                  dollars=dollars)
        if "random" in tallies:
            calls = in_order(s for s in ledger.steps(rec.run_id, store) if s.kind in STEP_KINDS)
            pick = random.Random(f"{seed}-{i}-random").choice(calls)
            score("random", pick, pick.node)
        if "judge" in tallies and judge_cfg:
            try:
                step, agent, cost = judge(rec.run_id, w, store, **judge_cfg)
            except Exception as exc:  # an API error: no answer
                print(f"  judge failed: {type(exc).__name__}: {exc}"[:200], file=sys.stderr)
                step, agent, cost = None, None, 0.0
            score("judge", step, agent, dollars=cost)
        w.noise.on = False

    def run(i: int) -> None:
        trial(i)
        with lock:
            counts["done"] += 1
            done = counts["done"]
        if done % every == 0:
            print(f"  {w.name}: {done}/{runs}, ${spend.total:.2f} spent", file=sys.stderr,
                  flush=True)

    if workers <= 1:
        for i in range(runs):
            run(i)
    else:
        with ThreadPoolExecutor(workers, thread_name_prefix="trial") as pool:
            list(pool.map(run, range(runs)))
    skipped = {k: counts[k] for k in ("clean_failed", "fault_masked")}
    return {"runs": runs, "attempted": counts["attempted"], "skipped": skipped,
            "faulted": counts["attempted"] - sum(skipped.values()),
            "methods": {m: t.summary() for m, t in tallies.items() if t.trials}}


def render(rep: dict[str, Any]) -> str:
    lines = [
        f"Blame: {rep['runs']} faulted runs per workload, recur={rep['noise']['recur']}, "
        f"flake={rep['noise']['flake']}, repeats={rep['repeats']}, "
        f"threshold={rep['threshold']}",
        f"{'workload':<10}{'method':<8}{'step acc (95% CI)':>24}{'agent acc':>12}"
        f"{'forks mean/p95':>17}{'live calls':>12}{'$/diagnosis':>13}",
    ]
    for name, res in rep["workloads"].items():
        for m, s in res["methods"].items():
            st, ag = s["step_accuracy"], s["agent_accuracy"]
            ci = f"{st['rate']:.2f} ({st['ci95'][0]:.2f}-{st['ci95'][1]:.2f})"
            forks = f"{s['forks']['mean']:.1f} / {s['forks']['p95']:.0f}" if m in ("ledger", "scan") else "-"
            live = f"{s['live_calls']['mean']:.1f}" if m in ("ledger", "scan") else "-"
            lines.append(f"{name:<10}{m:<8}{ci:>24}{ag['rate']:>12.2f}{forks:>17}{live:>12}"
                         f"{s['dollars']['mean']:>13.4f}")
        lines.append(f"{'':<10}{res['faulted']} faulted of {res['attempted']} tried; skipped: "
                     + ", ".join(f"{k} {v}" for k, v in res["skipped"].items()))
    lines.append(f"spent: " + ", ".join(f"{k} ${v:.3f}" for k, v in rep["dollars"].items()))
    return "\n".join(lines)


JUDGE_KEYS = ("LEDGER_JUDGE_API_KEY", "OPENAI_API_KEY")


def _judge_key(env_file: Path = ROOT / ".env") -> str | None:
    """The judge's API key, from the environment or the repo's .env (never printed)."""
    for name in JUDGE_KEYS:
        if os.environ.get(name):
            return os.environ[name]
    if env_file.exists():
        found = {}
        for line in env_file.read_text().splitlines():
            name, sep, value = line.strip().partition("=")
            if sep and name in JUDGE_KEYS and value:
                found[name] = value.strip().strip("'\"")
        for name in JUDGE_KEYS:
            if name in found:
                return found[name]
    return None


def _judge_config(args: argparse.Namespace) -> dict[str, Any] | None:
    if not args.judge_model:
        return None
    key = _judge_key() or "none"
    client = OpenAI(base_url=args.judge_base_url, api_key=key, max_retries=2)
    pricing = None
    if args.judge_price:
        pricing = Pricing(*args.judge_price)
    return {"client": client, "model": args.judge_model, "pricing": pricing,
            "options": judge_options(args.judge_temperature, args.judge_reasoning_effort)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.blame_eval", description=__doc__.splitlines()[0])
    p.add_argument("--runs", type=int, default=100, help="faulted runs per workload")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--repeats", type=int, default=1, help="runs per counterfactual")
    p.add_argument("--threshold", type=float, default=0.5,
                   help="share of a counterfactual's runs that must pass (0: any one)")
    p.add_argument("--recur", type=float, default=0.0)
    p.add_argument("--flake", type=float, default=0.0)
    p.add_argument("--max-forks", type=int, default=60, help="budget per diagnosis")
    p.add_argument("--workloads", default="toy,chain-8,chain-16,chain-32,chain-64")
    p.add_argument("--methods", default="ledger,scan,random,judge")
    p.add_argument("--model", help="the orders workload's model")
    p.add_argument("--base-url", help="its OpenAI-compatible endpoint (default: OpenAI)")
    p.add_argument("--price", type=float, nargs=2, metavar=("IN", "OUT"),
                   help="its dollars per million input and output tokens")
    p.add_argument("--reasoning-effort", default="low", help="its reasoning effort ('' for none)")
    p.add_argument("--max-dollars", type=float, help="cap on the whole run, models and judge")
    p.add_argument("--store", type=Path, help="keep the runs in this event store")
    p.add_argument("--workers", type=int, default=1, help="trials run at once")
    p.add_argument("--judge-model")
    p.add_argument("--judge-base-url")
    p.add_argument("--judge-price", type=float, nargs=2, metavar=("IN", "OUT"),
                   help="judge dollars per million input and output tokens")
    p.add_argument("--judge-temperature", type=float, help="left out unless given")
    p.add_argument("--judge-reasoning-effort", help="e.g. low, medium, high")
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)

    methods = tuple(m for m in args.methods.split(",") if m)
    judge_cfg = _judge_config(args)
    if "judge" in methods and judge_cfg is None:
        methods = tuple(m for m in methods if m != "judge")
        print("judge: not run (no --judge-model)", file=sys.stderr)
    budget = Budget(forks=args.max_forks)
    specs = [w for w in args.workloads.split(",") if w]
    if "orders" in specs and not args.model:
        p.error("the orders workload needs --model")
    spend = Spend(Pricing(*args.price) if args.price else None, args.max_dollars)
    started = time.monotonic()
    results: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="ledger-blame-") as tmp, \
            FakeLLMServer(latency_ms=(0, 2)) as server, \
            ledger.EventStore(args.store or Path(tmp) / "ledger.db") as store:
        for spec in specs:
            noise = Noise(args.recur, args.flake)
            if spec == "toy":
                w = toy_workload(server.base_url, noise)
            elif spec.startswith("chain-"):
                w = chain_workload(int(spec.split("-")[1]), noise)
            elif spec == "orders":
                client = OpenAI(base_url=args.base_url, api_key=_judge_key() or "none",
                                max_retries=2)
                w = orders_workload(client, args.model, args.seed,
                                    args.reasoning_effort or None)
            else:
                p.error(f"unknown workload {spec!r}")
            results[w.name] = run_workload(w, args.runs, args.seed, args.repeats,
                                           args.threshold, budget, store, judge_cfg, methods,
                                           spend, args.workers)
    rep = {
        "benchmark": "blame",
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "runs": args.runs,
        "seed": args.seed,
        "repeats": args.repeats,
        "workers": args.workers,
        "threshold": args.threshold,
        "noise": {"recur": args.recur, "flake": args.flake},
        "budget": {"forks": args.max_forks},
        "judge": args.judge_model,
        "model": args.model,
        "reasoning_effort": args.reasoning_effort if args.model else None,
        "dollars": {k: round(v, 4) for k, v in sorted(spend.by.items())} | {
            "total": round(spend.total, 4)},
        "max_dollars": args.max_dollars,
        "wall_s": round(time.monotonic() - started, 1),
        "machine": {"platform": platform.platform(), "python": platform.python_version()},
        "workloads": results,
    }
    out = args.out or RESULTS / "blame.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1) + "\n")
    print(render(rep))
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
