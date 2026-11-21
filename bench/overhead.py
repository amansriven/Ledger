"""Overhead benchmark: the latency Ledger adds per step, and the log it writes.

Runs the 5-node pipeline (``examples/toy_pipeline.py``) N times in each of
three modes, interleaved, against the fake LLM and search server answering
with no added delay. What's timed is then LangGraph, the local HTTP round
trips and Ledger:

off
    The same builder compiled as plain LangGraph, with an unwrapped OpenAI
    client. Ledger's tool decorator and its clock, uuid and rng shims are
    still in the code and pass straight through outside a session.
record
    ``ledger.instrument`` and ``ledger.wrap``, recording into an EventStore
    on local disk (WAL, ``synchronous=FULL``: every event is fsynced).
replay
    The run just recorded, replayed from its log: no call reaches the network.

Each iteration runs the same topic in every mode; ``off`` alternates between
going first and last, so drift and warm caches don't favour one mode.

Per step is a run's time over its 5 node executions. The three research
nodes run in parallel, so it's a share of wall time, not one node's time.
Added latency is a mode's percentile minus ``off``'s, at p50 and p95. Per
call divides the added run time by the run's LLM and tool calls (8) instead;
the run also logs its clock, uuid and rng reads, 13 events in all with its
start and finish. Log size is the run's events plus the blobs they refer to.

    python -m bench.overhead [--runs 1000] [--warmup 20] [--latency-ms 0]

Writes ``bench/results/overhead.json`` and prints a table. Exit status is 0
unless a replay wasn't identical.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from openai import OpenAI

import ledger
from bench.replay_fidelity import _pcts
from examples import toy_pipeline as toy
from ledger.testing.fake_llm import FakeLLMServer

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
STEPS = 5  # plan, three research branches, write
MODES = ("off", "record", "replay")


def _graphs(base_url: str) -> tuple[Any, Any]:
    def client() -> OpenAI:
        return OpenAI(base_url=f"{base_url}/v1", api_key="fake", max_retries=0)

    tools_url = f"{base_url}/tools"
    off = toy.build_builder(client(), tools_url=tools_url, model="fake-model").compile()
    on = toy.build_graph(ledger.wrap(client()), tools_url=tools_url, model="fake-model")
    return off, on


def measure(runs: int, warmup: int, latency_ms: float, data: Path) -> dict[str, Any]:
    """Time ``runs`` runs per mode, after ``warmup`` untimed iterations."""
    ms: dict[str, list[float]] = {m: [] for m in MODES}
    calls: list[int] = []
    events: list[int] = []
    log_kb: list[float] = []
    not_identical = 0
    with FakeLLMServer(latency_ms=(latency_ms, latency_ms)) as server, \
            ledger.EventStore(data / "ledger.db") as store:
        off, on = _graphs(server.base_url)
        for i in range(warmup + runs):
            inputs = {"topic": toy.TOPICS[i % len(toy.TOPICS)]}
            took: dict[str, float] = {}

            def run_off() -> None:
                t = time.perf_counter()
                off.invoke(inputs)
                took["off"] = (time.perf_counter() - t) * 1000

            if i % 2 == 0:
                run_off()
            t = time.perf_counter()
            with ledger.record(store) as rec:
                on.invoke(inputs)
            took["record"] = (time.perf_counter() - t) * 1000
            t = time.perf_counter()
            with ledger.replay(rec.run_id, store) as rep:
                on.invoke(inputs)
            took["replay"] = (time.perf_counter() - t) * 1000
            if i % 2 == 1:
                run_off()

            if i < warmup:
                continue
            for m in MODES:
                ms[m].append(took[m])
            calls.append(rec.stats.live_calls)
            events.append(sum(1 for _ in store.events(rec.run_id, hydrate=False)))
            size = store.run_storage(rec.run_id)
            log_kb.append((size.events + size.blobs) / 1024)
            identical = (rep.output_sha256 == rec.output_sha256
                         and rep.stats.live_calls == 0 and not rep.divergences)
            not_identical += not identical
            if (i - warmup + 1) % 50 == 0:
                print(f"  {i - warmup + 1}/{runs}", file=sys.stderr, flush=True)
    return {"ms": ms, "calls": calls, "events": events, "log_kb": log_kb,
            "not_identical": not_identical}


def summarize(raw: dict[str, Any]) -> dict[str, Any]:
    run = {m: _pcts(raw["ms"][m]) for m in MODES}
    calls = sorted(raw["calls"])[len(raw["calls"]) // 2]
    added: dict[str, Any] = {}
    for m in ("record", "replay"):
        added[m] = {
            p: {
                "run_ms": round(run[m][p] - run["off"][p], 2),
                "per_step_ms": round((run[m][p] - run["off"][p]) / STEPS, 3),
                "per_call_ms": round((run[m][p] - run["off"][p]) / calls, 3),
            }
            for p in ("p50", "p95")
        }
    return {
        "run_ms": run,
        "per_step_ms": {m: {p: round(v / STEPS, 3) for p, v in run[m].items()} for m in MODES},
        "added": added,
        "calls_per_run": calls,
        "events_per_run": _pcts([float(e) for e in raw["events"]]),
        "log_kb": _pcts(raw["log_kb"]),
        "replays_not_identical": raw["not_identical"],
    }


def render(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    lines = [
        f"Overhead: {rep['runs']} runs per mode of the 5-node pipeline, "
        f"fake server latency {rep['latency_ms']} ms",
        f"{'mode':<8}{'run ms p50/p95':>18}{'per step p50/p95':>20}"
        f"{'added/step p50/p95':>22}{'added/call p50/p95':>22}",
    ]
    for m in MODES:
        r, st = s["run_ms"][m], s["per_step_ms"][m]
        a = s["added"].get(m)
        step = f"{a['p50']['per_step_ms']:.2f} / {a['p95']['per_step_ms']:.2f}" if a else "-"
        call = f"{a['p50']['per_call_ms']:.3f} / {a['p95']['per_call_ms']:.3f}" if a else "-"
        lines.append(f"{m:<8}{r['p50']:>9.2f} / {r['p95']:<6.2f}"
                     f"{st['p50']:>11.2f} / {st['p95']:<6.2f}{step:>22}{call:>22}")
    lines.append(f"log per run: {s['events_per_run']['p50']:.0f} events, "
                 f"{s['log_kb']['p50']:.1f} KB p50 ({s['log_kb']['max']:.1f} KB max); "
                 f"{s['calls_per_run']} LLM and tool calls; "
                 f"replays not identical: {s['replays_not_identical']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.overhead", description=__doc__.splitlines()[0])
    p.add_argument("--runs", type=int, default=1000, help="timed runs per mode")
    p.add_argument("--warmup", type=int, default=20, help="untimed iterations first")
    p.add_argument("--latency-ms", type=float, default=0,
                   help="fake LLM and search server delay per request")
    p.add_argument("--data", type=Path, help="where the event store goes (default: a temp dir)")
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="ledger-overhead-") as tmp:
        data = args.data or Path(tmp)
        data.mkdir(parents=True, exist_ok=True)
        raw = measure(args.runs, args.warmup, args.latency_ms, data)
    rep = {
        "benchmark": "overhead",
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "runs": args.runs,
        "warmup": args.warmup,
        "latency_ms": args.latency_ms,
        "steps_per_run": STEPS,
        "machine": {"platform": platform.platform(), "python": platform.python_version(),
                    "processor": platform.processor() or platform.machine()},
        "summary": summarize(raw),
    }
    out = args.out or RESULTS / "overhead.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1) + "\n")
    print(render(rep))
    print(f"\nWrote {out}")
    return 0 if rep["summary"]["replays_not_identical"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
