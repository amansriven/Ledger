"""Replay-fidelity benchmark: the M1 gate.

Records N runs (200 by default) of the 5-node pipeline in
``examples/toy_pipeline.py`` against the nondeterministic fake LLM and search
server, then replays every one with the network off and checks it call by
call. The gate is 100% identical.

A replay counts as identical when all of these hold:

- it finishes, with the same output (SHA-256 of the canonical JSON);
- no call went live: no LLM, tool, clock, uuid or rng call;
- no divergence point was found;
- its log has the same calls as the recording, step for step: same kind, the
  same request byte for byte, the same outcome, every one served from the log.

The baseline is LangGraph's own checkpoint replay ("time travel"): the same
pipeline compiled with a ``SqliteSaver``, replayed from its first checkpoint
with ``graph.invoke(None, checkpoint_config)``. LangGraph re-executes every
node after that checkpoint, so its calls go to the network again. The
baseline is measured twice: with the network off, like Ledger, and with the
network on, where it can run but gets fresh, different answers.

Storage per run is the recording's bytes: Ledger's events plus the blobs they
refer to, and the baseline's checkpoint and write rows.

Usage:

    python -m bench.replay_fidelity run [--runs 200]
        Everything in one process: start the fake server, record, stop the
        server, block every socket, replay. Writes bench/results/.

    python -m bench.replay_fidelity record --data DIR [--fresh]
    python -m bench.replay_fidelity replay --data DIR --require-offline HOST:PORT
        The same in two phases, for Docker Compose: ``record`` runs with the
        network, ``replay`` in a container with ``network_mode: none``.

Exit status is 0 only if every Ledger replay was identical.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import random
import shutil
import socket
import sqlite3
import statistics
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import ledger
from examples import toy_pipeline as toy
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.store import step_key

GRAPH_REF = "examples.toy_pipeline:graph_from_env"
RESULTS = Path(__file__).parent / "results"
LEDGER_DB = "ledger.db"
BASELINE_DB = "langgraph.db"
RECORD_META = "record.json"


# ---------------------------------------------------------------- recording


def baseline_graph(conn: sqlite3.Connection) -> Any:
    """The same pipeline as plain LangGraph, with a SQLite checkpointer."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    saver = SqliteSaver(conn)
    saver.setup()
    return toy.builder_from_env().compile(checkpointer=saver)


def _thread(i: int) -> dict[str, Any]:
    return {"configurable": {"thread_id": f"run-{i:04d}"}}


def record(data: Path, runs: int, seed: int) -> dict[str, Any]:
    """Record ``runs`` runs with Ledger and with the baseline, same topics.

    Then replay the baseline once with the network still on. Returns what
    the replay phase needs to finish the report.
    """
    rng = random.Random(seed)
    topics = [rng.choice(toy.TOPICS) for _ in range(runs)]

    graph = toy.graph_from_env()
    started = time.perf_counter()
    with ledger.EventStore(data / LEDGER_DB) as store:
        for i, topic in enumerate(topics):
            with ledger.record(store) as session:
                session.graph_ref = GRAPH_REF
                graph.invoke({"topic": topic})
            if session.stats.live_calls != 8:
                raise SystemExit(f"run {i}: expected 8 live calls, got {session.stats.live_calls}")
            _progress("record ledger", i + 1, runs)
    ledger_s = time.perf_counter() - started

    conn = sqlite3.connect(data / BASELINE_DB, check_same_thread=False)
    try:
        app = baseline_graph(conn)
        started = time.perf_counter()
        for i, topic in enumerate(topics):
            app.invoke({"topic": topic}, _thread(i))
            _progress("record langgraph", i + 1, runs)
        baseline_s = time.perf_counter() - started
        storage = [baseline_storage(conn, _thread(i)) for i in range(runs)]
        online = [replay_baseline(app, i) for i in range(runs)]
    finally:
        conn.close()

    meta = {
        "runs": runs,
        "seed": seed,
        "topics": topics,
        "record_seconds": {"ledger": round(ledger_s, 2), "langgraph": round(baseline_s, 2)},
        "langgraph_storage_bytes": storage,
        "langgraph_online": [asdict(r) for r in online],
    }
    (data / RECORD_META).write_text(json.dumps(meta, indent=1))
    return meta


def baseline_storage(conn: sqlite3.Connection, config: dict[str, Any]) -> int:
    """Bytes of one thread's checkpoint and write rows."""
    tid = config["configurable"]["thread_id"]
    cp = conn.execute(
        "SELECT COALESCE(SUM(LENGTH(checkpoint) + LENGTH(metadata)), 0)"
        " FROM checkpoints WHERE thread_id = ?",
        (tid,),
    ).fetchone()[0]
    wr = conn.execute(
        "SELECT COALESCE(SUM(LENGTH(value)), 0) FROM writes WHERE thread_id = ?", (tid,)
    ).fetchone()[0]
    return int(cp + wr)


# ---------------------------------------------------------------- replaying


@dataclass
class RunResult:
    run_id: str
    identical: bool
    reason: str = ""
    seconds: float = 0.0
    storage_bytes: int = 0
    calls: int = 0
    events: int = 0


Trace = dict[str, tuple[str, str, str, bool]]


def call_trace(store: ledger.EventStore, run_id: str) -> Trace:
    """step key -> (kind, request digest, outcome digest, from_log), per call."""
    out: Trace = {}
    for ev in store.events(run_id):
        kind = body_kind(ev)
        if kind == "llm_call":
            body, request, result = ev.llm_call, ev.llm_call.request.sha256, "response"
        elif kind == "tool_call":
            body, request, result = ev.tool_call, ev.tool_call.args.sha256, "result"
        elif kind == "nondet":
            body, request, result = ev.nondet, ev.nondet.source, "value"
        else:
            continue
        if kind != "nondet" and body.HasField("error"):
            outcome = f"!{body.error.type}: {body.error.message}"
        else:
            outcome = getattr(body, result).sha256
        out[step_key(ev.step)] = (kind, request, outcome, ev.from_log)
    return out


def replay_ledger(graph: Any, store: ledger.EventStore, run_id: str) -> RunResult:
    storage = store.run_storage(run_id)
    recorded = call_trace(store, run_id)
    result = RunResult(
        run_id,
        identical=False,
        storage_bytes=storage.events + storage.blobs,
        calls=len(recorded),
        events=sum(1 for _ in store.events(run_id, hydrate=False)),
    )
    started = time.perf_counter()
    try:
        with ledger.replay(run_id, store) as session:
            assert session.replay_index is not None and session.replay_index.input is not None
            graph.invoke(load_payload(session.replay_index.input))
    except Exception as exc:  # a failed replay is a result, not a crash
        result.seconds = time.perf_counter() - started
        result.reason = f"{type(exc).__name__}: {exc}"
        return result
    result.seconds = time.perf_counter() - started

    problems = []
    if session.output_sha256 != session.replay_index.output_sha256:
        problems.append("output differs")
    live = session.stats.live_calls + session.stats.live_nondet
    if live:
        problems.append(f"{live} live calls")
    if session.divergences:
        problems.append(f"{len(session.divergences)} divergences")
    replayed = call_trace(store, session.run_id)
    if {k: v[:3] for k, v in replayed.items()} != {k: v[:3] for k, v in recorded.items()}:
        changed = sorted(set(replayed) ^ set(recorded)) or sorted(
            k for k in recorded if replayed[k][:3] != recorded[k][:3]
        )
        problems.append(f"calls differ at {changed[0]}")
    if not all(v[3] for v in replayed.values()):
        problems.append("a call was not served from the log")
    result.identical = not problems
    result.reason = "; ".join(problems)
    return result


def replay_baseline(app: Any, i: int) -> RunResult:
    """LangGraph time travel: re-run run ``i`` from its first checkpoint.

    The recorded output is the state at the oldest final checkpoint, so
    earlier baseline replays (which fork new branches in the same thread)
    don't change what counts as the recording.
    """
    config = _thread(i)
    history = list(app.get_state_history(config))  # newest first
    first = next(h for h in reversed(history) if h.metadata.get("step") == 0)
    recorded = next(h for h in reversed(history) if not h.next).values
    result = RunResult(config["configurable"]["thread_id"], identical=False)
    started = time.perf_counter()
    try:
        replayed = app.invoke(None, first.config)
    except Exception as exc:
        result.seconds = time.perf_counter() - started
        result.reason = f"{type(exc).__name__}: {exc}"
        return result
    result.seconds = time.perf_counter() - started
    result.identical = replayed == recorded
    if not result.identical:
        diff = sorted(k for k in recorded if replayed.get(k) != recorded[k])
        result.reason = f"output differs in {', '.join(diff)}"
    return result


def replay(data: Path) -> dict[str, Any]:
    """Replay every recorded run, Ledger and baseline. Expects no network."""
    meta = json.loads((data / RECORD_META).read_text())
    graph = toy.graph_from_env()
    with ledger.EventStore(data / LEDGER_DB) as store:
        run_ids = toy.recorded_runs(store)
        ours = []
        for n, run_id in enumerate(run_ids, 1):
            ours.append(replay_ledger(graph, store, run_id))
            _progress("replay ledger", n, len(run_ids))

    conn = sqlite3.connect(data / BASELINE_DB, check_same_thread=False)
    try:
        app = baseline_graph(conn)
        theirs = []
        for i in range(meta["runs"]):
            r = replay_baseline(app, i)
            r.storage_bytes = meta["langgraph_storage_bytes"][i]
            theirs.append(r)
            _progress("replay langgraph", i + 1, meta["runs"])
    finally:
        conn.close()
    online = [RunResult(**r) for r in meta["langgraph_online"]]
    for r, size in zip(online, meta["langgraph_storage_bytes"], strict=True):
        r.storage_bytes = size
    return report(meta, ours, theirs, online)


# ---------------------------------------------------------------- network


@contextlib.contextmanager
def network_off() -> Iterator[None]:
    """Make every outbound socket connection fail, in this process."""

    def deny(*args: Any, **kwargs: Any) -> Any:
        raise OSError("network is off for the replay benchmark")

    saved = socket.socket.connect, socket.socket.connect_ex, socket.create_connection
    socket.socket.connect = deny  # type: ignore[method-assign]
    socket.socket.connect_ex = deny  # type: ignore[method-assign]
    socket.create_connection = deny  # type: ignore[assignment]
    try:
        yield
    finally:
        socket.socket.connect, socket.socket.connect_ex, socket.create_connection = saved  # type: ignore[method-assign]


def check_offline(address: str) -> str:
    """Fail unless ``address`` (host:port of the recording's server) is unreachable."""
    host, _, port = address.rpartition(":")
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
    except OSError as exc:
        return f"{address} unreachable ({type(exc).__name__}: {exc})"
    raise SystemExit(f"{address} is reachable: the replay would not prove the network is off")


@contextlib.contextmanager
def fake_server_env() -> Iterator[Any]:
    """Run the fake LLM and search server in-process and point the pipeline at it."""
    from ledger.testing.fake_llm import FakeLLMServer

    server = FakeLLMServer().start()
    keys = ("OPENAI_BASE_URL", "TOOLS_URL", "OPENAI_API_KEY", "LEDGER_MODEL")
    saved = {k: os.environ.get(k) for k in keys}
    os.environ.update(
        OPENAI_BASE_URL=f"{server.base_url}/v1",
        TOOLS_URL=f"{server.base_url}/tools",
        OPENAI_API_KEY="fake",
        LEDGER_MODEL="fake-model",
    )
    try:
        yield server
    finally:
        server.stop()
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ---------------------------------------------------------------- report


@dataclass
class Summary:
    runs: int
    identical: int
    storage_kb: dict[str, float] = field(default_factory=dict)
    replay_ms: dict[str, float] = field(default_factory=dict)
    failures: dict[str, int] = field(default_factory=dict)


def _pcts(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)

    def pct(p: float) -> float:
        return ordered[min(len(ordered) - 1, round(p * (len(ordered) - 1)))]

    return {
        "mean": round(statistics.fmean(ordered), 2),
        "p50": round(pct(0.50), 2),
        "p95": round(pct(0.95), 2),
        "max": round(ordered[-1], 2),
    }


def summarize(results: list[RunResult]) -> Summary:
    failures: dict[str, int] = {}
    for r in results:
        if not r.identical:
            # Group by the reason's first clause, without run-specific detail.
            key = r.reason.split(";")[0].split(" at ")[0].split(" in ")[0][:120]
            failures[key] = failures.get(key, 0) + 1
    return Summary(
        runs=len(results),
        identical=sum(r.identical for r in results),
        storage_kb=_pcts([r.storage_bytes / 1024 for r in results]),
        replay_ms=_pcts([r.seconds * 1000 for r in results]),
        failures=failures,
    )


def report(
    meta: dict[str, Any],
    ours: list[RunResult],
    theirs: list[RunResult],
    online: list[RunResult],
) -> dict[str, Any]:
    return {
        "benchmark": "replay_fidelity",
        "workload": "examples/toy_pipeline.py: plan -> 3 parallel research branches -> write; "
        "8 calls (5 LLM, 3 tool) plus rng, uuid and clock reads per run",
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "platform": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        "seed": meta["seed"],
        "record_seconds": meta["record_seconds"],
        "gate": "100% of Ledger replays identical with the network off",
        "passed": bool(ours) and all(r.identical for r in ours),
        "network": meta.get("network", ""),
        "summary": {
            "ledger": asdict(summarize(ours)),
            "langgraph_network_off": asdict(summarize(theirs)),
            "langgraph_network_on": asdict(summarize(online)),
        },
        "runs": {
            "ledger": [asdict(r) for r in ours],
            "langgraph_network_off": [asdict(r) for r in theirs],
            "langgraph_network_on": [asdict(r) for r in online],
        },
    }


def render(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    rows = [
        ("Ledger replay, network off", s["ledger"]),
        ("LangGraph checkpoint replay, network off", s["langgraph_network_off"]),
        ("LangGraph checkpoint replay, network on", s["langgraph_network_on"]),
    ]
    lines = [
        f"Replay fidelity: {s['ledger']['runs']} recorded runs of the 5-node pipeline",
        f"Network during replay: {rep['network']}",
        "",
        f"{'':<42}{'identical':>12}{'storage KB p50/max':>22}{'replay ms p50/p95':>20}",
    ]
    for name, row in rows:
        kb, ms = row["storage_kb"], row["replay_ms"]
        lines.append(
            f"{name:<42}{row['identical']:>5}/{row['runs']:<6}"
            f"{kb.get('p50', 0):>11.1f} / {kb.get('max', 0):<8.1f}"
            f"{ms.get('p50', 0):>9.1f} / {ms.get('p95', 0):<8.1f}"
        )
    for name, row in rows:
        for why, count in row["failures"].items():
            lines.append(f"  {name}: {count} x {why}")
    lines += ["", f"Gate ({rep['gate']}): {'PASS' if rep['passed'] else 'FAIL'}"]
    return "\n".join(lines)


def write(rep: dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1) + "\n")
    print(render(rep))
    print(f"\nWrote {out}")


def _progress(what: str, n: int, total: int) -> None:
    if sys.stderr.isatty() and (n == total or n % 10 == 0):
        print(f"\r{what}: {n}/{total}", end="\n" if n == total else "", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- CLI


def _prepare(data: Path, fresh: bool) -> None:
    data.mkdir(parents=True, exist_ok=True)
    existing = [data / n for n in (LEDGER_DB, BASELINE_DB, RECORD_META) if (data / n).exists()]
    if existing and not fresh:
        raise SystemExit(f"{data} already holds a recording; pass --fresh to replace it")
    # The databases, their -wal and -shm files, and Ledger's blob directory.
    for path in data.iterdir():
        if path.name.startswith((LEDGER_DB, BASELINE_DB, RECORD_META)):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="replay_fidelity", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="record and replay in one process, sockets blocked for replay")
    rec = sub.add_parser("record", help="record runs (needs the fake server)")
    rep = sub.add_parser("replay", help="replay a recording (run with no network)")
    for sp in (run, rec):
        sp.add_argument("--runs", type=int, default=200)
        sp.add_argument("--seed", type=int, default=0, help="seed for the run topics")
    for sp in (rec, rep):
        sp.add_argument("--data", type=Path, required=True, help="directory for the recording")
    rec.add_argument("--fresh", action="store_true", help="replace an existing recording in --data")
    rep.add_argument(
        "--require-offline",
        metavar="HOST:PORT",
        help="refuse to run unless this address (the recording's server) is unreachable",
    )
    for sp in (run, rep):
        sp.add_argument("--out", type=Path, default=RESULTS / "replay_fidelity.json")
    args = p.parse_args(argv)

    if args.cmd == "record":
        _prepare(args.data, args.fresh)
        record(args.data, args.runs, args.seed)
        print(f"recorded {args.runs} runs with Ledger and with LangGraph checkpoints in {args.data}")
        return 0

    if args.cmd == "replay":
        network = check_offline(args.require_offline) if args.require_offline else "not checked"
        meta_path = args.data / RECORD_META
        meta = json.loads(meta_path.read_text())
        meta["network"] = network
        meta_path.write_text(json.dumps(meta, indent=1))
        rep_ = replay(args.data)
        write(rep_, args.out)
        return 0 if rep_["passed"] else 1

    with tempfile.TemporaryDirectory(prefix="ledger-bench-") as tmp:
        data = Path(tmp)
        with fake_server_env() as server:
            record(data, args.runs, args.seed)
            address = server.base_url.removeprefix("http://")
        with network_off():
            meta = json.loads((data / RECORD_META).read_text())
            meta["network"] = "sockets blocked in-process; " + check_offline(address)
            (data / RECORD_META).write_text(json.dumps(meta, indent=1))
            rep_ = replay(data)
    write(rep_, args.out)
    return 0 if rep_["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
