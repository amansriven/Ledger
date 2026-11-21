"""The chaos harness: kill the refund worker at random moments, cut its network,
restart it, and check Stripe afterwards.

    python -m chaos.harness --mode local  --runs 200            # kill -9 a subprocess
    python -m chaos.harness --mode docker --runs 10000          # docker kill, Toxiproxy

For each system (Ledger, then stock LangGraph, or ``--system`` for one):

1. Create one fresh charge per ticket at the fake Stripe server and write
   the tickets (seeded by ``--seed``; some amounts go over the automatic
   refund limit, so no refund is expected for them).
2. Start the worker (``chaos.worker``). After a random delay
   (``--kill-after``, seconds) kill it with SIGKILL: ``kill -9`` in local
   mode, ``docker kill`` in Docker mode. Start it again, and repeat until
   it exits on its own with every ticket settled.
3. Meanwhile inject network faults. Local mode asks the fake Stripe server
   to drop a request before or after applying it, or to answer 500.
   Docker mode puts Toxiproxy between the worker and Stripe and gives a
   share of connections a ``timeout`` toxic, upstream (the request never
   arrives) or downstream (it is applied and the answer is lost).
4. Check the invariants (``chaos.invariants``) against Stripe's state and,
   for Ledger, the event log.

Both systems run the same agent code against the same faults. Results go to
``chaos/results/<mode>.json`` with a table on stdout. Exit status is 0 only
if Ledger shows no duplicate, missing, unlogged or phantom refund and every
run settled.

A Mac that sleeps pauses everything; run long sessions under ``caffeinate``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from chaos import invariants

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "chaos" / ".work"
RESULTS = ROOT / "chaos" / "results"
SYSTEMS = ("ledger", "langgraph")
COMPOSE = ["docker", "compose", "-f", str(ROOT / "chaos" / "docker-compose.yml"), "-p", "ledger-chaos"]


# ---------------------------------------------------------------- HTTP


def _http(method: str, url: str, body: Any = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


def make_tickets(stripe_url: str, n: int, seed: int, system: str) -> list[dict[str, Any]]:
    rng = random.Random(f"{seed}:{system}")
    tickets = []
    for i in range(n):
        # About one in ten over the automatic limit: no refund expected.
        # A partial refund (say, the shipping fee), so Stripe would accept a
        # second one: a duplicate is possible, as it is in real use.
        amount = rng.choice([rng.randint(500, 40_000)] * 9 + [rng.randint(60_000, 90_000)])
        charge = _http("POST", f"{stripe_url}/_charges", {"amount": amount * 3})
        tickets.append({
            "ticket_id": f"{system[0].upper()}{i:05d}",
            "charge_id": charge["id"],
            "amount": amount,
            "message": "Please refund the shipping fee on this order.",
            "reason": "requested_by_customer",
        })
    return tickets


# ---------------------------------------------------------------- workers


class LocalWorker:
    """The worker as a subprocess on this machine, killed with SIGKILL."""

    def __init__(self, system: str, data: Path, env: dict[str, str]) -> None:
        self.cmd = [sys.executable, "-m", "chaos.worker", "--system", system, "--data", str(data)]
        self.env = {**os.environ, **env}
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        self.proc = subprocess.Popen(self.cmd, cwd=ROOT, env=self.env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def wait(self, timeout: float) -> int | None:
        assert self.proc is not None
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            return None

    def kill(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def close(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.kill()


class DockerWorker:
    """The worker in its own container, killed with ``docker kill``."""

    def __init__(self, system: str, data_in_container: str) -> None:
        self.name = f"ledger-chaos-worker-{system}"
        self.cmd = ["python", "-m", "chaos.worker", "--system", system, "--data", data_in_container]
        self.created = False

    def start(self) -> None:
        if not self.created:
            subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
            subprocess.run([*COMPOSE, "run", "-d", "--name", self.name, "worker", *self.cmd],
                           check=True, capture_output=True)
            self.created = True
        else:
            subprocess.run(["docker", "start", self.name], check=True, capture_output=True)

    def wait(self, timeout: float) -> int | None:
        deadline = time.monotonic() + timeout
        while True:
            out = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", self.name],
                check=True, capture_output=True, text=True,
            ).stdout.split()
            if out[0] == "false":
                return int(out[1])
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.1)

    def kill(self) -> None:
        subprocess.run(["docker", "kill", self.name], capture_output=True)
        self.wait(30)

    def close(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


# ---------------------------------------------------------------- faults


class LocalFaults(threading.Thread):
    """Every ``period`` seconds, with probability ``p``, the next request to Stripe is lost.

    A new fault is added only when the last one has been used, so faults
    don't pile up while the worker is down.
    """

    MODES = ("drop_before", "drop_after", "error_500")
    PATHS = ("/v1/refunds", "/v1/charges/*")

    def __init__(self, stripe_url: str, p: float, seed: int, period: float = 0.1) -> None:
        super().__init__(daemon=True)
        self.url, self.p, self.period = stripe_url, p, period
        self.rng = random.Random(seed)
        self.stop = threading.Event()
        self.injected = 0

    def run(self) -> None:
        while not self.stop.wait(self.period):
            if self.rng.random() < self.p:
                with contextlib.suppress(OSError):
                    if _http("GET", f"{self.url}/_state")["faults_pending"]:
                        continue
                    _http("POST", f"{self.url}/_faults", {
                        "path": self.rng.choice(self.PATHS),
                        "mode": self.rng.choice(self.MODES),
                        "count": 1,
                    })
                    self.injected += 1


class Toxiproxy:
    """Toxiproxy between the worker and Stripe, with timeout toxics on a share of connections."""

    def __init__(self, api: str, toxicity: float) -> None:
        self.api, self.toxicity = api, toxicity
        # Toxics hit connections at random; Toxiproxy doesn't count them.
        self.injected: int | None = None

    def setup(self) -> None:
        with contextlib.suppress(Exception):
            _http("DELETE", f"{self.api}/proxies/stripe")
        _http("POST", f"{self.api}/proxies", {
            "name": "stripe", "listen": "0.0.0.0:22220", "upstream": "fakestripe:12111",
            "enabled": True,
        })
        if self.toxicity <= 0:
            return
        for stream in ("upstream", "downstream"):
            _http("POST", f"{self.api}/proxies/stripe/toxics", {
                "name": f"timeout_{stream}", "type": "timeout", "stream": stream,
                "toxicity": self.toxicity, "attributes": {"timeout": 200},
            })

    def start(self) -> None:
        pass

    def finish(self) -> None:
        with contextlib.suppress(Exception):
            _http("DELETE", f"{self.api}/proxies/stripe")


# ---------------------------------------------------------------- a session


@dataclass
class Session:
    system: str
    runs: int
    kills: int = 0
    restarts: int = 0
    wall_seconds: float = 0.0
    resume_seconds: list[float] = field(default_factory=list)
    faults: int | None = 0
    report: dict[str, Any] = field(default_factory=dict)


def _pcts(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    v = sorted(values)

    def pct(p: float) -> float:
        return v[min(len(v) - 1, round(p * (len(v) - 1)))]

    return {"p50": round(pct(0.5), 3), "p95": round(pct(0.95), 3), "max": round(v[-1], 3),
            "mean": round(statistics.fmean(v), 3)}


def resume_times(events_path: Path, starts: list[float]) -> list[float]:
    """Seconds from each restart to the first ticket it resumed."""
    if not events_path.exists():
        return []
    resumed = [json.loads(line)["t"] for line in events_path.read_text().splitlines()
               if '"resumed"' in line]
    out = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else float("inf")
        first = next((t for t in resumed if start <= t < end), None)
        if first is not None:
            out.append(first - start)
    return out


def run_session(system: str, worker: Any, data: Path, kill_after: tuple[float, float],
                rng: random.Random, max_restarts: int) -> Session:
    session = Session(system, 0)
    starts: list[float] = []
    began = time.monotonic()
    try:
        while True:
            starts.append(time.time())
            worker.start()
            code = worker.wait(rng.uniform(*kill_after))
            if code is None:
                worker.kill()
                session.kills += 1
            elif code == 0:
                break
            session.restarts += 1
            if session.restarts > max_restarts:
                raise RuntimeError(f"{system}: worker didn't finish after {max_restarts} restarts")
    finally:
        session.wall_seconds = round(time.monotonic() - began, 1)
        worker.close()
    session.resume_seconds = resume_times(data / f"{system}-events.jsonl", starts[1:])
    return session


def _fresh(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def run_system(system: str, args: argparse.Namespace, stripe_url: str, worker: Any,
               data: Path, faults: Any, get_state: Any) -> Session:
    tickets = make_tickets(stripe_url, args.runs, args.seed, system)
    (data / "tickets.json").write_text(json.dumps(tickets))
    rng = random.Random(f"{args.seed}:kills:{system}")
    faults.start()
    try:
        session = run_session(system, worker, data, tuple(args.kill_after), rng, args.max_restarts)
    finally:
        if hasattr(faults, "stop"):
            faults.stop.set()
        else:
            faults.finish()
    session.runs = len(tickets)
    session.faults = faults.injected
    state = get_state()
    report = invariants.check_stripe(system, tickets, state)
    if system == "ledger":
        import ledger

        with ledger.EventStore(data / "ledger.db") as store:
            invariants.check_ledger_log(report, tickets, state, store)
    session.report = report.as_dict()
    return session


# ---------------------------------------------------------------- modes


@contextlib.contextmanager
def local_servers() -> Iterator[tuple[str, str]]:
    from ledger.testing.fake_llm import FakeLLMServer
    from ledger.testing.fake_stripe import FakeStripeServer

    with FakeStripeServer() as stripe_server, FakeLLMServer(latency_ms=(0, 5)) as llm_server:
        yield stripe_server.base_url, llm_server.base_url


def run_local(args: argparse.Namespace) -> list[Session]:
    sessions = []
    with local_servers() as (stripe_url, llm_url):
        env = {
            "STRIPE_API_BASE": stripe_url,
            "OPENAI_BASE_URL": f"{llm_url}/v1",
            "OPENAI_API_KEY": "fake",
            "STRIPE_SECRET_KEY": "",
        }
        for i, system in enumerate(args.systems):
            data = _fresh(WORK / "local" / system)
            worker = LocalWorker(system, data, env)
            faults = LocalFaults(stripe_url, args.fault_rate, args.seed + i)
            sessions.append(run_system(
                system, args, stripe_url, worker, data, faults,
                lambda: _http("GET", f"{stripe_url}/_state"),
            ))
    return sessions


def run_docker(args: argparse.Namespace) -> list[Session]:
    stripe_url, toxi_api = "http://127.0.0.1:12111", "http://127.0.0.1:8474"
    subprocess.run([*COMPOSE, "up", "-d", "--build", "--wait", "fakestripe", "fakellm", "toxiproxy"],
                   check=True)
    sessions = []
    try:
        for system in args.systems:
            data = _fresh(WORK / "docker" / system)
            worker = DockerWorker(system, f"/work/docker/{system}")
            proxy = Toxiproxy(toxi_api, args.toxicity)
            proxy.setup()
            sessions.append(run_system(
                system, args, stripe_url, worker, data, proxy,
                lambda: _http("GET", f"{stripe_url}/_state"),
            ))
    finally:
        if not args.keep:
            subprocess.run([*COMPOSE, "down", "-v"], capture_output=True)
    return sessions


# ---------------------------------------------------------------- report


def render(sessions: list[Session], mode: str) -> str:
    head = (f"{'system':<11}{'runs':>7}{'kills':>7}{'faults':>8}{'refunds':>9}{'dup':>6}"
            f"{'missing':>9}{'unlogged':>10}{'unfinished':>12}{'resume s p50/p95':>20}{'wall s':>9}")
    lines = [f"Chaos session ({mode})", head]
    if mode == "docker":
        lines[0] += " - faults: Toxiproxy timeout toxics on a share of Stripe connections"
    for s in sessions:
        r, t = s.report, _pcts(s.resume_seconds)
        unlogged = r["unlogged"] + r["phantom"] if s.system == "ledger" else "-"
        unfinished = r["unfinished"] if s.system == "ledger" else "-"
        faults = "-" if s.faults is None else s.faults
        lines.append(
            f"{s.system:<11}{s.runs:>7}{s.kills:>7}{faults!s:>8}{r['refunds']:>9}{r['duplicate']:>6}"
            f"{r['missing']:>9}{unlogged!s:>10}{unfinished!s:>12}"
            f"{t.get('p50', 0):>11.2f} / {t.get('p95', 0):<6.2f}{s.wall_seconds:>9.0f}"
        )
    for s in sessions:
        for ticket, what in list(s.report["examples"].items())[:5]:
            lines.append(f"  {s.system} {ticket}: {what}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="chaos.harness", description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=("local", "docker"), default="local")
    p.add_argument("--system", choices=(*SYSTEMS, "both"), default="both")
    p.add_argument("--runs", type=int, default=200, help="tickets per system")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--kill-after", type=float, nargs=2, default=(1.5, 4.0), metavar=("MIN", "MAX"),
                   help="kill the worker this many seconds after each start (uniform)")
    p.add_argument("--fault-rate", type=float, default=0.5,
                   help="local mode: chance per 100 ms that the next Stripe request is lost")
    p.add_argument("--toxicity", type=float, default=0.03,
                   help="docker mode: share of Stripe connections cut by a timeout toxic, per direction")
    p.add_argument("--max-restarts", type=int, default=100_000)
    p.add_argument("--keep", action="store_true", help="docker mode: leave the services running")
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)
    args.systems = SYSTEMS if args.system == "both" else (args.system,)

    sessions = run_local(args) if args.mode == "local" else run_docker(args)
    result = {
        "benchmark": "exactly_once_chaos",
        "mode": args.mode,
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "args": {k: v for k, v in vars(args).items() if k not in ("out", "systems")},
        "sessions": [
            {**{k: v for k, v in s.__dict__.items() if k != "resume_seconds"},
             "resume_seconds": _pcts(s.resume_seconds), "resumes": len(s.resume_seconds)}
            for s in sessions
        ],
    }
    out = args.out or RESULTS / f"{args.mode}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=str) + "\n")
    print(render(sessions, args.mode))
    print(f"\nWrote {out}")
    ours = next((s for s in sessions if s.system == "ledger"), None)
    return 0 if ours is None or ours.report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
