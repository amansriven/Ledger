"""Build a demo event store with one of each kind of run, for the trace UI.

    uv run python scripts/make_demo_db.py [--out demo.db]

Everything runs against the fake LLM, tool and Stripe servers in
``ledger.testing``, so it needs no network and no keys. The store holds:

- 3 recordings of the toy pipeline, and a strict replay of the first
- a toy recording with a ~6 KB topic, so some payloads live in the blob store
- a fork of the first with one research summary edited
- a toy run whose research_web reply was corrupted, and a saved diagnosis
  naming that call (plus the 7 forks the diagnosis ran)
- a refund agent run killed after Stripe made the refund but before the
  outcome was logged, then resumed (the refund is settled by reconcile)

The servers, the UI tests and the hosted read-only UI all use this store.
Run ids are random, so tests should look runs up by what they are, not by id.

The three research branches run in parallel, so where the corrupted reply
lands in the log, and so how many forks the diagnosis takes, varies: 7 when
it's #6 or #7, 4 when it's the last research call (#8). ``build`` rebuilds
until it gets 7, which shows more of the bisection (about half of builds do).
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

from openai import OpenAI

import ledger
from ledger.examples import refund_agent, toy_pipeline
from ledger.testing import faults
from ledger.testing.fake_llm import FakeLLMServer
from ledger.testing.fake_stripe import FakeStripeServer

TOY_REF = "ledger.examples.toy_pipeline:graph_from_env"
REFUND_REF = "ledger.examples.refund_agent:graph_from_env"
TOPICS = ["fencing tokens", "idempotency keys", "write-ahead logs"]
DIAGNOSIS_FORKS = 7


def clean(out: dict[str, Any]) -> bool:
    """The demo's success check: no corrupted text reached the output."""
    return not any("CORRUPT" in v for v in [out["plan"], out["draft"], *out["notes"]])


class Killed(BaseException):
    """The process dying: nothing after it reaches the log."""


def record_until_killed(store: ledger.EventStore, n: int, invoke) -> str:
    """Record ``invoke()``, letting only ``n`` events reach the log."""
    real, written = store.append, 0

    def append(run_id, event):
        nonlocal written
        if written >= n:
            raise Killed
        written += 1
        return real(run_id, event)

    store.append = append  # type: ignore[method-assign]
    try:
        with ledger.record(store) as rec:
            rec.graph_ref = REFUND_REF
            invoke()
    except Killed:
        pass
    finally:
        del store.append
    return rec.run_id


def build(path: Path, latency_ms: tuple[int, int] = (20, 120), attempts: int = 30) -> None:
    """(Re)create the demo store at ``path``, with a 7-fork diagnosis."""
    for _ in range(attempts):
        forks = build_once(path, latency_ms)
        if forks == DIAGNOSIS_FORKS:
            return
        print(f"rebuilding: the diagnosis took {forks} forks, not {DIAGNOSIS_FORKS}")
    raise RuntimeError(f"no {DIAGNOSIS_FORKS}-fork diagnosis in {attempts} builds")


def build_once(path: Path, latency_ms: tuple[int, int]) -> int:
    """Build the store once; returns how many forks the diagnosis took."""
    for p in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        p.unlink(missing_ok=True)
    shutil.rmtree(f"{path}.blobs", ignore_errors=True)

    with (
        FakeLLMServer(latency_ms=latency_ms) as llm_server,
        FakeStripeServer() as stripe_server,
        ledger.EventStore(path) as store,
    ):
        llm = ledger.wrap(OpenAI(base_url=f"{llm_server.base_url}/v1", api_key="fake", max_retries=0))
        toy = toy_pipeline.build_graph(llm, tools_url=f"{llm_server.base_url}/tools", model="fake-model")

        recorded = []
        for topic in TOPICS:
            with ledger.record(store) as rec:
                rec.graph_ref = TOY_REF
                toy.invoke({"topic": topic})
            recorded.append(rec.run_id)
            print(f"recorded  {rec.run_id}  toy: {topic}")

        # A long topic: its input and LLM requests go to the blob store.
        long_topic = "how fencing tokens stop a paused leader from writing; " * 110
        with ledger.record(store) as rec:
            rec.graph_ref = TOY_REF
            toy.invoke({"topic": long_topic})
        print(f"recorded  {rec.run_id}  toy: a {len(long_topic)}-character topic (payloads in blobs)")

        first = recorded[0]
        with ledger.replay(first, store) as rep:
            rep.graph_ref = TOY_REF
            toy.invoke({"topic": TOPICS[0]})
        print(f"replayed  {rep.run_id}")

        target = next(s for s in ledger.steps(first, store)
                      if s.kind == "llm_call" and s.node == "research_web")
        with ledger.fork(first, at=target, edit="A much shorter summary.", store=store) as fk:
            fk.graph_ref = TOY_REF
            toy.invoke({"topic": TOPICS[0]})
        print(f"forked    {fk.run_id}  (edited #{target.index})")

        fault = faults.Fault(node="research_web", kind="llm_call",
                             corrupt=faults.replace_text("CORRUPT"))
        with ledger.record(store) as rec, faults.inject(fault):
            rec.graph_ref = TOY_REF
            toy.invoke({"topic": "consensus"})
        failed = rec.run_id
        d = ledger.diagnose(failed, toy, clean, store=store)
        report = d.to_dict() | {
            "graph": TOY_REF,
            "check": "scripts/make_demo_db.py:clean",
            "settings": {"kinds": list(ledger.blame.CANDIDATE_KINDS), "repeats": 1,
                         "threshold": 0.5, "budget": None, "pricing": None},
        }
        store.save_diagnosis(failed, report)
        assert d.complete and d.step is not None and d.step.key == fault.step, d
        print(f"diagnosed {failed}  {d.status} #{d.step.index} ({d.forks} forks)")

        stripe = refund_agent.stripe_client("sk_test_fake", stripe_server.base_url)
        agent = refund_agent.build_graph(llm, stripe, model="fake-model")
        charge = stripe_server.create_charge(4200)
        ticket = {"ticket_id": "T-1001", "charge_id": charge["id"],
                  "message": "I was charged twice for the same order.", "reason": "duplicate"}
        # run_started, lookup, the claim, the refund's PENDING: killed on CONFIRMED.
        crashed = record_until_killed(store, 5, lambda: agent.invoke(ticket))
        with ledger.resume(crashed, store):
            agent.invoke(ticket)
        assert len(stripe_server.refunds_of(charge["id"])) == 1
        print(f"resumed   {crashed}  refund: killed before CONFIRMED, settled by reconcile")

        print(f"{len(store.run_ids())} runs in {path}")
        return d.forks


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", default="demo.db", help="store to (re)create (default demo.db)")
    p.add_argument("--latency-ms", type=int, nargs=2, default=(20, 120), metavar=("LO", "HI"),
                   help="fake LLM latency range, which spreads the timeline (default 20 120)")
    args = p.parse_args()
    build(Path(args.out), latency_ms=tuple(args.latency_ms))


if __name__ == "__main__":
    main()
