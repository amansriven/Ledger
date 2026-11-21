"""The chaos worker: works through a list of refund tickets, and picks up where it
was killed when it is started again.

    python -m chaos.worker --system ledger --data DIR
    python -m chaos.worker --system langgraph --data DIR

``DIR/tickets.json`` lists the tickets. Each system keeps its state in DIR:

ledger
    One Ledger run per ticket, with run id ``ticket-<id>``, in
    ``DIR/ledger.db``. On start, a ticket with no run is recorded; one whose
    run stopped part way, or failed with an effect still pending, is resumed
    (``ledger.resume``); a finished or final run is skipped.

langgraph
    The baseline: the same agent as plain LangGraph, with a ``SqliteSaver``
    checkpointer (``durability="sync"``) in ``DIR/langgraph.db``, one thread
    per ticket, and node retries on every node including the refund. On
    start, a ticket with no checkpoint is invoked; one whose thread has
    nodes left (cut off, or failed) is invoked again from its last
    checkpoint, which reruns the node that was cut off. A ticket that fails
    five times is given up (``DIR/langgraph-attempts.json``).

Each start and each ticket finished after a restart is appended to
``DIR/<system>-events.jsonl``, with the time since the process started, so
the harness can measure how long resuming takes. The worker makes up to
``--passes`` passes over the tickets (a run left in doubt by network faults
gets another try), and exits 0 when every ticket is settled, 3 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

STARTED = time.monotonic()
# The baseline gives a ticket this many failed invocations before giving up.
GIVE_UP_AFTER = 5


class Events:
    """Append-only JSON-lines record of what this worker process did."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.pid = os.getpid()

    def write(self, event: str, **fields: Any) -> None:
        line = {"t": time.time(), "since_start": time.monotonic() - STARTED,
                "pid": self.pid, "event": event, **fields}
        with self.path.open("a") as f:
            f.write(json.dumps(line) + "\n")
            f.flush()
            os.fsync(f.fileno())


def load_tickets(data: Path) -> list[dict[str, Any]]:
    return json.loads((data / "tickets.json").read_text())


def run_id_for(ticket: dict[str, Any]) -> str:
    return f"ticket-{ticket['ticket_id']}"


# ---------------------------------------------------------------- Ledger


def _settled(log: list[Any]) -> bool:
    """Whether a Ledger run needs nothing more: finished, or failed for good."""
    from ledger.replay.resume import pending_effects, run_status

    status, error = run_status(log)
    if status == "ok":
        return True
    if status == "unfinished":
        return False
    stopped = error is not None and error.type in ("builtins.KeyboardInterrupt", "builtins.SystemExit")
    return not stopped and not pending_effects(log)


def ledger_pass(data: Path, events: Events, tickets: list[dict[str, Any]]) -> int:
    """One pass over the tickets with Ledger. Returns how many are not settled."""
    import ledger
    from examples import refund_agent

    graph = refund_agent.graph_from_env()
    unsettled = 0
    with ledger.EventStore(data / "ledger.db") as store:
        for ticket in tickets:
            run_id = run_id_for(ticket)
            log = list(store.events(run_id, hydrate=False))
            if log and _settled(log):
                continue
            outcome = "ok"
            try:
                if not log:
                    with ledger.record(store, run_id=run_id) as session:
                        session.graph_ref = "examples.refund_agent:graph_from_env"
                        graph.invoke(ticket)
                else:
                    with ledger.resume(run_id, store):
                        graph.invoke(ticket)
            except Exception as exc:  # failed for good, or left in doubt
                outcome = f"error: {type(exc).__name__}"
            if log:
                events.write("resumed", ticket=ticket["ticket_id"], outcome=outcome)
            if not _settled(list(store.events(run_id, hydrate=False))):
                unsettled += 1
    return unsettled


# ---------------------------------------------------------------- baseline


def langgraph_pass(data: Path, events: Events, tickets: list[dict[str, Any]]) -> int:
    """One pass over the tickets with stock LangGraph. Returns how many are not settled."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    from examples import refund_agent

    attempts_path = data / "langgraph-attempts.json"
    attempts: dict[str, int] = (
        json.loads(attempts_path.read_text()) if attempts_path.exists() else {}
    )
    unsettled = 0
    conn = sqlite3.connect(data / "langgraph.db", check_same_thread=False)
    try:
        saver = SqliteSaver(conn)
        saver.setup()
        app = refund_agent.builder_from_env(retry_refund=True).compile(checkpointer=saver)
        for ticket in tickets:
            tid = ticket["ticket_id"]
            if attempts.get(tid, 0) >= GIVE_UP_AFTER:
                continue  # failed every time: given up
            config = {"configurable": {"thread_id": tid}}
            state = app.get_state(config)
            started = state.created_at is not None
            if started and not state.next:
                continue  # finished
            outcome = "ok"
            try:
                app.invoke(None if started else ticket, config, durability="sync")
            except Exception as exc:
                outcome = f"error: {type(exc).__name__}"
                attempts[tid] = attempts.get(tid, 0) + 1
                tmp = attempts_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(attempts))
                os.replace(tmp, attempts_path)
                if attempts[tid] < GIVE_UP_AFTER:
                    unsettled += 1
            if started:
                events.write("resumed", ticket=tid, outcome=outcome)
    finally:
        conn.close()
    return unsettled


PASSES = {"ledger": ledger_pass, "langgraph": langgraph_pass}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="chaos.worker", description=__doc__.splitlines()[0])
    p.add_argument("--system", choices=sorted(PASSES), required=True)
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--passes", type=int, default=5)
    args = p.parse_args(argv)

    events = Events(args.data / f"{args.system}-events.jsonl")
    events.write("start")
    tickets = load_tickets(args.data)
    unsettled = 0
    for _ in range(args.passes):
        unsettled = PASSES[args.system](args.data, events, tickets)
        if not unsettled:
            break
    events.write("done", unsettled=unsettled)
    return 0 if not unsettled else 3


if __name__ == "__main__":
    sys.exit(main())
