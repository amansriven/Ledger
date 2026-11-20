"""Resume: pick a crashed or failed recording up where it stopped.

    with ledger.resume(run_id) as session:
        graph.invoke(inputs)            # the input the run started with

A resume continues the same run, in the same log, so every effect keeps the
idempotency key it had. The graph runs from the start, as in a replay:

* calls already in the log are answered from it, strictly (a changed request
  raises ``ReplayMismatch``), and are not logged again;
* an effect the log left PENDING (the process died during it, or it ended in
  doubt) is settled before the run goes on: its ``reconcile`` function, if it
  has one, looks the outcome up at the API; otherwise, or if that finds
  nothing, it is called again with the same idempotency key, so an API that
  honours keys applies it at most once;
* calls past the end of the log run live and are appended after a
  ``RunResumed`` event, as in a recording.

A run that finished cleanly can't be resumed. One that ended with an error
can: calls that failed are answered with the same error again, while an
effect left in doubt gets settled, which is usually what the resume is for.

Resuming assumes nothing else is writing to the run: one worker per run.
Leases and fencing tokens for several workers come with M5.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from ledger.core.events import body_kind, pb
from ledger.core.session import Mode, Session, activate
from ledger.core.store import EventStore
from ledger.replay.engine import ReplayIndex, _check_exit

__all__ = ["RunAlreadyFinished", "resume", "resumable_runs", "run_status"]


class RunAlreadyFinished(RuntimeError):
    """The run finished cleanly; there is nothing to resume."""


def run_status(events: list[pb.Event]) -> tuple[str, pb.Error | None]:
    """("ok" | "error" | "unfinished", the error if it failed), from a run's log.

    Only the last RunFinished after the last RunResumed counts, so a run
    that failed and was resumed is unfinished until the resume ends.
    """
    status: tuple[str, pb.Error | None] = ("unfinished", None)
    for ev in events:
        kind = body_kind(ev)
        if kind == "run_resumed":
            status = ("unfinished", None)
        elif kind == "run_finished":
            if ev.run_finished.HasField("error"):
                status = ("error", ev.run_finished.error)
            else:
                status = ("ok", None)
    return status


def resumable_runs(store: EventStore) -> list[str]:
    """Recordings that stopped without finishing cleanly, oldest first."""
    out = []
    for run_id in store.run_ids():
        events = list(store.events(run_id, hydrate=False))
        first = events[0] if events else None
        if first is None or body_kind(first) != "run_started":
            continue
        if first.run_started.mode != pb.MODE_RECORD:
            continue
        if run_status(events)[0] != "ok":
            out.append(run_id)
    return out


@contextmanager
def resume(run_id: str, store: EventStore | None = None) -> Iterator[Session]:
    """Continue recording ``run_id`` from where its log ends.

    Invoke the graph inside the block with the input the run started with.
    On leaving the block, recorded calls the resume never made raise
    ``ReplayDivergence``, as in a strict replay.
    """
    owns_store = store is None
    store = store or EventStore()
    try:
        events = list(store.events(run_id))
        if not events or body_kind(events[0]) != "run_started":
            raise KeyError(f"no recorded run {run_id!r} in {store.path}")
        if events[0].run_started.mode != pb.MODE_RECORD:
            raise ValueError(f"{run_id} is a replay or fork; only recordings are resumed")
        status, error = run_status(events)
        if status == "ok":
            raise RunAlreadyFinished(f"run {run_id} already finished")
        index = ReplayIndex(events, resume=True)
        session = Session(store, run_id, Mode.RESUME, source_run_id=run_id, replay_index=index)
        session.graph_ref = events[0].run_started.graph
        session.resume_attempt = 1 + sum(body_kind(e) == "run_resumed" for e in events)
        session.previous_error = error
        with activate(session):
            yield session
        _check_exit(index, run_id, strict=True)
    finally:
        if owns_store:
            store.close()
