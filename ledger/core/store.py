"""Append-only event store on SQLite in WAL mode.

Each append is its own transaction with ``synchronous=FULL``, so an event
either lands whole or not at all, even if the process is killed mid-write.
Events are stored as serialized ``ledger.v1.Event`` protobufs; the columns
next to them are denormalized copies for lookup.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

from ledger.core.events import body_kind, pb

DEFAULT_DB_ENV = "LEDGER_DB"
DEFAULT_DB_PATH = "ledger.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    run_id    TEXT    NOT NULL,
    idx       INTEGER NOT NULL,
    step_key  TEXT,
    kind      TEXT    NOT NULL,
    data      BLOB    NOT NULL,
    PRIMARY KEY (run_id, idx)
) WITHOUT ROWID;
"""


def step_key(step: pb.StepRef) -> str:
    """Stable string form of a StepRef, used as the replay lookup key."""
    return f"{step.namespace}/{step.superstep}/{step.node}/{step.path}#{step.seq}"


class EventStore:
    """Thread-safe append-only log of run events.

    Parallel LangGraph branches run on worker threads and append concurrently;
    a single connection guarded by a lock keeps index assignment gap-free.
    """

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        if path is None:
            path = os.environ.get(DEFAULT_DB_ENV, DEFAULT_DB_PATH)
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._next_idx: dict[str, int] = {}

    def append(self, run_id: str, event: pb.Event) -> pb.Event:
        """Assign ``run_id`` and the next index, then durably append."""
        with self._lock:
            idx = self._next_idx.get(run_id)
            if idx is None:
                row = self._conn.execute(
                    "SELECT COALESCE(MAX(idx) + 1, 0) FROM events WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                idx = row[0]
            event.run_id = run_id
            event.index = idx
            key = step_key(event.step) if event.HasField("step") else None
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO events (run_id, idx, step_key, kind, data) VALUES (?, ?, ?, ?, ?)",
                    (run_id, idx, key, body_kind(event), event.SerializeToString()),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._next_idx[run_id] = idx + 1
            return event

    def events(self, run_id: str) -> Iterator[pb.Event]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM events WHERE run_id = ? ORDER BY idx", (run_id,)
            ).fetchall()
        for (data,) in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            yield ev

    def run_ids(self) -> list[str]:
        """Run ids in order of first appearance."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id, data FROM events WHERE idx = 0"
            ).fetchall()
        started: list[tuple[int, str]] = []
        for run_id, data in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            started.append((ev.recorded_at.ToNanoseconds(), run_id))
        return [r for _, r in sorted(started)]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> EventStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
