"""Append-only event store on SQLite in WAL mode.

Each append is its own transaction with ``synchronous=FULL``, so an event
either lands whole or not at all, even if the process is killed mid-write.
Events are stored as serialized ``ledger.v1.Event`` protobufs; the columns
next to them are denormalized copies for lookup.

Payloads larger than ``blob_threshold`` bytes go to a content-addressed blob
store next to the database (``ledger.db`` -> ``ledger.db.blobs/``) and are
stored in the event with ``json`` empty. Blobs are written and fsynced before
the event that refers to them is committed, so a committed event never points
at a missing blob. ``events()`` fills the payloads back in.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from ledger.core.blobs import BlobStore
from ledger.core.events import body_kind, pb

DEFAULT_DB_ENV = "LEDGER_DB"
DEFAULT_DB_PATH = "ledger.db"
DEFAULT_BLOB_THRESHOLD = 4096

# Payload fields of each event body, in schema order.
_PAYLOAD_FIELDS = {
    "run_started": ("input",),
    "run_finished": ("output",),
    "llm_call": ("request", "response"),
    "tool_call": ("args", "result"),
    "nondet": ("value",),
    "effect": ("args", "result"),
}

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


def payloads(event: pb.Event) -> Iterator[pb.Payload]:
    """Every Payload set on ``event``."""
    kind = body_kind(event)
    body = getattr(event, kind) if kind else None
    for name in _PAYLOAD_FIELDS.get(kind, ()):
        if body.HasField(name):
            yield getattr(body, name)


@dataclass(frozen=True)
class RunStorage:
    """Bytes one run takes up. Blobs shared with other runs count in full."""

    events: int
    blobs: int
    blob_count: int

    @property
    def total(self) -> int:
        return self.events + self.blobs


class EventStore:
    """Thread-safe append-only log of run events.

    Parallel LangGraph branches run on worker threads and append concurrently;
    a single connection guarded by a lock keeps index assignment gap-free.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        blob_threshold: int | None = DEFAULT_BLOB_THRESHOLD,
    ) -> None:
        """Open (or create) the store at ``path``.

        ``blob_threshold`` is the payload size in bytes above which a payload
        is moved to the blob store; ``None`` keeps every payload inline. An
        in-memory store (``":memory:"``) always keeps payloads inline.
        """
        if path is None:
            path = os.environ.get(DEFAULT_DB_ENV, DEFAULT_DB_PATH)
        self.path = Path(path)
        in_memory = str(self.path) == ":memory:"
        if not in_memory:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.blob_threshold = None if in_memory else blob_threshold
        self.blobs = BlobStore(self.path.with_name(self.path.name + ".blobs"))
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._next_idx: dict[str, int] = {}

    def _large(self, event: pb.Event) -> list[pb.Payload]:
        if self.blob_threshold is None:
            return []
        return [p for p in payloads(event) if len(p.json) > self.blob_threshold]

    def _stored_form(self, event: pb.Event, large: list[pb.Payload]) -> pb.Event:
        """Copy of ``event`` with the large payloads' bytes left out."""
        if not large:
            return event
        stored = pb.Event()
        stored.CopyFrom(event)
        for p in payloads(stored):
            if len(p.json) > self.blob_threshold:
                p.json = b""
        return stored

    def _hydrate(self, event: pb.Event) -> None:
        for p in payloads(event):
            if not p.json and p.sha256:
                p.json = self.blobs.get(p.sha256)

    def append(self, run_id: str, event: pb.Event) -> pb.Event:
        """Assign ``run_id`` and the next index, then durably append.

        ``event`` keeps its payloads inline; only the stored copy refers to
        blobs.
        """
        # Blobs first, outside the lock: they are content-addressed, so
        # parallel writers can't clash, and fsyncing them shouldn't serialize
        # the branches. Each is durable before the event that names it.
        large = self._large(event)
        for p in large:
            p.sha256 = self.blobs.put(p.json, p.sha256 or None)
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
            stored = self._stored_form(event, large)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO events (run_id, idx, step_key, kind, data) VALUES (?, ?, ?, ?, ?)",
                    (run_id, idx, key, body_kind(event), stored.SerializeToString()),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._next_idx[run_id] = idx + 1
            return event

    def events(self, run_id: str, *, hydrate: bool = True) -> Iterator[pb.Event]:
        """The run's events in log order.

        With ``hydrate`` (the default) payloads held in the blob store are
        read back and checked against their digest; without it they come back
        with ``json`` empty, as stored.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM events WHERE run_id = ? ORDER BY idx", (run_id,)
            ).fetchall()
        for (data,) in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            if hydrate:
                self._hydrate(ev)
            yield ev

    def run_storage(self, run_id: str) -> RunStorage:
        """Bytes of the run's stored events plus the distinct blobs they refer to."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        digests = {
            p.sha256
            for ev in self.events(run_id, hydrate=False)
            for p in payloads(ev)
            if not p.json and p.sha256
        }
        return RunStorage(
            events=row[0],
            blobs=sum(self.blobs.size(d) for d in digests),
            blob_count=len(digests),
        )

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
