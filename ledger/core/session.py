"""Run sessions: the active recording or replay, and step identity.

A Session is bound to a context variable. LangGraph copies the context into
each task it runs (threads for sync nodes, tasks for async ones), so every
intercepted call made inside the graph sees the session that was active when
the graph was invoked.

Step identity comes from the LangGraph task that is running the call: its
namespace (with task ids stripped, since those change from run to run), its
superstep, node and task path, plus a per-task counter. Replay looks recorded
calls up by that key rather than by global log position, so parallel branches
that finish in a different order still get their own responses back.
"""

from __future__ import annotations

import enum
import random
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ledger.core.events import make_error, make_payload, new_event, pb
from ledger.core.store import EventStore

if TYPE_CHECKING:
    from ledger.replay.engine import ReplayIndex

__all__ = [
    "Mode",
    "Session",
    "SessionStats",
    "current_session",
    "record",
    "new_run_id",
    "task_coordinates",
]


class Mode(enum.Enum):
    RECORD = "record"
    REPLAY = "replay"
    FORK = "fork"

    @property
    def proto(self) -> int:
        return {
            Mode.RECORD: pb.MODE_RECORD,
            Mode.REPLAY: pb.MODE_REPLAY,
            Mode.FORK: pb.MODE_FORK,
        }[self]


@dataclass
class SessionStats:
    """Counts of intercepted calls. ``*_calls`` are LLM and tool calls only;
    reads of time, uuids and random seeds are counted as ``*_nondet``.
    ``edited`` counts calls of any kind whose outcome a fork replaced."""

    live_calls: int = 0
    replayed_calls: int = 0
    live_nondet: int = 0
    replayed_nondet: int = 0
    edited: int = 0


_current: ContextVar[Session | None] = ContextVar("ledger_session", default=None)

ROOT_NODE = "__root__"


def current_session() -> Session | None:
    return _current.get()


def new_run_id() -> str:
    return uuid.uuid4().hex


def _render_path(part: Any) -> str:
    if isinstance(part, (tuple, list)):
        return "(" + ",".join(_render_path(p) for p in part) + ")"
    if isinstance(part, bool):
        return "T" if part else "F"
    if isinstance(part, (str, int)):
        return str(part)
    # Functional-API Call objects and the like have no stable rendering yet.
    return f"<{type(part).__name__}>"


def task_coordinates() -> tuple[str, int, str, str]:
    """(namespace, superstep, node, path) of the LangGraph task running now."""
    try:
        from langgraph.config import get_config

        metadata = get_config().get("metadata") or {}
    except (RuntimeError, ImportError):
        metadata = {}
    if "langgraph_node" not in metadata:
        return ("", -1, ROOT_NODE, "")
    ns = metadata.get("langgraph_checkpoint_ns", "") or ""
    # "outer:<task_id>|inner:<task_id>" -> ["outer", "inner"]; the last
    # segment is the current task itself, so drop it to get the parents.
    segments = [seg.partition(":")[0] for seg in ns.split("|") if seg]
    return (
        "|".join(segments[:-1]),
        int(metadata.get("langgraph_step", -1)),
        str(metadata["langgraph_node"]),
        _render_path(metadata.get("langgraph_path", ())),
    )


class Session:
    """One recorded or replayed run."""

    def __init__(
        self,
        store: EventStore,
        run_id: str,
        mode: Mode,
        *,
        source_run_id: str = "",
        replay_index: ReplayIndex | None = None,
    ) -> None:
        if mode is not Mode.RECORD and replay_index is None:
            raise ValueError(f"a {mode.value} session needs a replay index")
        self.store = store
        self.run_id = run_id
        self.mode = mode
        self.source_run_id = source_run_id
        self.replay_index = replay_index
        self.stats = SessionStats()
        self.output: Any = None
        self.output_sha256: str = ""
        # "module:attr" of the graph, set by the CLI and logged in RunStarted.
        self.graph_ref: str = ""
        # Whether an effect (ledger.effect) may run live. Always for a
        # recording; a replay or fork has to ask for it.
        self.live_effects = mode is Mode.RECORD
        self._counters: dict[tuple[str, int, str, str], int] = {}
        self._rngs: dict[tuple[str, int, str, str], random.Random] = {}
        self._lock = threading.Lock()

    # Step identity

    def next_step(self) -> pb.StepRef:
        """StepRef for the next intercepted call made by the current task."""
        coords = task_coordinates()
        with self._lock:
            seq = self._counters.get(coords, 0)
            self._counters[coords] = seq + 1
        namespace, superstep, node, path = coords
        return pb.StepRef(
            namespace=namespace, superstep=superstep, node=node, path=path, seq=seq
        )

    def count_call(self, *, live: bool, kind: str = "llm_call", edited: bool = False) -> None:
        with self._lock:
            if edited:
                self.stats.edited += 1
            elif kind == "nondet":
                if live:
                    self.stats.live_nondet += 1
                else:
                    self.stats.replayed_nondet += 1
            elif live:
                self.stats.live_calls += 1
            else:
                self.stats.replayed_calls += 1

    # Logging

    def append(self, event: pb.Event) -> pb.Event:
        return self.store.append(self.run_id, event)

    def run_started(self, graph_input: Any) -> None:
        ev = new_event()
        ev.run_started.CopyFrom(
            pb.RunStarted(
                mode=self.mode.proto,
                input=make_payload(graph_input),
                source_run_id=self.source_run_id,
                graph=self.graph_ref,
            )
        )
        self.append(ev)

    def run_finished(self, output: Any = None, error: BaseException | None = None) -> None:
        ev = new_event()
        if error is not None:
            ev.run_finished.error.CopyFrom(make_error(error))
        else:
            payload = make_payload(output)
            ev.run_finished.output.CopyFrom(payload)
            self.output = output
            self.output_sha256 = payload.sha256
        self.append(ev)

    @property
    def is_replay(self) -> bool:
        """Whether calls are looked up in a recording (replays and forks)."""
        return self.mode is not Mode.RECORD

    @property
    def divergences(self) -> list[Any]:
        """Where a replay stopped matching its recording (``DivergencePoint``s).

        Empty for recordings and for replays that matched throughout. Skipped
        recorded calls are added when the ``replay`` block exits.
        """
        return list(self.replay_index.divergences) if self.replay_index else []


@contextmanager
def activate(session: Session) -> Iterator[Session]:
    token = _current.set(session)
    try:
        yield session
    finally:
        _current.reset(token)


@contextmanager
def record(store: EventStore | None = None, run_id: str | None = None) -> Iterator[Session]:
    """Record every intercepted call made inside the block as one run."""
    owns_store = store is None
    store = store or EventStore()
    try:
        with activate(Session(store, run_id or new_run_id(), Mode.RECORD)) as session:
            yield session
    finally:
        if owns_store:
            store.close()
