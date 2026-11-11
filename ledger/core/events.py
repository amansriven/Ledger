"""Event construction helpers and canonical JSON payloads.

Every value Ledger logs (requests, responses, tool args, results) is turned
into canonical JSON first: sorted keys, no insignificant whitespace, UTF-8.
The SHA-256 of those bytes is the value's identity, which is what replay
compares and what the M1 blob store will be keyed by.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from ledger._proto import events_pb2 as pb

__all__ = [
    "pb",
    "canonical_json",
    "make_payload",
    "load_payload",
    "make_error",
    "new_event",
    "body_kind",
]


def _default(obj: Any) -> Any:
    # Pydantic models (OpenAI responses, LangChain messages) and dataclasses.
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if hasattr(obj, "__dataclass_fields__"):
        import dataclasses

        return dataclasses.asdict(obj)
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=canonical_json)
    if isinstance(obj, bytes):
        return {"__bytes_hex__": obj.hex()}
    raise TypeError(f"Ledger cannot log a value of type {type(obj).__name__}")


def canonical_json(value: Any) -> bytes:
    """Serialize ``value`` to canonical JSON bytes."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_default,
    ).encode("utf-8")


def make_payload(value: Any) -> pb.Payload:
    data = canonical_json(value)
    return pb.Payload(sha256=hashlib.sha256(data).hexdigest(), json=data)


def load_payload(payload: pb.Payload) -> Any:
    return json.loads(payload.json) if payload.json else None


def make_error(exc: BaseException) -> pb.Error:
    cls = type(exc)
    return pb.Error(type=f"{cls.__module__}.{cls.__qualname__}", message=str(exc))


def new_event(step: pb.StepRef | None = None) -> pb.Event:
    """An Event stamped with wall-clock time. run_id and index are set by the store."""
    ts = Timestamp()
    ts.GetCurrentTime()
    ev = pb.Event(recorded_at=ts)
    if step is not None:
        ev.step.CopyFrom(step)
    return ev


def body_kind(event: pb.Event) -> str:
    """'llm_call', 'tool_call', 'run_started' or 'run_finished'."""
    return event.WhichOneof("body") or ""
