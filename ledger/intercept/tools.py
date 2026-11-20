"""Interceptor for tool functions.

``@ledger.tool`` records a function's arguments and result when a session is
recording, and returns the recorded result without calling the function when
it is replaying. Works on sync and async functions.

Arguments and results must be JSON-serializable (Pydantic models and
dataclasses are converted). On replay the result comes back as decoded JSON,
so a tool that returns a tuple gets a list back; return JSON-shaped values.

Replay matches a call on its normalized args (``ledger.core.normalize``), so
a timestamp or id inside an argument does not count as a change. Pass
``normalizer=`` to change that.

Tools with side effects that must happen once (payments, refunds) belong in
``@ledger.effect`` instead, which adds idempotency keys and a write-ahead
record. ``@ledger.tool`` only records and replays.
"""

from __future__ import annotations

import functools
import inspect
import time
from collections.abc import Callable
from typing import Any, TypeVar, overload

from ledger.core.events import make_payload, pb
from ledger.core.normalize import DEFAULT, Normalizer
from ledger.core.session import current_session
from ledger.intercept._common import GO_LIVE, answer_from_log, log_live

__all__ = ["tool"]

F = TypeVar("F", bound=Callable[..., Any])


def _identity(value: Any) -> Any:
    return value


def _wrap(fn: F, name: str, normalizer: Normalizer) -> F:
    def prepare(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[pb.ToolCall, pb.Payload]:
        payload = make_payload({"args": list(args), "kwargs": kwargs})
        body = pb.ToolCall(name=name, args=payload, args_key=normalizer.key_of(payload))
        return body, payload

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            session = current_session()
            if session is None:
                return await fn(*args, **kwargs)
            body, request = prepare(args, kwargs)
            step = session.next_step()
            if session.is_replay:
                out = answer_from_log(
                    session, step, "tool_call", request, _identity, match_key=body.args_key
                )
                if out is not GO_LIVE:
                    return out
            started = time.perf_counter_ns()
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                log_live(session, step, "tool_call", body, error=exc, started_ns=started)
                raise
            log_live(session, step, "tool_call", body, response=result, started_ns=started)
            return result

        async_wrapper.__ledger_tool__ = name  # type: ignore[attr-defined]
        return async_wrapper  # type: ignore[return-value]

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        session = current_session()
        if session is None:
            return fn(*args, **kwargs)
        body, request = prepare(args, kwargs)
        step = session.next_step()
        if session.is_replay:
            out = answer_from_log(
                session, step, "tool_call", request, _identity, match_key=body.args_key
            )
            if out is not GO_LIVE:
                return out
        started = time.perf_counter_ns()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            log_live(session, step, "tool_call", body, error=exc, started_ns=started)
            raise
        log_live(session, step, "tool_call", body, response=result, started_ns=started)
        return result

    wrapper.__ledger_tool__ = name  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


@overload
def tool(fn: F) -> F: ...
@overload
def tool(
    *, name: str | None = None, normalizer: Normalizer | None = None
) -> Callable[[F], F]: ...


def tool(
    fn: F | None = None, *, name: str | None = None, normalizer: Normalizer | None = None
) -> Any:
    """Record and replay a tool function. Use as ``@tool`` or ``@tool(name=...)``."""

    def decorate(f: F) -> F:
        return _wrap(f, name or f"{f.__module__}.{f.__qualname__}", normalizer or DEFAULT)

    return decorate(fn) if fn is not None else decorate
