"""``@ledger.effect``: tool calls with side effects that must happen once.

An effect is a tool that changes the world (a refund, a charge, an email).
While recording, every call to it goes through a write-ahead protocol:

1. A PENDING event is appended (and fsynced) before the call starts. It
   carries the call's args and its idempotency key.
2. The function is called with ``idempotency_key=`` set to a key derived
   from the run id and the step (``ledger.effects.idempotency``). Errors in
   ``retry_on`` mean the outcome is unknown, so the call is retried with the
   same key, up to ``attempts`` times.
3. The outcome is appended at the same step: CONFIRMED with the result, or
   FAILED with an error the API gave (any exception not in ``retry_on``),
   which means the effect did not happen.

If the retries run out, the effect is in doubt: it may or may not have taken
effect. Ledger appends one more PENDING event with the last error and raises
``EffectInDoubt``. Resuming the run settles it against the API before going
on; the key makes a repeat safe.

A replay answers an effect from the log and never calls the function. A
replay or fork that would have to run an effect live (it diverged upstream,
or a fork reruns it) raises ``LiveEffectRefused`` unless it was started with
``live_effects=True``, so a counterfactual never refunds anyone by accident.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import random
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar, overload

from ledger.core.events import make_error, make_payload, new_event, pb
from ledger.core.normalize import DEFAULT, Normalizer
from ledger.core.session import Session, current_session
from ledger.core.store import step_key
from ledger.effects.idempotency import idempotency_key
from ledger.intercept._common import GO_LIVE, Settle, answer_from_log

__all__ = ["EffectInDoubt", "LiveEffectRefused", "effect"]

F = TypeVar("F", bound=Callable[..., Any])
KEY_ARG = "idempotency_key"
Phase = pb.EffectCall.Phase


class EffectInDoubt(RuntimeError):
    """An effect's outcome is unknown: every attempt failed in a way that
    leaves open whether it took effect. Its step stays PENDING in the log."""

    def __init__(self, name: str, step: str, key: str, attempts: int) -> None:
        super().__init__(
            f"effect {name} at step {step} is in doubt after {attempts} attempt(s) "
            f"(idempotency key {key}); resume the run to settle it"
        )
        self.name = name
        self.step = step
        self.idempotency_key = key
        self.attempts = attempts


class LiveEffectRefused(RuntimeError):
    """A replay or fork reached an effect it can't answer from the log."""


def _identity(value: Any) -> Any:
    return value


def _accepts_key(fn: Callable[..., Any]) -> bool:
    params = inspect.signature(fn).parameters.values()
    return any(
        p.kind is p.VAR_KEYWORD
        or (p.name == KEY_ARG and p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD))
        for p in params
    )


class _Call:
    """One effect call inside a session: its log entries and its retries."""

    def __init__(
        self,
        session: Session,
        name: str,
        normalizer: Normalizer,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if KEY_ARG in kwargs:
            raise TypeError(f"Ledger derives {KEY_ARG} for effect {name}; don't pass one")
        self.session = session
        self.step = session.next_step()
        self.request = make_payload({"args": list(args), "kwargs": kwargs})
        self.body = pb.EffectCall(
            name=name,
            args=self.request,
            args_key=normalizer.key_of(self.request),
            idempotency_key=idempotency_key(session.run_id, self.step),
        )
        self.started = 0

    def from_log(self) -> Any:
        """The recorded outcome, GO_LIVE, Settle (a resume), or LiveEffectRefused."""
        out = answer_from_log(
            self.session, self.step, "effect", self.request, _identity,
            match_key=self.body.args_key,
        )
        if out is GO_LIVE and not self.session.live_effects:
            raise LiveEffectRefused(
                f"{self.session.mode.value} of {self.session.source_run_id} would run "
                f"effect {self.body.name} live at step {step_key(self.step)}; "
                "pass live_effects=True to allow it"
            )
        return out

    def log(self, phase: int, *, result: Any = None, error: BaseException | None = None,
            attempts: int = 0, reconciled: bool = False) -> None:
        body = pb.EffectCall()
        body.CopyFrom(self.body)
        body.phase = phase
        body.attempts = attempts
        body.reconciled = reconciled
        if self.started:
            body.latency_us = (time.perf_counter_ns() - self.started) // 1000
        if error is not None:
            body.error.CopyFrom(make_error(error))
        if phase == Phase.PHASE_CONFIRMED:
            body.result.CopyFrom(make_payload(result))
        ev = new_event(self.step)
        ev.effect.CopyFrom(body)
        self.session.append(ev)

    def begin(self) -> None:
        self.log(Phase.PHASE_PENDING)
        self.started = time.perf_counter_ns()

    def confirmed(self, result: Any, attempts: int) -> None:
        self.log(Phase.PHASE_CONFIRMED, result=result, attempts=attempts)
        self.session.count_call(live=True, kind="effect")

    def failed(self, exc: BaseException, attempts: int) -> None:
        self.log(Phase.PHASE_FAILED, error=exc, attempts=attempts)
        self.session.count_call(live=True, kind="effect")

    def reconciled(self, result: Any, attempts: int) -> None:
        self.log(Phase.PHASE_CONFIRMED, result=result, attempts=attempts, reconciled=True)
        self.session.count_call(live=True, kind="effect")

    def in_doubt(self, exc: BaseException, attempts: int) -> EffectInDoubt:
        self.log(Phase.PHASE_PENDING, error=exc, attempts=attempts)
        self.session.count_call(live=True, kind="effect")
        return EffectInDoubt(
            self.body.name, step_key(self.step), self.body.idempotency_key, attempts
        )


def _delay(backoff: float, attempt: int) -> float:
    return backoff * (2 ** (attempt - 1)) * random.uniform(0.5, 1.0)


def _start(call: _Call) -> tuple[pb.Event | None, Any]:
    """(the PENDING event a resume must settle, or None; an answer from the log, or GO_LIVE).

    Logs PENDING when the effect is about to run for the first time.
    """
    if call.session.is_replay:
        out = call.from_log()
        if isinstance(out, Settle):
            recorded = out.event.effect
            if recorded.idempotency_key != call.body.idempotency_key:
                raise RuntimeError(
                    f"effect {recorded.name} at step {step_key(call.step)} was logged with "
                    "a different idempotency key; refusing to settle it"
                )
            return out.event, GO_LIVE
        if out is not GO_LIVE:
            return None, out
    call.begin()
    return None, GO_LIVE


def _wrap(
    fn: F,
    name: str,
    normalizer: Normalizer,
    retry_on: tuple[type[BaseException], ...],
    attempts: int,
    backoff: float,
    reconcile: Callable[..., Any] | None,
) -> F:
    if not _accepts_key(fn):
        raise TypeError(f"effect {name} must accept an {KEY_ARG} keyword argument")
    if reconcile is not None and not _accepts_key(reconcile):
        raise TypeError(f"reconcile for effect {name} must accept an {KEY_ARG} keyword argument")
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            session = current_session()
            if session is None:
                kwargs.setdefault(KEY_ARG, uuid.uuid4().hex)
                return await fn(*args, **kwargs)
            call = _Call(session, name, normalizer, args, kwargs)
            settling, out = _start(call)
            if out is not GO_LIVE:
                return out
            key = {KEY_ARG: call.body.idempotency_key}
            done = settling.effect.attempts if settling is not None else 0
            if settling is not None and reconcile is not None:
                found = reconcile(*args, **kwargs, **key)
                if inspect.isawaitable(found):
                    found = await found
                if found is not None:
                    call.reconciled(found, done)
                    return found
            for attempt in range(1, attempts + 1):
                try:
                    result = await fn(*args, **kwargs, **key)
                except retry_on as exc:
                    if attempt == attempts:
                        raise call.in_doubt(exc, done + attempt) from exc
                    await asyncio.sleep(_delay(backoff, attempt))
                except Exception as exc:
                    call.failed(exc, done + attempt)
                    raise
                else:
                    call.confirmed(result, done + attempt)
                    return result
            raise AssertionError("unreachable")

        async_wrapper.__ledger_effect__ = name  # type: ignore[attr-defined]
        return async_wrapper  # type: ignore[return-value]

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        session = current_session()
        if session is None:
            kwargs.setdefault(KEY_ARG, uuid.uuid4().hex)
            return fn(*args, **kwargs)
        call = _Call(session, name, normalizer, args, kwargs)
        settling, out = _start(call)
        if out is not GO_LIVE:
            return out
        key = {KEY_ARG: call.body.idempotency_key}
        done = settling.effect.attempts if settling is not None else 0
        if settling is not None and reconcile is not None:
            found = reconcile(*args, **kwargs, **key)
            if found is not None:
                call.reconciled(found, done)
                return found
        for attempt in range(1, attempts + 1):
            try:
                result = fn(*args, **kwargs, **key)
            except retry_on as exc:
                if attempt == attempts:
                    raise call.in_doubt(exc, done + attempt) from exc
                time.sleep(_delay(backoff, attempt))
            except Exception as exc:
                call.failed(exc, done + attempt)
                raise
            else:
                call.confirmed(result, done + attempt)
                return result
        raise AssertionError("unreachable")

    wrapper.__ledger_effect__ = name  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


@overload
def effect(fn: F) -> F: ...
@overload
def effect(
    *,
    name: str | None = None,
    retry_on: tuple[type[BaseException], ...] = ...,
    attempts: int = ...,
    backoff: float = ...,
    reconcile: Callable[..., Any] | None = None,
    normalizer: Normalizer | None = None,
) -> Callable[[F], F]: ...


def effect(
    fn: F | None = None,
    *,
    name: str | None = None,
    retry_on: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError),
    attempts: int = 3,
    backoff: float = 0.25,
    reconcile: Callable[..., Any] | None = None,
    normalizer: Normalizer | None = None,
) -> Any:
    """Make a side-effecting function happen once per run step.

    The function must take an ``idempotency_key`` keyword argument and pass
    it to the API. ``retry_on`` lists the errors after which the outcome is
    unknown and a retry with the same key is safe (timeouts, dropped
    connections, 5xx); any other exception means the API refused and the
    effect did not happen. ``attempts`` counts the first try; retries wait
    ``backoff`` seconds, doubling, with jitter.

    ``reconcile`` is used when a resume finds the effect PENDING in the log.
    It takes the effect's arguments and ``idempotency_key`` and returns the
    effect's result if the API shows it happened, or None if it didn't. Without
    it (or when it returns None) the resume calls the effect again with the
    same key, which is safe as long as the API still remembers the key.

    Outside a session the function runs as is, with a fresh random key
    unless the caller passes one.
    """

    def decorate(f: F) -> F:
        return _wrap(
            f, name or f"{f.__module__}.{f.__qualname__}", normalizer or DEFAULT,
            tuple(retry_on), attempts, backoff, reconcile,
        )

    return decorate(fn) if fn is not None else decorate
