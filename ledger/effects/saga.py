"""Sagas: undo a run's confirmed effects when it fails for good.

An effect declared with ``compensate=`` is remembered once it is confirmed.
If the graph then raises an error that resuming can't fix, the instrumented
graph runs the compensations of every confirmed effect, newest first, before
the run is logged as failed and the error is raised:

    @ledger.effect(compensate=release_claim)
    def claim_charge(charge_id, ticket_id, *, idempotency_key): ...

    def release_claim(claimed, charge_id, ticket_id, *, idempotency_key): ...

A compensation is an effect itself: it is logged PENDING then CONFIRMED (or
FAILED) at a step of its own (the effect's step, with ``:compensate`` added
to the node), with its own idempotency key and the effect's retry policy.
So a crash during compensation is resumed like any other: the resume fails
the same way again, compensations already confirmed are answered from the
log, one left pending is settled, and the rest run.

Errors that don't count as failing for good: ``EffectInDoubt`` (a resume may
still settle the effect, and the run go on), Ledger's own replay errors, and
anything that isn't an ``Exception`` (a ``KeyboardInterrupt`` is a crash, not
a verdict). A replay of a compensated run answers the compensations from
the log like any effect.

If a compensation fails, the others still run, and the run fails with
``SagaIncomplete`` (chained from the original error). A compensation left in
doubt is settled by resuming the run. One the API refused (FAILED) is
refused again on every resume, since the log answers it: that needs a
person, and the run is final.
"""

from __future__ import annotations

import inspect
from typing import Any

from ledger.core.events import pb
from ledger.core.session import Session
from ledger.core.store import step_key
from ledger.effects.manager import (
    KEY_ARG,
    Compensation,
    EffectInDoubt,
    LiveEffectRefused,
    _Call,
    aexecute,
    execute,
)

__all__ = ["SagaIncomplete", "acompensate", "compensate", "compensation_step", "fails_for_good"]

SUFFIX = ":compensate"


class SagaIncomplete(RuntimeError):
    """The run failed and some of its effects could not be compensated."""

    def __init__(self, failures: list[tuple[str, str, BaseException]]) -> None:
        names = "; ".join(
            f"{name} at {step}: {type(exc).__name__}: {exc}" for name, step, exc in failures
        )
        super().__init__(
            f"{len(failures)} compensation(s) did not complete ({names}); "
            "resume the run if one is in doubt, otherwise this needs a person"
        )
        self.failures = failures


def fails_for_good(error: BaseException) -> bool:
    """Whether ``error`` ends the run for good, so its effects get compensated."""
    from ledger.replay.engine import ReplayDivergence, ReplayMismatch

    if not isinstance(error, Exception):
        return False
    return not isinstance(
        error, (EffectInDoubt, SagaIncomplete, LiveEffectRefused, ReplayMismatch, ReplayDivergence)
    )


def compensation_step(step: pb.StepRef) -> pb.StepRef:
    """Where the compensation of the effect at ``step`` is logged."""
    out = pb.StepRef()
    out.CopyFrom(step)
    out.node = step.node + SUFFIX
    return out


def _call(session: Session, c: Compensation) -> _Call:
    return _Call(
        session, c.name + SUFFIX, c.normalizer, c.args, c.kwargs,
        step=compensation_step(c.step), undoes=c.result,
    )


def _invoke(c: Compensation) -> Any:
    return lambda key: c.fn(c.result, *c.args, **c.kwargs, **{KEY_ARG: key})


def _raise_if_failed(failures: list[tuple[str, str, BaseException]], error: BaseException) -> None:
    if failures:
        raise SagaIncomplete(failures) from error


def compensate(session: Session, error: BaseException) -> None:
    """Undo the session's confirmed effects, newest first. Raises SagaIncomplete."""
    failures: list[tuple[str, str, BaseException]] = []
    for c in reversed(session.take_compensations()):
        if inspect.iscoroutinefunction(c.fn):
            raise TypeError(f"compensation of {c.name} is async; invoke the graph with ainvoke")
        try:
            execute(_call(session, c), _invoke(c), None, c.policy)
        except Exception as exc:
            failures.append((c.name, step_key(c.step), exc))
    _raise_if_failed(failures, error)


async def acompensate(session: Session, error: BaseException) -> None:
    """``compensate`` for ``ainvoke``: compensations may be sync or async."""
    failures: list[tuple[str, str, BaseException]] = []
    for c in reversed(session.take_compensations()):
        try:
            if inspect.iscoroutinefunction(c.fn):
                await aexecute(_call(session, c), _invoke(c), None, c.policy)
            else:
                execute(_call(session, c), _invoke(c), None, c.policy)
        except Exception as exc:
            failures.append((c.name, step_key(c.step), exc))
    _raise_if_failed(failures, error)
