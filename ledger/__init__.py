"""Ledger: replayable, crash-safe agent runs under LangGraph.

    import ledger
    from openai import OpenAI

    llm = ledger.wrap(OpenAI())
    graph = ledger.instrument(builder.compile())

    graph.invoke(inputs)                      # recorded as a new run
    with ledger.replay(run_id) as session:    # answered from the log
        graph.invoke(inputs)
    with ledger.fork(run_id, at=step, edit="other text"):   # counterfactual
        graph.invoke(inputs)

Inside nodes, read the clock, ids and randomness through ``ledger.now()``,
``ledger.time()``, ``ledger.uuid4()`` and ``ledger.rng()`` so replays see the
recorded values.
"""

from ledger.core.clock import now, rng, time, uuid4
from ledger.core.normalize import EXACT, Normalizer
from ledger.core.session import Mode, Session, current_session, record
from ledger.core.store import EventStore
from ledger.effects import EffectInDoubt, LiveEffectRefused, effect
from ledger.intercept.langgraph import instrument
from ledger.intercept.llm import wrap
from ledger.intercept.tools import tool
from ledger.replay.engine import (
    RERUN,
    DivergencePoint,
    DivergenceReason,
    ReplayDivergence,
    ReplayedError,
    ReplayMismatch,
    replay,
)
from ledger.replay.fork import StepInfo, fork, steps

__all__ = [
    "DivergencePoint",
    "DivergenceReason",
    "EXACT",
    "EffectInDoubt",
    "RERUN",
    "EventStore",
    "LiveEffectRefused",
    "Mode",
    "Normalizer",
    "ReplayDivergence",
    "ReplayMismatch",
    "ReplayedError",
    "Session",
    "StepInfo",
    "current_session",
    "effect",
    "fork",
    "instrument",
    "now",
    "record",
    "replay",
    "rng",
    "steps",
    "time",
    "tool",
    "uuid4",
    "wrap",
]

__version__ = "0.0.1"
