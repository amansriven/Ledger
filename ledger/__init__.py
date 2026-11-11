"""Ledger: replayable, crash-safe agent runs under LangGraph.

    import ledger
    from openai import OpenAI

    llm = ledger.wrap(OpenAI())
    graph = ledger.instrument(builder.compile())

    graph.invoke(inputs)                      # recorded as a new run
    with ledger.replay(run_id) as session:    # answered from the log
        graph.invoke(inputs)
"""

from ledger.core.session import Mode, Session, current_session, record
from ledger.core.store import EventStore
from ledger.intercept.langgraph import instrument
from ledger.intercept.llm import wrap
from ledger.intercept.tools import tool
from ledger.replay.engine import ReplayDivergence, ReplayedError, ReplayMismatch, replay

__all__ = [
    "EventStore",
    "Mode",
    "ReplayDivergence",
    "ReplayMismatch",
    "ReplayedError",
    "Session",
    "current_session",
    "instrument",
    "record",
    "replay",
    "tool",
    "wrap",
]

__version__ = "0.0.1"
