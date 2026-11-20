"""LangGraph integration.

    llm = ledger.wrap(OpenAI())
    graph = ledger.instrument(builder.compile())

``instrument`` returns a proxy for a compiled graph. ``invoke``/``ainvoke``
log the run's input and output, and when the graph fails for good, undo its
confirmed effects that have compensations (``ledger.effects.saga``). If no session is active, each invocation is
recorded as a new run in the default store (``$LEDGER_DB`` or ``ledger.db``).
Inside ``ledger.replay(...)`` the same call replays instead.

``stream``/``astream`` pass through for now: calls inside them are still
intercepted when a session is active, but the run's input and output are not
logged.
"""

from __future__ import annotations

from typing import Any

from ledger.core.session import Session, current_session, record

__all__ = ["instrument", "InstrumentedGraph"]


class InstrumentedGraph:
    def __init__(self, graph: Any) -> None:
        self._graph = graph

    def __getattr__(self, name: str) -> Any:
        return getattr(self._graph, name)

    @property
    def graph(self) -> Any:
        return self._graph

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        session = current_session()
        if session is None:
            with record() as session:
                return self._invoke(session, input, config, kwargs)
        return self._invoke(session, input, config, kwargs)

    def _invoke(self, session: Session, input: Any, config: Any, kwargs: dict[str, Any]) -> Any:
        from ledger.effects.saga import SagaIncomplete, compensate, fails_for_good

        session.run_started(input)
        try:
            output = self._graph.invoke(input, config, **kwargs)
        except BaseException as exc:
            final: BaseException = exc
            if fails_for_good(exc):
                try:
                    compensate(session, exc)
                except SagaIncomplete as incomplete:
                    final = incomplete
            session.run_finished(error=final)
            if final is exc:
                raise
            raise final from exc
        session.run_finished(output)
        return output

    async def ainvoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        session = current_session()
        if session is None:
            with record() as session:
                return await self._ainvoke(session, input, config, kwargs)
        return await self._ainvoke(session, input, config, kwargs)

    async def _ainvoke(
        self, session: Session, input: Any, config: Any, kwargs: dict[str, Any]
    ) -> Any:
        from ledger.effects.saga import SagaIncomplete, acompensate, fails_for_good

        session.run_started(input)
        try:
            output = await self._graph.ainvoke(input, config, **kwargs)
        except BaseException as exc:
            final: BaseException = exc
            if fails_for_good(exc):
                try:
                    await acompensate(session, exc)
                except SagaIncomplete as incomplete:
                    final = incomplete
            session.run_finished(error=final)
            if final is exc:
                raise
            raise final from exc
        session.run_finished(output)
        return output


def instrument(graph: Any) -> InstrumentedGraph:
    """Make a compiled LangGraph graph record and replay its runs."""
    if isinstance(graph, InstrumentedGraph):
        return graph
    return InstrumentedGraph(graph)
