from __future__ import annotations

import asyncio
import operator
from typing import Annotated, TypedDict

import pytest
from langgraph.graph import END, START, StateGraph
from openai import AsyncOpenAI, OpenAI

import ledger
from ledger.core.events import load_payload
from tests.conftest import make_toy_graph


def _record_one(graph, store, topic="fencing tokens"):
    with ledger.record(store) as session:
        graph.invoke({"topic": topic})
    return session.run_id


def test_changed_request_is_a_mismatch(fake_server, toy_graph, store):
    run_id = _record_one(toy_graph, store)
    edited = make_toy_graph(fake_server.base_url, model="another-model")
    with pytest.raises(ledger.ReplayMismatch, match="request at step .*plan"):
        with ledger.replay(run_id, store):
            edited.invoke({"topic": "fencing tokens"})


def test_changed_input_is_a_mismatch(toy_graph, store):
    run_id = _record_one(toy_graph, store)
    with pytest.raises(ledger.ReplayMismatch):
        with ledger.replay(run_id, store):
            toy_graph.invoke({"topic": "something else"})


def test_skipped_calls_are_a_divergence(fake_server, store):
    llm = ledger.wrap(OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))

    def ask():
        return llm.chat.completions.create(model="m", messages=[{"role": "user", "content": "hi"}])

    with ledger.record(store) as rec:
        ask()
        ask()
    with pytest.raises(ledger.ReplayDivergence, match="skipped 1"):
        with ledger.replay(rec.run_id, store):
            ask()


def test_recorded_tool_errors_are_raised_again(store):
    calls = []

    @ledger.tool(name="flaky")
    def flaky(x: int) -> int:
        calls.append(x)
        raise ValueError(f"bad {x}")

    with ledger.record(store) as rec:
        with pytest.raises(ValueError):
            flaky(3)
    with ledger.replay(rec.run_id, store):
        with pytest.raises(ValueError, match="^bad 3$") as exc:
            flaky(3)
    assert calls == [3]
    assert getattr(exc.value, "__ledger_replayed__", False)


class NeedsKeywords(Exception):
    def __init__(self, *, code: int) -> None:
        super().__init__(f"code {code}")
        self.code = code


def _replayed(exc: BaseException) -> BaseException:
    from ledger.core.events import make_error
    from ledger.replay.engine import replayed_exception

    return replayed_exception(make_error(exc))


@pytest.mark.parametrize(
    "original",
    [ValueError("bad 3"), KeyError("missing"), OSError(2, "No such file"), ValueError()],
)
def test_replayed_exceptions_keep_type_args_and_text(original):
    again = _replayed(original)
    assert type(again) is type(original)
    assert again.args == original.args
    assert str(again) == str(original)


def test_exception_with_keyword_only_init_keeps_type_and_text():
    again = _replayed(NeedsKeywords(code=7))
    assert type(again) is NeedsKeywords and str(again) == "code 7"


def test_unloaded_exception_class_falls_back_to_replayed_error():
    from ledger.core.events import pb
    from ledger.replay.engine import replayed_exception

    again = replayed_exception(pb.Error(type="not_imported_pkg.Boom", message="boom"))
    assert isinstance(again, ledger.ReplayedError)
    assert again.original_type == "not_imported_pkg.Boom"


def test_logs_without_args_rebuild_from_the_message():
    from ledger.core.events import pb
    from ledger.replay.engine import replayed_exception

    again = replayed_exception(pb.Error(type="builtins.ValueError", message="old log"))
    assert type(again) is ValueError and str(again) == "old log"


def test_unserializable_args_still_replay(store):
    @ledger.tool(name="odd")
    def odd() -> None:
        raise ValueError(object())

    with ledger.record(store) as rec:
        with pytest.raises(ValueError):
            odd()
    with ledger.replay(rec.run_id, store):
        with pytest.raises(ValueError, match="object at"):
            odd()


def test_recorded_llm_errors_are_raised_as_openai_errors(store):
    import openai

    llm = ledger.wrap(OpenAI(base_url="http://127.0.0.1:9/v1", api_key="x", max_retries=0))

    def ask():
        return llm.chat.completions.create(model="m", messages=[{"role": "user", "content": "hi"}])

    with ledger.record(store) as rec:
        with pytest.raises(openai.APIConnectionError) as recorded:
            ask()
    with ledger.replay(rec.run_id, store):
        with pytest.raises(openai.APIConnectionError) as replayed:
            ask()
    assert str(replayed.value) == str(recorded.value)


def test_calls_outside_a_session_pass_through(fake_server, store):
    llm = ledger.wrap(OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))
    resp = llm.chat.completions.create(model="m", messages=[{"role": "user", "content": "hi"}])
    assert resp.choices[0].message.content
    assert store.run_ids() == []


class AState(TypedDict, total=False):
    q: str
    out: Annotated[list[str], operator.add]


def test_async_graph_records_and_replays(fake_server, store, request):
    llm = ledger.wrap(AsyncOpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))

    @ledger.tool(name="upper")
    async def upper(s: str) -> str:
        await asyncio.sleep(0)
        return s.upper()

    def branch(tag: str):
        async def node(state: AState) -> AState:
            r = await llm.chat.completions.create(
                model="m", messages=[{"role": "user", "content": f"{tag} {state['q']}"}]
            )
            return {"out": [await upper(r.choices[0].message.content)]}

        return node

    b = StateGraph(AState)
    for tag in ("a", "b"):
        b.add_node(tag, branch(tag))
        b.add_edge(START, tag)
        b.add_edge(tag, END)
    graph = ledger.instrument(b.compile())

    with ledger.record(store) as rec:
        recorded = asyncio.run(graph.ainvoke({"q": "go"}))
    request.getfixturevalue("network_off")
    with ledger.replay(rec.run_id, store) as rep:
        replayed = asyncio.run(graph.ainvoke(load_payload(rep.replay_index.input)))
    assert replayed == recorded
    assert rep.stats == ledger.core.session.SessionStats(live_calls=0, replayed_calls=4)
