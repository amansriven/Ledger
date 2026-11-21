from __future__ import annotations

import socket
from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from openai import OpenAI

import ledger
from ledger.examples import refund_agent
from ledger.examples.toy_pipeline import build_graph
from ledger.core.events import load_payload
from ledger.testing.fake_llm import FakeLLMServer
from ledger.testing.fake_stripe import FakeStripeServer


@pytest.fixture
def fake_server() -> Iterator[FakeLLMServer]:
    with FakeLLMServer(latency_ms=(0, 15)) as server:
        yield server


@pytest.fixture
def store(tmp_path) -> Iterator[ledger.EventStore]:
    with ledger.EventStore(tmp_path / "ledger.db") as s:
        yield s


def make_toy_graph(base_url: str, model: str = "fake-model"):
    llm = ledger.wrap(OpenAI(base_url=f"{base_url}/v1", api_key="fake", max_retries=0))
    return build_graph(llm, tools_url=f"{base_url}/tools", model=model)


@pytest.fixture
def toy_graph(fake_server):
    return make_toy_graph(fake_server.base_url)


@pytest.fixture
def network_off(monkeypatch):
    """Make every outbound socket connection fail, like running with no network."""

    def deny(*args, **kwargs):
        raise OSError("network is disabled for this test")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)
    monkeypatch.setattr(socket, "create_connection", deny)


@pytest.fixture
def stripe_server() -> Iterator[FakeStripeServer]:
    with FakeStripeServer() as server:
        yield server


@pytest.fixture
def agent(fake_server, stripe_server):
    """The refund agent, wired to the fake LLM and fake Stripe servers."""
    llm = ledger.wrap(OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))
    client = refund_agent.stripe_client("sk_test_fake", stripe_server.base_url)
    return refund_agent.build_graph(llm, client, model="fake-model")


class Crash(BaseException):
    """The process dying: nothing after it reaches the log."""


@contextmanager
def crash_after(store: ledger.EventStore, n: int) -> Iterator[None]:
    """Let ``n`` events reach the log, then fail every append, like a kill -9."""
    real = store.append
    written = 0

    def append(run_id, event):
        nonlocal written
        if written >= n:
            raise Crash(f"killed before event {n}")
        written += 1
        return real(run_id, event)

    store.append = append  # type: ignore[method-assign]
    try:
        with pytest.raises(Crash):
            yield
    finally:
        del store.append


def replays_strictly(store: ledger.EventStore, run_id: str, graph, expected) -> None:
    """Assert the run replays from its log alone, with ``expected`` as output."""
    with ledger.replay(run_id, store) as rep:
        out = graph.invoke(load_payload(rep.replay_index.input))
    assert out == expected
    assert rep.stats.live_calls == 0
