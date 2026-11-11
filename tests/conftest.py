from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest
from openai import OpenAI

import ledger
from examples.toy_pipeline import build_graph
from ledger.testing.fake_llm import FakeLLMServer


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
