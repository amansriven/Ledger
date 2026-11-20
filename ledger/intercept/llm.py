"""Interceptor for OpenAI-compatible clients.

``ledger.wrap(client)`` returns a proxy that behaves like the client. Calls to
``client.chat.completions.create`` are recorded or replayed when a Ledger
session is active and passed straight through otherwise. Everything else on
the client is forwarded untouched.

Any provider with an OpenAI-compatible endpoint (NVIDIA NIM, vLLM, Together,
a local server) plugs in the same way: point the client's ``base_url`` at it.

Replay matches a request on its normalized form (``ledger.core.normalize``):
timestamps and ids in the prompt are masked, and ``timeout`` and
``extra_headers`` are ignored. Pass ``normalizer=`` to change that.

Not yet supported: ``stream=True``, ``with_raw_response``, the Responses API.
"""

from __future__ import annotations

import inspect
import time
from typing import Any

from ledger.core.events import make_payload, pb
from ledger.core.normalize import LLM_DEFAULT, Normalizer
from ledger.core.session import current_session
from ledger.intercept._common import GO_LIVE, answer_from_log, log_live

__all__ = ["wrap"]

OPERATION = "chat.completions.create"


def _decode_completion(data: Any) -> Any:
    from openai.types.chat import ChatCompletion

    return ChatCompletion.model_validate(data)


def _encode_completion(resp: Any) -> Any:
    return resp.model_dump(mode="json") if hasattr(resp, "model_dump") else resp


class _Completions:
    def __init__(self, inner: Any, base_url: str, normalizer: Normalizer) -> None:
        self._inner = inner
        self._base_url = base_url
        self._normalizer = normalizer
        # openai's decorators hide that AsyncCompletions.create is a coroutine
        # function, so check the unwrapped function and the resource type.
        self._is_async = inspect.iscoroutinefunction(
            inspect.unwrap(inner.create)
        ) or type(inner).__name__.startswith("Async")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _prepare(self, kwargs: dict[str, Any]) -> tuple[pb.LLMCall, pb.Payload]:
        if kwargs.get("stream"):
            raise NotImplementedError("Ledger does not record streaming completions yet")
        request = make_payload(kwargs)
        body = pb.LLMCall(
            base_url=self._base_url,
            operation=OPERATION,
            model=str(kwargs.get("model", "")),
            request=request,
            request_key=self._normalizer.key_of(request),
        )
        return body, request

    def create(self, **kwargs: Any) -> Any:
        if self._is_async:
            return self._acreate(**kwargs)
        session = current_session()
        if session is None:
            return self._inner.create(**kwargs)
        body, request = self._prepare(kwargs)
        step = session.next_step()
        if session.is_replay:
            out = answer_from_log(
                session, step, "llm_call", request, _decode_completion,
                match_key=body.request_key,
            )
            if out is not GO_LIVE:
                return out
        started = time.perf_counter_ns()
        try:
            resp = self._inner.create(**kwargs)
        except Exception as exc:
            log_live(session, step, "llm_call", body, error=exc, started_ns=started)
            raise
        log_live(
            session, step, "llm_call", body,
            response=resp, started_ns=started, encode=_encode_completion,
        )
        return resp

    async def _acreate(self, **kwargs: Any) -> Any:
        session = current_session()
        if session is None:
            return await self._inner.create(**kwargs)
        body, request = self._prepare(kwargs)
        step = session.next_step()
        if session.is_replay:
            out = answer_from_log(
                session, step, "llm_call", request, _decode_completion,
                match_key=body.request_key,
            )
            if out is not GO_LIVE:
                return out
        started = time.perf_counter_ns()
        try:
            resp = await self._inner.create(**kwargs)
        except Exception as exc:
            log_live(session, step, "llm_call", body, error=exc, started_ns=started)
            raise
        log_live(
            session, step, "llm_call", body,
            response=resp, started_ns=started, encode=_encode_completion,
        )
        return resp


class _Chat:
    def __init__(self, inner: Any, base_url: str, normalizer: Normalizer) -> None:
        self._inner = inner
        self.completions = _Completions(inner.completions, base_url, normalizer)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class LedgerClient:
    """Proxy around an OpenAI or AsyncOpenAI client."""

    def __init__(self, client: Any, normalizer: Normalizer = LLM_DEFAULT) -> None:
        self._client = client
        self.chat = _Chat(client.chat, str(getattr(client, "base_url", "")), normalizer)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    @property
    def wrapped(self) -> Any:
        return self._client


def wrap(client: Any, *, normalizer: Normalizer | None = None) -> LedgerClient:
    """Wrap an OpenAI-compatible client so its completions are recorded and replayed.

    ``normalizer`` decides which requests count as the same during replay;
    the default masks timestamps and ids (see ``ledger.core.normalize``).
    """
    if isinstance(client, LedgerClient):
        if normalizer is None:
            return client
        client = client.wrapped
    return LedgerClient(client, normalizer or LLM_DEFAULT)
