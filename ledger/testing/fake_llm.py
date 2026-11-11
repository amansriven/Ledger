"""A fake OpenAI-compatible LLM server, plus a fake search tool endpoint.

Replies are deliberately nondeterministic: each one carries a random nonce
and arrives after a random delay. A replay that reproduces a recorded run
exactly can therefore only have come from the log, and parallel branches
finish in a different order from run to run.

Endpoints:
    POST /v1/chat/completions   OpenAI chat completion (non-streaming)
    GET  /tools/search?q=...    {"query": q, "results": [...]}
    GET  /stats                 request counts per endpoint
    GET  /healthz

Run it with ``python -m ledger.testing.fake_llm --port 8787``.
"""

from __future__ import annotations

import argparse
import json
import random
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

__all__ = ["FakeLLMServer", "main"]


class _Handler(BaseHTTPRequestHandler):
    server: FakeLLMServer

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        if self.server.verbose:
            super().log_message(format, *args)

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _delay(self) -> None:
        lo, hi = self.server.latency_ms
        time.sleep(random.uniform(lo, hi) / 1000)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/healthz":
            return self._send(200, {"ok": True})
        if url.path == "/stats":
            return self._send(200, self.server.stats())
        if url.path == "/tools/search":
            self.server.count("search")
            self._delay()
            q = parse_qs(url.query).get("q", [""])[0]
            results = [
                f"fact {i} about {q} ({secrets.token_hex(3)})" for i in range(1, 4)
            ]
            return self._send(200, {"query": q, "results": results})
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        url = urlparse(self.path)
        if url.path != "/v1/chat/completions":
            return self._send(404, {"error": "not found"})
        self.server.count("chat")
        length = int(self.headers.get("Content-Length") or 0)
        req = json.loads(self.rfile.read(length) or b"{}")
        if req.get("stream"):
            return self._send(400, {"error": {"message": "streaming not supported"}})
        self._delay()
        messages = req.get("messages") or []
        last = str(messages[-1].get("content", "")) if messages else ""
        content = f"[{req.get('model', 'fake')}] {last[:80]} :: {secrets.token_hex(4)}"
        prompt_tokens = sum(len(str(m.get("content", "")).split()) for m in messages)
        completion_tokens = len(content.split())
        self._send(
            200,
            {
                "id": f"chatcmpl-{secrets.token_hex(8)}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": req.get("model", "fake-model"),
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
            },
        )


class FakeLLMServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        latency_ms: tuple[float, float] = (0, 30),
        verbose: bool = False,
    ) -> None:
        super().__init__((host, port), _Handler)
        self.latency_ms = latency_ms
        self.verbose = verbose
        self._counts: dict[str, int] = {"chat": 0, "search": 0}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def count(self, endpoint: str) -> None:
        with self._lock:
            self._counts[endpoint] += 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def start(self) -> FakeLLMServer:
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self.shutdown()
        self.server_close()

    def __enter__(self) -> FakeLLMServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8787)
    p.add_argument("--min-latency-ms", type=float, default=0)
    p.add_argument("--max-latency-ms", type=float, default=30)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    server = FakeLLMServer(
        args.host,
        args.port,
        latency_ms=(args.min_latency_ms, args.max_latency_ms),
        verbose=args.verbose,
    )
    print(f"fake LLM listening on {server.base_url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
