from __future__ import annotations

import threading
import urllib.request

from ledger.testing.fake_llm import FakeLLMServer


def test_a_burst_of_connections_is_not_reset():
    # The stdlib backlog of 5 made macOS reset connections when parallel
    # benchmark trials connected at once.
    n = 64
    barrier = threading.Barrier(n)
    errors: list[BaseException] = []

    with FakeLLMServer(latency_ms=(0, 0)) as server:
        def get() -> None:
            barrier.wait()
            try:
                with urllib.request.urlopen(f"{server.base_url}/healthz", timeout=10) as r:
                    r.read()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=get) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert not errors, f"{len(errors)} of {n} failed: {errors[0]!r}"
