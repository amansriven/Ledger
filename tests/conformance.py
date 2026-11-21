"""Conformance check for the two trace servers (`ledger ui` and the Go server).

Both must answer ``ledger.v1.TraceService`` identically over the same store.
``responses(base_url)`` sends a fixed plan of requests and returns every
answer normalized, so two servers agree when their results compare equal:

    build_store(path)                    # the demo store plus a large-payload run
    # start `ledger ui --db path --port 0` and the Go server on the same file
    assert responses(python_url) == responses(go_url)

The plan is fixed, but run ids in the demo store are random, so it starts
with ListRuns and then asks about every run it lists: GetRun, ListDiagnoses,
WatchRun from 0 and from near the end, GetPayload of each payload left out
of an event plus one that was inlined from the blob store, and requests that
must fail (unknown run or digest, a malformed digest, a bad body, the wrong
Content-Type).

Normalizing keeps what the API promises and drops what may differ:
successful responses are compared as parsed JSON (the ``json`` string of a
Diagnosis is parsed too, since it is JSON held in a string); errors keep the
HTTP status and the Connect code but not the message; a stream becomes its
list of messages plus its end-of-stream error code (or None).

Standard library only, so it runs anywhere the tests do:

    uv run python tests/conformance.py http://127.0.0.1:8700 [other_url]
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import struct
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

SERVICE = "ledger.v1.TraceService"
UNKNOWN_RUN = "no-such-run"
UNKNOWN_DIGEST = "0" * 64
LARGE_RESULT_BYTES = 300 * 1024  # over the 256 KiB inline limit
MEDIUM_RESULT_BYTES = 10 * 1024  # in the blob store, but under the limit
BLOB_THRESHOLD = 4096  # ledger.core.store.DEFAULT_BLOB_THRESHOLD


# A tiny Connect client


def _connection(base_url: str, timeout: float) -> http.client.HTTPConnection:
    parts = urlsplit(base_url)
    return http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)


def call(
    base_url: str, method: str, body: Any = None, *,
    content_type: str = "application/json", raw: bytes | None = None, timeout: float = 30,
) -> tuple[int, Any]:
    """A unary call: (HTTP status, the parsed JSON body or None)."""
    data = raw if raw is not None else json.dumps(body or {}).encode()
    conn = _connection(base_url, timeout)
    try:
        conn.request("POST", f"/{SERVICE}/{method}", data, {
            "Content-Type": content_type, "Connect-Protocol-Version": "1",
        })
        resp = conn.getresponse()
        text = resp.read()
        return resp.status, (json.loads(text) if text else None)
    finally:
        conn.close()


def envelope(flags: int, data: bytes) -> bytes:
    return struct.pack(">BI", flags, len(data)) + data


class Stream:
    """A server-streaming call, read one envelope at a time.

    Iterating yields each message (parsed JSON); afterwards ``status`` is the
    HTTP status and ``end`` the end-of-stream message (None if the stream
    broke off without one).
    """

    def __init__(self, base_url: str, method: str, body: Any, *,
                 content_type: str = "application/connect+json", timeout: float = 30) -> None:
        self.conn = _connection(base_url, timeout)
        self.conn.request(
            "POST", f"/{SERVICE}/{method}", envelope(0, json.dumps(body).encode()),
            {"Content-Type": content_type, "Connect-Protocol-Version": "1"},
        )
        self.resp = self.conn.getresponse()
        self.status = self.resp.status
        self.end: Any = None

    def __iter__(self) -> Iterator[Any]:
        try:
            if self.status != 200:
                self.resp.read()
                return
            while True:
                head = self.resp.read(5)
                if len(head) < 5:
                    return
                flags, length = struct.unpack(">BI", head)
                msg = json.loads(self.resp.read(length))
                if flags & 0x02:
                    self.end = msg
                    return
                yield msg
        finally:
            self.conn.close()

    def close(self) -> None:
        self.conn.close()


def watch(base_url: str, run_id: str, from_index: int = 0, **kw: Any) -> tuple[int, list[Any], Any]:
    """(HTTP status, messages, end-of-stream message) of a whole WatchRun."""
    s = Stream(base_url, "WatchRun", {"runId": run_id, "fromIndex": str(from_index)}, **kw)
    msgs = list(s)
    return s.status, msgs, s.end


# Normalizing


def _norm_error(status: int, body: Any) -> dict[str, Any]:
    return {"status": status, "code": (body or {}).get("code")}


def _norm_ok(status: int, body: Any) -> dict[str, Any]:
    if status != 200:
        return _norm_error(status, body)
    for d in (body or {}).get("diagnoses", []):
        if "json" in d:
            d["json"] = json.loads(d["json"])
    return {"status": 200, "body": body}


def _norm_watch(status: int, msgs: list[Any], end: Any) -> dict[str, Any]:
    error = (end or {}).get("error")
    return {
        "status": status,
        "messages": msgs,
        "end": None if end is None else (error["code"] if error else "ok"),
    }


def left_out(event: dict[str, Any]) -> Iterator[str]:
    """Digests of the payloads an event (in JSON) came back without."""
    for key, body in event.items():
        if isinstance(body, dict):
            for value in body.values():
                if isinstance(value, dict) and "sha256" in value and not value.get("json"):
                    yield value["sha256"]


def responses(base_url: str) -> list[tuple[str, Any]]:
    """Every answer in the plan, normalized, labelled by what was asked."""
    out: list[tuple[str, Any]] = []

    def add(label: str, value: Any) -> None:
        out.append((label, value))

    status, listed = call(base_url, "ListRuns", {})
    add("ListRuns", _norm_ok(status, listed))
    runs = (listed or {}).get("runs", [])

    digests: set[str] = set()
    inlined_from_blobs: set[str] = set()
    for run in runs:
        rid = run["runId"]
        status, got = call(base_url, "GetRun", {"runId": rid})
        add(f"GetRun {rid}", _norm_ok(status, got))
        for ev in (got or {}).get("events", []):
            digests.update(left_out(ev))
            for body in ev.values():
                if isinstance(body, dict):
                    for p in body.values():
                        # Over the store's default blob threshold: in the blob store.
                        if isinstance(p, dict) and len(payload_bytes(p)) > BLOB_THRESHOLD:
                            inlined_from_blobs.add(p["sha256"])
        add(f"ListDiagnoses {rid}", _norm_ok(*call(base_url, "ListDiagnoses", {"runId": rid})))
        add(f"WatchRun {rid} from 0", _norm_watch(*watch(base_url, rid, 0)))
        n = int(run.get("events", 0))
        add(f"WatchRun {rid} from {max(n - 2, 0)}", _norm_watch(*watch(base_url, rid, max(n - 2, 0))))
        add(f"WatchRun {rid} from {n + 5}", _norm_watch(*watch(base_url, rid, n + 5)))

    for digest in sorted(digests) + sorted(inlined_from_blobs)[:1]:
        add(f"GetPayload {digest}", _norm_ok(*call(base_url, "GetPayload", {"sha256": digest})))

    # Failures.
    add("GetRun unknown", _norm_ok(*call(base_url, "GetRun", {"runId": UNKNOWN_RUN})))
    add("GetRun empty", _norm_ok(*call(base_url, "GetRun", {})))
    add("ListDiagnoses unknown", _norm_ok(*call(base_url, "ListDiagnoses", {"runId": UNKNOWN_RUN})))
    add("GetPayload unknown", _norm_ok(*call(base_url, "GetPayload", {"sha256": UNKNOWN_DIGEST})))
    add("GetPayload malformed", _norm_ok(*call(base_url, "GetPayload", {"sha256": "../../etc/passwd"})))
    add("GetRun bad JSON", _norm_ok(*call(base_url, "GetRun", raw=b"{not json")))
    add("GetRun unknown field", _norm_ok(*call(base_url, "GetRun", {"runId": UNKNOWN_RUN, "extra": 1})))
    # A codec neither server has (connect-go always has application/proto,
    # while `ledger ui` speaks JSON only, so that one would differ).
    add("ListRuns as text", _norm_ok(*call(base_url, "ListRuns", {}, content_type="text/plain")))
    add("WatchRun unknown", _norm_watch(*watch(base_url, UNKNOWN_RUN)))
    if runs:
        rid = runs[0]["runId"]
        add("WatchRun negative", _norm_watch(*watch(base_url, rid, -1)))
        add("WatchRun as unary JSON",
            _norm_watch(*watch(base_url, rid, content_type="application/json")))
    return out


def diff(a: list[tuple[str, Any]], b: list[tuple[str, Any]]) -> list[str]:
    """Labels of the answers that differ, for a readable failure."""
    out = [f"{la}" for (la, va), (lb, vb) in zip(a, b) if la != lb or va != vb]
    if len(a) != len(b):
        out.append(f"{len(a)} answers vs {len(b)}")
    return out


# The store both servers are checked over


def add_large_payload_run(path: str | Path) -> str:
    """Append a run with one payload over the inline limit and one in the
    blob store under it, so GetPayload and the limit get exercised. Returns
    its run id."""
    import ledger
    from ledger.core.events import make_payload, new_event, pb

    big = {"blob": "x" * LARGE_RESULT_BYTES}
    medium = {"blob": "y" * MEDIUM_RESULT_BYTES}
    with ledger.EventStore(path) as store:
        run_id = "large-payloads"
        start = new_event()
        start.run_started.CopyFrom(pb.RunStarted(mode=pb.MODE_RECORD, input=make_payload({"q": 1})))
        store.append(run_id, start)
        for seq, (name, result) in enumerate([("fetch_big", big), ("fetch_medium", medium)]):
            ev = new_event(pb.StepRef(superstep=1, node="fetch", path="('__pregel_pull', 'fetch')", seq=seq))
            ev.tool_call.CopyFrom(pb.ToolCall(
                name=name, args=make_payload({"args": [], "kwargs": {}}), result=make_payload(result),
            ))
            store.append(run_id, ev)
        end = new_event()
        end.run_finished.CopyFrom(pb.RunFinished(output=make_payload({"ok": True})))
        store.append(run_id, end)
    return run_id


def build_store(path: str | Path) -> None:
    """The demo store (scripts/make_demo_db.py) plus ``add_large_payload_run``."""
    import importlib.util

    script = Path(__file__).resolve().parents[1] / "scripts" / "make_demo_db.py"
    spec = importlib.util.spec_from_file_location("make_demo_db", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.build(Path(path), latency_ms=(0, 5))
    add_large_payload_run(path)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def payload_bytes(payload: dict[str, Any]) -> bytes:
    """A Payload's bytes from its JSON form (bytes fields are base64)."""
    return base64.b64decode(payload.get("json", ""))


if __name__ == "__main__":
    urls = sys.argv[1:]
    if not 1 <= len(urls) <= 2:
        sys.exit("usage: conformance.py BASE_URL [OTHER_BASE_URL]")
    first = responses(urls[0])
    if len(urls) == 1:
        print(json.dumps(first, indent=2))
    else:
        differ = diff(first, responses(urls[1]))
        print("identical" if not differ else "differ:\n  " + "\n  ".join(differ))
        sys.exit(1 if differ else 0)
