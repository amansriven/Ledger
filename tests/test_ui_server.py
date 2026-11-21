"""`ledger ui`'s trace server: every RPC over the demo store, the Connect
protocol details, live WatchRun streaming, static files and the command."""

from __future__ import annotations

import http.client
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from google.protobuf import json_format

import ledger
from ledger.cli import list_runs, main
from ledger.core.events import body_kind, make_error, make_payload, new_event, pb
from ledger.core.store import StoreNotFound
from ledger.testing.fake_llm import FakeLLMServer
from ledger.ui.server import INLINE_LIMIT, TraceServer

from conftest import crash_after, make_toy_graph
from conformance import (
    LARGE_RESULT_BYTES, MEDIUM_RESULT_BYTES, Stream, add_large_payload_run, build_store, call,
    diff, payload_bytes, responses, sha256, watch,
)


@pytest.fixture(scope="module")
def demo_path(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("ui") / "demo.db"
    build_store(path)
    return path


@pytest.fixture(scope="module")
def server(demo_path):
    with TraceServer(ledger.EventStore(demo_path, read_only=True), port=0, static_dir=None) as s:
        yield s


@pytest.fixture(scope="module")
def runs(server):
    status, body = call(server.url, "ListRuns", {})
    assert status == 200
    return {r["runId"]: r for r in body["runs"]}


def as_json(event: pb.Event) -> dict:
    return json_format.MessageToDict(event)


def request(url: str, method: str, path: str, headers: dict | None = None, body: bytes | None = None):
    parts = urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    try:
        conn.request(method, path, body, headers or {})
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


# ListRuns and GetRun


def test_list_runs_summarizes_every_run_as_ledger_runs_does(server, runs, demo_path):
    with ledger.EventStore(demo_path, read_only=True) as store:
        infos = list_runs(store)
        assert list(runs) == [i.run_id for i in infos]  # oldest first
        for info in infos:
            r = runs[info.run_id]
            events = list(store.events(info.run_id, hydrate=False))
            assert r["mode"] == f"MODE_{info.mode.upper()}"
            assert r.get("sourceRunId", "") == info.source
            assert r.get("graph", "") == info.graph
            assert r["status"] == f"RUN_STATUS_{info.status.upper()}"
            for field in ("calls", "live", "diverged", "resumed", "events"):
                assert r.get(field, 0) == getattr(info, field), field
            assert r.get("diagnoses", 0) == len(store.diagnoses(info.run_id))
            assert r["startedAt"] == json_format.MessageToDict(events[0])["recordedAt"]
            last_finished = [e for e in events if body_kind(e) == "run_finished"][-1]
            assert r["finishedAt"] == as_json(last_finished)["recordedAt"]
    kinds = [r["mode"] for r in runs.values()]
    assert "MODE_REPLAY" in kinds and "MODE_FORK" in kinds
    assert any(r.get("resumed") for r in runs.values())
    assert sum(r.get("diagnoses", 0) for r in runs.values()) == 1


def test_get_run_returns_the_summary_and_the_whole_log(server, runs, demo_path):
    with ledger.EventStore(demo_path, read_only=True) as store:
        for run_id, summary in runs.items():
            status, body = call(server.url, "GetRun", {"runId": run_id})
            assert status == 200
            assert body["summary"] == summary
            if run_id == "large-payloads":
                continue  # see the inline limit test
            assert body["events"] == [as_json(e) for e in store.events(run_id)]


def test_a_failed_run_carries_its_error(tmp_path):
    path = tmp_path / "failed.db"
    with ledger.EventStore(path) as store:
        for body in ("run_started", "run_finished"):
            ev = new_event()
            if body == "run_started":
                ev.run_started.CopyFrom(pb.RunStarted(mode=pb.MODE_RECORD, input=make_payload({})))
            else:
                ev.run_finished.CopyFrom(pb.RunFinished(error=make_error(ValueError("boom"))))
            store.append("failed", ev)
    with TraceServer(ledger.EventStore(path, read_only=True), port=0) as s:
        _, body = call(s.url, "GetRun", {"runId": "failed"})
    summary = body["summary"]
    assert summary["status"] == "RUN_STATUS_ERROR"
    assert summary["error"]["type"] == "builtins.ValueError"
    assert summary["error"]["message"] == "boom"
    assert summary["finishedAt"] == body["events"][1]["recordedAt"]


# Payloads


def test_payloads_over_the_inline_limit_are_left_out_and_fetched_by_digest(server):
    _, body = call(server.url, "GetRun", {"runId": "large-payloads"})
    tools = [e["toolCall"] for e in body["events"] if "toolCall" in e]
    big, medium = tools[0]["result"], tools[1]["result"]
    # Over 256 KiB: digest only.
    assert "json" not in big and re.fullmatch(r"[0-9a-f]{64}", big["sha256"])
    # In the blob store but under the limit: filled in.
    data = payload_bytes(medium)
    assert len(data) > MEDIUM_RESULT_BYTES and sha256(data) == medium["sha256"]
    assert len(data) <= INLINE_LIMIT

    status, got = call(server.url, "GetPayload", {"sha256": big["sha256"]})
    assert status == 200 and got["payload"]["sha256"] == big["sha256"]
    data = payload_bytes(got["payload"])
    assert sha256(data) == big["sha256"] and len(data) > LARGE_RESULT_BYTES
    assert json.loads(data) == {"blob": "x" * LARGE_RESULT_BYTES}

    # WatchRun leaves it out the same way.
    _, msgs, end = watch(server.url, "large-payloads")
    assert end == {} and [m["event"] for m in msgs] == body["events"]


def test_the_limit_applies_to_payloads_kept_inline_too(tmp_path):
    # A store with no blob threshold keeps even big payloads in the event.
    path = tmp_path / "inline.db"
    with ledger.EventStore(path, blob_threshold=None) as store:
        ev = new_event()
        ev.run_started.CopyFrom(pb.RunStarted(
            mode=pb.MODE_RECORD, input=make_payload("z" * (INLINE_LIMIT + 1))))
        store.append("r", ev)
    with TraceServer(ledger.EventStore(path, read_only=True), port=0) as s:
        _, body = call(s.url, "GetRun", {"runId": "r"})
    payload = body["events"][0]["runStarted"]["input"]
    assert "json" not in payload and payload["sha256"]


# Diagnoses


def test_list_diagnoses_returns_what_ledger_diagnose_saved(server, runs, demo_path):
    [failed] = [r for r, s in runs.items() if s.get("diagnoses")]
    status, body = call(server.url, "ListDiagnoses", {"runId": failed})
    assert status == 200
    with ledger.EventStore(demo_path, read_only=True) as store:
        [saved] = store.diagnoses(failed)
    [d] = body["diagnoses"]
    assert d["id"] == str(saved.id) and d["runId"] == failed
    assert d["createdAt"] == saved.created_at
    assert json.loads(d["json"]) == saved.data
    assert json.loads(d["json"])["schema"] == "ledger.diagnosis/1"
    other = next(r for r in runs if r != failed)
    assert call(server.url, "ListDiagnoses", {"runId": other}) == (200, {})


# Errors and protocol


@pytest.mark.parametrize("method, body, status, code", [
    ("GetRun", {"runId": "nope"}, 404, "not_found"),
    ("GetRun", {}, 404, "not_found"),
    ("ListDiagnoses", {"runId": "nope"}, 404, "not_found"),
    ("GetPayload", {"sha256": "0" * 64}, 404, "not_found"),
    ("GetPayload", {"sha256": "../../../../etc/passwd"}, 400, "invalid_argument"),
    ("GetPayload", {"sha256": "A" * 64}, 400, "invalid_argument"),
    ("GetRun", {"runId": 5, "unknownField": True}, 400, "invalid_argument"),
])
def test_errors_use_connect_codes_and_statuses(server, method, body, status, code):
    got_status, got = call(server.url, method, body)
    assert (got_status, got["code"]) == (status, code)
    assert got["message"]


def test_bad_requests(server, runs):
    run_id = next(iter(runs))
    assert call(server.url, "GetRun", raw=b"{oops")[1]["code"] == "invalid_argument"
    # Unknown fields are ignored, as connect-go does.
    assert call(server.url, "GetRun", {"runId": run_id, "later": 1})[0] == 200
    # An empty body is the empty message.
    assert call(server.url, "ListRuns", raw=b"")[0] == 200
    # JSON only.
    status, headers, _ = request(server.url, "POST", "/ledger.v1.TraceService/ListRuns",
                                 {"Content-Type": "application/proto"}, b"")
    assert status == 415 and headers["Accept-Post"] == "application/json"
    status, _, _ = request(server.url, "POST", "/ledger.v1.TraceService/ListRuns",
                           {"Content-Type": "application/json; charset=latin-1"}, b"{}")
    assert status == 415
    assert call(server.url, "ListRuns", {}, content_type="application/json; charset=utf-8")[0] == 200
    # A streaming method wants the streaming content type, and vice versa.
    stream = Stream(server.url, "WatchRun", {"runId": run_id}, content_type="application/json")
    assert stream.status == 415 and list(stream) == [] and stream.end is None
    status, _, _ = request(server.url, "POST", "/ledger.v1.TraceService/GetRun",
                           {"Content-Type": "application/connect+json"}, b"")
    assert status == 415
    # Unknown method, and GET (Connect's optional GET form isn't supported).
    status, body = call(server.url, "DeleteRun", {})
    assert status == 404 and body["code"] == "unimplemented"
    assert request(server.url, "GET", "/ledger.v1.TraceService/ListRuns")[0] == 405
    # Compression isn't supported.
    status, _, body = request(server.url, "POST", "/ledger.v1.TraceService/ListRuns",
                              {"Content-Type": "application/json", "Content-Encoding": "gzip"}, b"{}")
    assert status == 501 and json.loads(body)["code"] == "unimplemented"
    # A zero timeout has already expired.
    status, _, body = request(server.url, "POST", "/ledger.v1.TraceService/ListRuns",
                              {"Content-Type": "application/json", "Connect-Timeout-Ms": "0"}, b"{}")
    assert status == 504 and json.loads(body)["code"] == "deadline_exceeded"


def test_unary_responses_are_json_and_keep_the_connection_alive(server):
    parts = urlsplit(server.url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    try:
        for _ in range(3):  # three calls, one connection
            conn.request("POST", "/ledger.v1.TraceService/ListRuns", b"{}",
                         {"Content-Type": "application/json", "Connect-Protocol-Version": "1"})
            resp = conn.getresponse()
            assert resp.status == 200
            assert resp.getheader("Content-Type") == "application/json"
            assert "runs" in json.loads(resp.read())
    finally:
        conn.close()


def test_cors_allows_localhost_origins_only(server):
    pre = {"Origin": "http://localhost:5173", "Access-Control-Request-Method": "POST",
           "Access-Control-Request-Headers": "content-type,connect-protocol-version"}
    status, headers, _ = request(server.url, "OPTIONS", "/ledger.v1.TraceService/ListRuns", pre)
    assert status == 204
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:5173"
    assert "POST" in headers["Access-Control-Allow-Methods"]
    assert headers["Access-Control-Allow-Headers"] == "content-type,connect-protocol-version"
    for origin in ("http://127.0.0.1:4173", "http://app.localhost:3000", "http://[::1]:5173"):
        _, headers, _ = request(server.url, "POST", "/ledger.v1.TraceService/ListRuns",
                                {"Origin": origin, "Content-Type": "application/json"}, b"{}")
        assert headers["Access-Control-Allow-Origin"] == origin
    for origin in ("https://evil.example", "http://localhost.evil.example", "null"):
        _, headers, _ = request(server.url, "OPTIONS", "/ledger.v1.TraceService/ListRuns",
                                {**pre, "Origin": origin})
        assert "Access-Control-Allow-Origin" not in headers


# WatchRun


def test_watch_sends_a_finished_runs_backlog_and_ends(server, runs):
    for run_id in runs:
        _, body = call(server.url, "GetRun", {"runId": run_id})
        status, msgs, end = watch(server.url, run_id)
        assert status == 200 and end == {}
        assert [m["event"] for m in msgs] == body["events"]
        _, tail, end = watch(server.url, run_id, 3)
        assert [m["event"] for m in tail] == body["events"][3:] and end == {}
    _, msgs, end = watch(server.url, run_id, 10_000)
    assert msgs == [] and end == {}


def test_watch_errors_end_the_stream(server, runs):
    status, msgs, end = watch(server.url, "nope")
    assert status == 200 and msgs == [] and end["error"]["code"] == "not_found"
    _, _, end = watch(server.url, next(iter(runs)), -1)
    assert end["error"]["code"] == "invalid_argument"


def test_watch_streams_events_from_a_run_being_recorded(tmp_path):
    """A recording on another thread, held after its RunStarted until the
    watch has that event, so the rest can only arrive by streaming."""
    path = tmp_path / "live.db"
    ledger.EventStore(path).close()
    started, watching, done = threading.Event(), threading.Event(), threading.Event()
    run_ids: list[str] = []
    errors: list[BaseException] = []

    def record() -> None:
        try:
            with FakeLLMServer(latency_ms=(5, 20)) as llm, ledger.EventStore(path) as store:
                real = store.append

                def append(run_id, event):
                    out = real(run_id, event)
                    if event.index == 0:
                        run_ids.append(run_id)
                        started.set()
                        assert watching.wait(10)
                    return out

                store.append = append
                with ledger.record(store):
                    make_toy_graph(llm.base_url).invoke({"topic": "streaming"})
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)
        finally:
            started.set()
            done.set()

    with TraceServer(ledger.EventStore(path, read_only=True), port=0, poll_interval=0.05) as s:
        writer = threading.Thread(target=record)
        writer.start()
        assert started.wait(10) and run_ids, errors
        stream = Stream(s.url, "WatchRun", {"runId": run_ids[0]})
        events = []
        for msg in stream:
            events.append(msg["event"])
            if len(events) == 1:
                assert "runStarted" in events[0] and not done.is_set()
                watching.set()
        writer.join(10)
        assert not errors, errors
        assert stream.end == {}
        _, body = call(s.url, "GetRun", {"runId": run_ids[0]})
    assert len(events) > 5
    assert events == body["events"]
    assert [int(e.get("index", 0)) for e in events] == list(range(len(events)))
    assert "runFinished" in events[-1]
    assert body["summary"]["status"] == "RUN_STATUS_OK"


def test_an_unfinished_runs_watch_waits_until_the_server_stops(tmp_path, toy_graph):
    path = tmp_path / "crashed.db"
    with ledger.EventStore(path) as store:
        with crash_after(store, 3), ledger.record(store) as rec:
            toy_graph.invoke({"topic": "crash"})
    s = TraceServer(ledger.EventStore(path, read_only=True), port=0, poll_interval=0.05).start()
    _, listed = call(s.url, "ListRuns", {})
    assert listed["runs"][0]["status"] == "RUN_STATUS_UNFINISHED"
    assert "finishedAt" not in listed["runs"][0]
    stream = Stream(s.url, "WatchRun", {"runId": rec.run_id})
    got = []
    for msg in stream:
        got.append(msg)
        if len(got) == 3:
            threading.Timer(0.2, s.stop).start()
    assert len(got) == 3
    assert stream.end["error"]["code"] == "unavailable"


# Static files


def test_static_files_and_the_index_fallback(tmp_path, demo_path):
    static = tmp_path / "static"
    (static / "_app" / "immutable").mkdir(parents=True)
    (static / "index.html").write_text("<!doctype html><p>the app</p>")
    (static / "about.html").write_text("about")
    (static / "_app" / "immutable" / "app.js").write_text("console.log(1)")
    (tmp_path / "secret.txt").write_text("secret")
    with TraceServer(ledger.EventStore(demo_path, read_only=True), port=0, static_dir=static) as s:
        status, headers, body = request(s.url, "GET", "/")
        assert status == 200 and b"the app" in body and headers["Content-Type"].startswith("text/html")
        status, headers, body = request(s.url, "GET", "/_app/immutable/app.js")
        assert body == b"console.log(1)" and "immutable" in headers["Cache-Control"]
        assert "javascript" in headers["Content-Type"]
        assert request(s.url, "GET", "/about")[2] == b"about"
        assert b"the app" in request(s.url, "GET", "/runs/abc/diff?with=def")[2]
        for path in ("/../secret.txt", "/%2e%2e/secret.txt", "/_app/../../secret.txt"):
            assert b"secret" not in request(s.url, "GET", path)[2]
        # The API still answers next to the files.
        assert call(s.url, "ListRuns", {})[0] == 200


def test_without_a_built_ui_a_page_says_so(server):
    status, headers, body = request(server.url, "GET", "/")
    assert status == 200 and headers["Content-Type"].startswith("text/html")
    assert b"not built" in body and b"ledger.v1.TraceService" in body
    assert b"not built" in request(server.url, "GET", "/runs/x")[2]


# Read-only


def test_the_server_never_writes_to_the_store(tmp_path, toy_graph):
    path = tmp_path / "old.db"
    with ledger.EventStore(path) as store, ledger.record(store) as rec:
        toy_graph.invoke({"topic": "old"})
    # A store from before diagnoses were saved.
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE diagnoses")
    conn.close()
    with TraceServer(ledger.EventStore(path, read_only=True), port=0) as s:
        _, listed = call(s.url, "ListRuns", {})
        assert listed["runs"][0]["runId"] == rec.run_id
        assert call(s.url, "ListDiagnoses", {"runId": rec.run_id}) == (200, {})
        assert watch(s.url, rec.run_id)[2] == {}
    tables = {r[0] for r in sqlite3.connect(path).execute("SELECT name FROM sqlite_master")}
    assert "diagnoses" not in tables
    with pytest.raises(StoreNotFound):
        ledger.EventStore(tmp_path / "missing.db", read_only=True)
    assert not (tmp_path / "missing.db").exists()


# The command


def test_ledger_ui_refuses_a_missing_store(tmp_path, capsys):
    assert main(["--db", str(tmp_path / "nope.db"), "ui"]) == 2
    assert "no event store" in capsys.readouterr().err
    assert not (tmp_path / "nope.db").exists()


def test_ledger_ui_serves_the_store(demo_path, runs):
    proc = subprocess.Popen(
        [sys.executable, "-m", "ledger", "ui", "--db", str(demo_path), "--port", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        line = proc.stdout.readline()
        url = re.search(r"http://\S+", line).group(0)
        status, body = call(url, "ListRuns", {})
        assert status == 200 and [r["runId"] for r in body["runs"]] == list(runs)
    finally:
        proc.terminate()
        proc.wait(10)


# Conformance


def test_the_conformance_plan_runs_and_is_repeatable(server, runs):
    first = responses(server.url)
    labels = [label for label, _ in first]
    assert len(labels) == len(set(labels))
    for run_id in runs:
        assert f"GetRun {run_id}" in labels and f"WatchRun {run_id} from 0" in labels
    payloads = [v for label, v in first if re.fullmatch(r"GetPayload [0-9a-f]{64}", label)]
    assert len(payloads) >= 2 and all(v["status"] == 200 for v in payloads)
    assert diff(first, responses(server.url)) == []


def test_add_large_payload_run_appends_one_run(tmp_path):
    path = tmp_path / "x.db"
    run_id = add_large_payload_run(path)
    with ledger.EventStore(path) as store:
        assert store.run_ids() == [run_id]
