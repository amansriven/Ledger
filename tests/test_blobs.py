from __future__ import annotations

import hashlib
import sqlite3

import pytest

import ledger
from examples.toy_pipeline import recorded_runs
from ledger.core.blobs import BlobCorrupt, BlobMissing, BlobStore
from ledger.core.events import load_payload, make_payload, new_event, pb


def _tool_event(result: object, seq: int = 0) -> pb.Event:
    ev = new_event(pb.StepRef(node="n", superstep=1, path="p", seq=seq))
    ev.tool_call.name = "t"
    ev.tool_call.args.CopyFrom(make_payload({"args": [], "kwargs": {}}))
    ev.tool_call.result.CopyFrom(make_payload(result))
    return ev


def _blob_files(store: ledger.EventStore) -> list:
    root = store.blobs.root
    return sorted(p for p in root.rglob("*") if p.is_file()) if root.exists() else []


def _stored_rows(store: ledger.EventStore, run_id: str) -> list[pb.Event]:
    with sqlite3.connect(store.path) as c:
        rows = c.execute("SELECT data FROM events WHERE run_id = ? ORDER BY idx", (run_id,))
        out = []
        for (data,) in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            out.append(ev)
    return out


@pytest.fixture
def small_store(tmp_path):
    with ledger.EventStore(tmp_path / "l.db", blob_threshold=64) as s:
        yield s


def test_large_payloads_move_to_blobs_and_come_back(small_store):
    big = {"text": "x" * 500}
    ev = _tool_event(big)
    small_store.append("r", ev)

    # The caller's event keeps its bytes; the stored row does not.
    assert load_payload(ev.tool_call.result) == big
    (row,) = _stored_rows(small_store, "r")
    assert row.tool_call.result.json == b""
    assert row.tool_call.args.json != b""  # small payload stays inline
    assert row.tool_call.result.sha256 in small_store.blobs

    (back,) = small_store.events("r")
    assert load_payload(back.tool_call.result) == big
    (raw,) = small_store.events("r", hydrate=False)
    assert raw.tool_call.result.json == b""


def test_identical_payloads_are_stored_once(small_store):
    big = ["same"] * 100
    small_store.append("a", _tool_event(big))
    small_store.append("b", _tool_event(big))
    small_store.append("b", _tool_event(big, seq=1))
    assert len(_blob_files(small_store)) == 1
    assert small_store.run_storage("b").blob_count == 1


def test_corrupt_and_missing_blobs_are_reported(small_store):
    ev = _tool_event({"text": "y" * 500})
    small_store.append("r", ev)
    path = small_store.blobs.path_for(ev.tool_call.result.sha256)

    path.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(BlobCorrupt):
        list(small_store.events("r"))

    path.unlink()
    with pytest.raises(BlobMissing):
        list(small_store.events("r"))


def test_put_rejects_a_wrong_digest(tmp_path):
    blobs = BlobStore(tmp_path / "b")
    with pytest.raises(ValueError, match="does not match"):
        blobs.put(b"abc", hashlib.sha256(b"abd").hexdigest())
    assert blobs.put(b"abc") == hashlib.sha256(b"abc").hexdigest()


def test_memory_store_keeps_payloads_inline():
    with ledger.EventStore(":memory:", blob_threshold=1) as s:
        s.append("r", _tool_event({"text": "z" * 500}))
        (ev,) = s.events("r", hydrate=False)
        assert ev.tool_call.result.json != b""


def test_toy_runs_replay_from_blobs_and_share_them(fake_server, tmp_path, request):
    from tests.conftest import make_toy_graph

    graph = make_toy_graph(fake_server.base_url)
    with ledger.EventStore(tmp_path / "l.db", blob_threshold=64) as store:
        outputs = {}
        for topic in ("vector clocks", "fencing tokens"):
            with ledger.record(store) as rec:
                outputs[rec.run_id] = graph.invoke({"topic": topic})
        files_after_record = _blob_files(store)
        assert files_after_record

        fake_server.stop()
        request.getfixturevalue("network_off")
        for run_id in recorded_runs(store):
            with ledger.replay(run_id, store) as rep:
                out = graph.invoke(load_payload(rep.replay_index.input))
            assert out == outputs[run_id]
            assert rep.stats.live_calls == 0

            recorded, replayed = store.run_storage(run_id), store.run_storage(rep.run_id)
            assert recorded.blob_count > 0
            # The replay logs the same responses, so it adds no new blobs.
            assert replayed.blob_count == recorded.blob_count
        assert _blob_files(store) == files_after_record
