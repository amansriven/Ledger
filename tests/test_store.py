from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time

import ledger
from ledger.core.events import new_event, pb


def _tool_event(i: int) -> pb.Event:
    ev = new_event(pb.StepRef(node="n", superstep=1, path="p", seq=i))
    ev.tool_call.name = "t"
    return ev


def test_append_assigns_contiguous_indexes(store):
    for i in range(5):
        store.append("run-a", _tool_event(i))
    store.append("run-b", _tool_event(0))
    assert [e.index for e in store.events("run-a")] == [0, 1, 2, 3, 4]
    assert [e.index for e in store.events("run-b")] == [0]


def test_run_ids_by_prefix_match_the_full_listing(store):
    ids = ["r", "r.replay-1", "r.replay-2", "r.replay-1.fork-1", "rx.replay-1", "q", "r.replay-"]
    for run_id in ids:
        for i in range(2):
            store.append(run_id, _tool_event(i))
    for prefix in ("r.replay-", "r", "rx", "s", ""):
        assert sorted(store.run_ids_with_prefix(prefix)) == sorted(
            r for r in store.run_ids() if r.startswith(prefix)
        )


def test_concurrent_appends_have_no_gaps(store):
    def worker(offset: int) -> None:
        for i in range(50):
            store.append("run", _tool_event(offset + i))

    threads = [threading.Thread(target=worker, args=(k * 100,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert [e.index for e in store.events("run")] == list(range(400))


def test_reopened_store_continues_indexes(tmp_path):
    path = tmp_path / "l.db"
    with ledger.EventStore(path) as s:
        s.append("r", _tool_event(0))
    with ledger.EventStore(path) as s:
        s.append("r", _tool_event(1))
        assert [e.index for e in s.events("r")] == [0, 1]


def test_sigkill_mid_write_leaves_only_whole_events(tmp_path):
    """Kill a writer with SIGKILL while it appends; every surviving event is whole."""
    path = tmp_path / "crash.db"
    script = textwrap.dedent(
        f"""
        import ledger
        from ledger.core.events import new_event, pb
        store = ledger.EventStore({str(path)!r})
        i = 0
        while True:
            ev = new_event(pb.StepRef(node="n", seq=i))
            ev.tool_call.name = "t"
            ev.tool_call.args.json = b"x" * 4096
            store.append("crash-run", ev)
            i += 1
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", script])
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            time.sleep(0.2)
            try:
                with sqlite3.connect(path) as c:
                    n = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            except sqlite3.OperationalError:
                continue
            if n >= 200:
                break
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()

    with ledger.EventStore(path) as s:
        events = list(s.events("crash-run"))
    assert len(events) >= 200
    assert [e.index for e in events] == list(range(len(events)))
    assert all(e.tool_call.args.json == b"x" * 4096 for e in events)
    assert [e.step.seq for e in events] == list(range(len(events)))
