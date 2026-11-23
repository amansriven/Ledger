"""scripts/make_demo_db.py builds the store the trace servers and UI test against."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import ledger
from ledger.cli import list_runs

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "make_demo_db.py"


def load_script():
    spec = importlib.util.spec_from_file_location("make_demo_db", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_demo_store_has_one_of_each_kind_of_run(tmp_path):
    path = tmp_path / "demo.db"
    load_script().build(path, latency_ms=(0, 5))
    with ledger.EventStore(path) as store:
        runs = {r.run_id: r for r in list_runs(store)}
        modes = sorted(r.mode for r in runs.values())
        assert modes.count("record") == 6 and modes.count("replay") == 1
        assert any(path.with_name("demo.db.blobs").rglob("*"))
        assert all(r.status == "ok" for r in runs.values())
        [replay] = [r for r in runs.values() if r.mode == "replay"]
        assert replay.live == 0
        [resumed] = [r for r in runs.values() if r.resumed]
        assert resumed.graph.endswith("refund_agent:graph_from_env")
        [failed] = [r for r in runs.values() if store.diagnoses(r.run_id)]
        [saved] = store.diagnoses(failed.run_id)
        assert saved.data["status"] == "found" and saved.data["step"]["node"] == "research_web"
        forks = [c for cf in saved.data["counterfactuals"] for c in cf["run_ids"]]
        assert len(forks) == 7 and all(f in runs for f in forks)
    # Building again starts from scratch.
    load_script().build(path, latency_ms=(0, 5))
    with ledger.EventStore(path) as store:
        assert not set(store.run_ids()) & set(runs)
