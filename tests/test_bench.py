"""The replay-fidelity benchmark, on a few runs."""

from __future__ import annotations

import json

import pytest

from bench import replay_fidelity as bench


def test_small_run_passes_and_beats_the_baseline(tmp_path):
    out = tmp_path / "report.json"
    assert bench.main(["run", "--runs", "4", "--out", str(out)]) == 0

    rep = json.loads(out.read_text())
    s = rep["summary"]
    assert rep["passed"]
    assert s["ledger"]["runs"] == s["ledger"]["identical"] == 4
    assert s["ledger"]["storage_kb"]["p50"] > 0
    # Stock LangGraph re-executes the nodes: offline it can't, online it gets new answers.
    assert s["langgraph_network_off"]["identical"] == 0
    assert set(s["langgraph_network_off"]["failures"]) == {"APIConnectionError: Connection error."}
    assert s["langgraph_network_on"]["identical"] == 0
    for r in rep["runs"]["ledger"]:
        assert r["identical"] and r["calls"] == 11  # 5 LLM, 3 tool, 3 nondet


def test_two_phases_and_refusing_to_overwrite(tmp_path):
    data = tmp_path / "data"
    with bench.fake_server_env() as server:
        assert bench.main(["record", "--data", str(data), "--runs", "2"]) == 0
        with pytest.raises(SystemExit, match="--fresh"):
            bench.main(["record", "--data", str(data), "--runs", "2"])
        address = server.base_url.removeprefix("http://")
        with pytest.raises(SystemExit, match="is reachable"):
            bench.main(["replay", "--data", str(data), "--require-offline", address])
    # Server stopped: the address no longer answers.
    out = tmp_path / "report.json"
    with bench.network_off():
        assert bench.main(["replay", "--data", str(data), "--out", str(out)]) == 0
    assert json.loads(out.read_text())["summary"]["ledger"]["identical"] == 2


def test_a_tampered_recording_is_not_identical(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    with bench.fake_server_env():
        bench.record(data, runs=1, seed=0)
    import ledger
    from examples import toy_pipeline as toy

    with ledger.EventStore(data / bench.LEDGER_DB) as store, bench.network_off():
        [run_id] = toy.recorded_runs(store)
        # A graph whose plan prompt changed: replay must not call it identical.
        graph = toy.graph_from_env()
        original = toy.TONES
        toy.TONES = ("terse", "terse", "terse")
        try:
            result = bench.replay_ledger(toy.graph_from_env(), store, run_id)
        finally:
            toy.TONES = original
        assert not result.identical and "ReplayMismatch" in result.reason
        assert bench.replay_ledger(graph, store, run_id).identical


def test_the_overhead_benchmark_measures_every_mode(tmp_path):
    from bench import overhead

    out = tmp_path / "overhead.json"
    assert overhead.main(["--runs", "6", "--warmup", "1", "--data", str(tmp_path / "data"),
                          "--out", str(out)]) == 0

    s = json.loads(out.read_text())["summary"]
    assert s["replays_not_identical"] == 0
    assert s["calls_per_run"] == 8  # 5 LLM, 3 tool
    assert s["events_per_run"]["p50"] == 13  # plus 3 nondet reads, start and finish
    assert s["log_kb"]["p50"] > 0
    for mode in overhead.MODES:
        assert 0 < s["run_ms"][mode]["p50"] <= s["run_ms"][mode]["p95"]
        assert s["per_step_ms"][mode]["p50"] == pytest.approx(s["run_ms"][mode]["p50"] / 5, abs=0.01)
    for mode in ("record", "replay"):
        added = s["added"][mode]["p50"]
        assert added["run_ms"] == pytest.approx(
            s["run_ms"][mode]["p50"] - s["run_ms"]["off"]["p50"], abs=0.01)
        assert added["per_call_ms"] == pytest.approx(added["run_ms"] / 8, abs=0.01)
