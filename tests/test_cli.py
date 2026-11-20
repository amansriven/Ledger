"""The ledger command."""

from __future__ import annotations

import json

import pytest

import ledger
from ledger.cli import CLIError, load_graph, main
from ledger.core.events import body_kind
from ledger.intercept.langgraph import InstrumentedGraph

TOY = "examples.toy_pipeline:graph_from_env"
SIMPLE = "tests.cli_targets:make"


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "ledger.db"
    monkeypatch.setenv("LEDGER_DB", str(path))
    monkeypatch.delenv("LEDGER_GRAPH", raising=False)
    return path


@pytest.fixture
def toy_env(fake_server, monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", f"{fake_server.base_url}/v1")
    monkeypatch.setenv("TOOLS_URL", f"{fake_server.base_url}/tools")
    monkeypatch.setenv("LEDGER_MODEL", "fake-model")
    return fake_server


def run(capsys, *argv):
    code = main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def record_toy(capsys, n=1):
    code, out, _ = run(capsys, "record", "-g", TOY, "--input", '{"topic": "fencing tokens"}', "--runs", str(n))
    assert code == 0
    return [line.split()[1] for line in out.splitlines() if line.startswith("recorded")]


def test_record_logs_the_graph_and_replay_finds_it(db, toy_env, capsys, request):
    [run_id] = record_toy(capsys)
    with ledger.EventStore(db) as store:
        first = next(iter(store.events(run_id)))
    assert first.run_started.graph == TOY

    toy_env.stop()
    request.getfixturevalue("network_off")
    code, out, _ = run(capsys, "replay", run_id)  # no -g
    assert code == 0
    assert out.startswith(f"OK   {run_id} -> {run_id}.replay-1  live=0 replayed=8 output=match")


def test_replay_all(db, toy_env, capsys):
    record_toy(capsys, n=3)
    code, out, _ = run(capsys, "replay", "--all")
    assert code == 0
    assert out.splitlines()[-1] == "3/3 replayed identically"
    # Replays are not replayed by --all.
    code, out, _ = run(capsys, "replay", "--all")
    assert out.splitlines()[-1] == "3/3 replayed identically"


def test_changed_graph_fails_strictly_and_diverges_with_live(db, toy_env, capsys, monkeypatch):
    [run_id] = record_toy(capsys)
    monkeypatch.setenv("LEDGER_MODEL", "another-model")
    code, out, _ = run(capsys, "replay", run_id)
    assert code == 1
    assert out.startswith(f"FAIL {run_id}  ReplayMismatch: request at step /1/plan/")

    code, out, _ = run(capsys, "replay", "--live", run_id)
    assert code == 1
    lines = out.splitlines()
    assert lines[0].startswith(f"DIFF {run_id} -> ") and "live=8 replayed=0" in lines[0]
    assert lines[1].strip() == "changed_request at /1/plan/(__pregel_pull,plan)#1 (llm_call)"


def test_steps(db, toy_env, capsys):
    [run_id] = record_toy(capsys)
    code, out, _ = run(capsys, "steps", run_id)
    assert code == 0
    lines = out.splitlines()
    assert len(lines) == 11  # 8 calls and 3 nondet reads
    assert lines[0].startswith("#1    nondet    rng_seed")
    code, out, _ = run(capsys, "steps", run_id, "--node", "write")
    assert [line.split()[1] for line in out.splitlines()] == ["nondet", "nondet", "llm_call"]


def _index(capsys, run_id, node, kind):
    _, out, _ = run(capsys, "steps", run_id, "--node", node)
    return next(int(line.split()[0][1:]) for line in out.splitlines() if line.split()[1] == kind)


def test_fork_edit_rerun_and_replay_of_the_fork(db, toy_env, capsys, request):
    [run_id] = record_toy(capsys)
    web = _index(capsys, run_id, "research_web", "llm_call")
    search = _index(capsys, run_id, "research_code", "tool_call")

    code, out, _ = run(
        capsys, "fork", run_id, "--edit", f"{web}=EDITED NOTE",
        "--edit-json", f'{search}={{"query": "q", "results": ["edited fact"]}}',
        "--show-output",
    )
    assert code == 0, out
    first, rest = out.split("\n", 1)
    # code's LLM call follows its edited search in the same task, so it is live.
    assert first == f"forked {run_id} -> {run_id}.fork-1  live=2 replayed=4 edited=2"
    output = json.loads(rest[rest.index("{"):])
    assert "web: EDITED NOTE" in output["notes"]
    assert any("edited fact" in n for n in output["notes"] if n.startswith("code:"))

    code, out, _ = run(capsys, "fork", run_id, "--rerun", str(web))
    assert code == 0
    assert out.splitlines()[1].strip().startswith("rerun at /2/research_web/")

    toy_env.stop()
    request.getfixturevalue("network_off")
    code, out, _ = run(capsys, "replay", f"{run_id}.fork-1", f"{run_id}.fork-2")
    assert code == 0, out
    assert out.splitlines()[-1] == "2/2 replayed identically"


def test_a_failed_run_replays_identically_if_it_fails_the_same_way(db, capsys):
    code, out, _ = run(capsys, "record", "-g", SIMPLE, "--input", '{"x": -1}')
    assert code == 1
    run_id = out.split()[1]
    code, out, _ = run(capsys, "replay", run_id)
    assert code == 0, out
    assert "error=match (ValueError: x must not be negative)" in out


def test_record_without_a_graph(db, capsys, monkeypatch):
    code, _, err = run(capsys, "record", "--input", "{}")
    assert code == 2 and "record needs -g" in err
    monkeypatch.setenv("LEDGER_GRAPH", SIMPLE)
    code, out, _ = run(capsys, "record", "--input", '{"x": 1}')
    assert code == 0 and out.startswith("recorded ")


def test_replay_of_a_python_recorded_run_needs_a_graph(db, capsys, monkeypatch):
    with ledger.EventStore(db) as store, ledger.record(store) as rec:
        load_graph(SIMPLE).invoke({"x": 1})
    code, _, err = run(capsys, "replay", rec.run_id)
    assert code == 2 and "pass -g module:attr" in err
    code, out, _ = run(capsys, "replay", "-g", SIMPLE, rec.run_id)
    assert code == 0 and out.startswith("OK")
    monkeypatch.setenv("LEDGER_GRAPH", SIMPLE)
    code, out, _ = run(capsys, "replay", rec.run_id)
    assert code == 0


def test_recorded_graph_beats_the_environment(db, capsys, monkeypatch):
    code, out, _ = run(capsys, "record", "-g", SIMPLE, "--input", '{"x": 1}')
    run_id = out.split()[1]
    monkeypatch.setenv("LEDGER_GRAPH", "tests.cli_targets:not_a_graph")
    code, out, _ = run(capsys, "replay", run_id)
    assert code == 0


def test_input_file_and_stdin(db, capsys, tmp_path, monkeypatch):
    f = tmp_path / "in.json"
    f.write_text('{"x": 5}')
    code, out, _ = run(capsys, "record", "-g", SIMPLE, "--input-file", str(f), "--show-output")
    assert code == 0 and json.loads(out.split("\n", 1)[1]) == {"x": 6}
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO('{"x": 7}'))
    code, out, _ = run(capsys, "record", "-g", SIMPLE, "--input-file", "-", "--show-output")
    assert json.loads(out.split("\n", 1)[1]) == {"x": 8}
    code, _, err = run(capsys, "record", "-g", SIMPLE, "--input", "{nope")
    assert code == 2 and "not JSON" in err


def test_db_before_or_after_the_command(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("LEDGER_DB", raising=False)
    monkeypatch.chdir(tmp_path)
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    run(capsys, "--db", str(a), "record", "-g", SIMPLE, "--input", '{"x": 1}')
    run(capsys, "record", "--db", str(b), "-g", SIMPLE, "--input", '{"x": 1}')
    _, out_a, _ = run(capsys, "--db", str(a), "runs")
    _, out_b, _ = run(capsys, "runs", "--db", str(b))
    assert len(out_a.splitlines()) == len(out_b.splitlines()) == 1
    assert not (tmp_path / "ledger.db").exists()


def test_runs(db, capsys):
    run(capsys, "record", "-g", SIMPLE, "--input", '{"x": 1}')
    _, out, _ = run(capsys, "record", "-g", SIMPLE, "--input", '{"x": -1}')
    failed = out.split()[1]
    _, out, _ = run(capsys, "runs")
    statuses = [line.split()[2] for line in out.splitlines()]
    assert statuses == ["ok", "error"]
    run(capsys, "replay", failed)
    _, out, _ = run(capsys, "runs", "--mode", "replay")
    assert out.split()[:4] == [f"{failed}.replay-1", "replay", "of", failed]


def test_fork_errors(db, capsys):
    _, out, _ = run(capsys, "record", "-g", SIMPLE, "--input", '{"x": 1}')
    run_id = out.split()[1]
    for argv, message in [
        ([], "at least one"),
        (["--edit", "0=x"], "not a call"),
        (["--edit", "nokey"], "STEP=VALUE"),
        (["--edit-json", "1={bad"], "--edit-json 1"),
        (["--edit", "1=a", "--rerun", "1"], "given twice"),
        (["--rerun", "/9/x/()#0"], "no call at step"),
    ]:
        code, _, err = run(capsys, "fork", run_id, *argv)
        assert code == 2 and message in err, (argv, err)


def test_load_graph(capsys):
    assert isinstance(load_graph("tests.cli_targets:compiled"), InstrumentedGraph)
    g = load_graph("tests.cli_targets:instrumented")
    assert g is __import__("tests.cli_targets").cli_targets.instrumented
    assert isinstance(load_graph(SIMPLE), InstrumentedGraph)
    for ref, message in [
        ("tests.cli_targets", "module:attr"),
        ("tests.nope:x", "cannot import"),
        ("tests.cli_targets:missing", "no attribute"),
        ("tests.cli_targets:not_a_graph", "not a graph"),
    ]:
        with pytest.raises(CLIError, match=message):
            load_graph(ref)


def test_fork_run_is_marked_in_the_log(db, capsys):
    _, out, _ = run(capsys, "record", "-g", SIMPLE, "--input", '{"x": 1}')
    run_id = out.split()[1]
    code, out, _ = run(capsys, "fork", run_id, "--edit-json", "1=41", "--show-output")
    assert code == 0 and json.loads(out[out.index("{"):]) == {"x": 41}
    with ledger.EventStore(db) as store:
        kinds = [body_kind(ev) for ev in store.events(f"{run_id}.fork-1")]
    assert kinds == ["run_started", "diverged", "tool_call", "run_finished"]
