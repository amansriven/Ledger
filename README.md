# Ledger

A Python runtime under LangGraph that makes agent runs replayable, crash-safe and self-diagnosing.

**Status: M1 (deterministic replay), week 3.** Runs record and replay end to end with zero live calls, including clock reads, ids and randomness; large payloads live in a content-addressed blob store; requests are matched after masking timestamps and ids. A replay of edited code can go live from the first call that no longer matches, and only downstream of it. `ledger.fork` replays a run with chosen calls edited or rerun live, and the `ledger` command records, replays and forks runs from the shell. Hypothesis property tests check replay, fork and divergence on randomly generated graphs. The 200-run replay benchmark comes next. Exactly-once effects, blame and the UI come in later milestones (see `Ledger Detailed Build Plan.md`).

## Integration

```python
import ledger
from openai import OpenAI

llm = ledger.wrap(OpenAI())                    # any OpenAI-compatible endpoint
graph = ledger.instrument(builder.compile())

@ledger.tool
def search(query: str) -> dict: ...

def plan(state):                              # inside a node
    tone = ledger.rng().choice(["brief", "detailed"])
    stamp = ledger.now()                      # not datetime.now()
    report_id = ledger.uuid4()                # not uuid.uuid4()

with ledger.record() as run:                  # calls go live and are logged
    graph.invoke({"topic": "fencing tokens"})

with ledger.replay(run.run_id) as session:    # calls are answered from the log
    graph.invoke({"topic": "fencing tokens"})
```

Calls made outside a `record`/`replay` block pass straight through. `graph.invoke` outside any block records a new run automatically.

## How replay finds a call

Each intercepted call is keyed by the LangGraph task that made it: parent namespace (task ids stripped), superstep, node, task path, and a per-task call counter. Replay looks calls up by that key, so parallel branches that finish in a different order still get their own responses.

At that key, the request must be the same one that was recorded. "The same" means equal after normalization: ISO 8601 date-times become `<timestamp>`, UUIDs (and 32-hex UUID4s, like Ledger run ids) become `<uuid>`, and the `timeout` and `extra_headers` arguments of LLM calls are ignored. Everything else (model, messages, temperature, tools, tool arguments) must match exactly. The raw request is still logged in full. Pass `normalizer=ledger.EXACT` to `ledger.wrap` or `ledger.tool` for byte-for-byte matching, or `Normalizer.extend(...)` to add rules.

## When replay stops matching

By default replay is strict: a changed request raises `ReplayMismatch`, and recorded calls that were never made raise `ReplayDivergence`.

With `on_divergence="live"`, the first call that doesn't match its recording (changed request, a call the recording doesn't have at that step, or a different kind of call there) runs live, along with everything downstream of it. Everything else keeps coming from the log:

```python
with ledger.replay(run_id, on_divergence="live") as session:
    edited_graph.invoke(inputs)
session.divergences   # [DivergencePoint(step=".../2/research_web/...#1", reason=CHANGED_REQUEST, ...)]
```

"Downstream" follows LangGraph's superstep model. It covers later calls of the diverged task and every call in a later superstep. Parallel tasks in the same superstep are not downstream: they run on the state from before the superstep, so they keep replaying, whatever order the branches finish in. A mismatch in one of them is a divergence point of its own. Recorded calls that were never made, with no divergence to explain them, are reported as `SKIPPED_CALL` instead of raising.

The replay's log shows what happened. Each divergence point gets a `Diverged` event at its step, just before the live call, and every call event carries `from_log`. The log is a complete recording of the edited run, so it can be replayed strictly in turn.

## Fork

A fork replays a run with chosen calls changed: each one gets an edited outcome, or runs live again. Everything downstream of them runs live, and the rest replays from the log, so a counterfactual pays only for what it changes.

```python
for s in ledger.steps(run_id):                # every call, in log order
    print(s)  # #12  llm_call  fake-model  /2/research_web/(__pregel_pull,research_web)#1  Summary of ...

with ledger.fork(run_id, at=12, edit="a different summary") as session:
    graph.invoke(inputs)

ledger.fork(run_id, at=step)                                  # rerun live (edit=ledger.RERUN)
ledger.fork(run_id, at=step, edit=lambda old: old.upper())    # derive from the recorded outcome
ledger.fork(run_id, edits={step_a: "x", step_b: ledger.RERUN})
```

`at` takes a step key, the call's index in the run's log, a `StepRef` or a `StepInfo`. An edit is the call's new outcome. For an LLM call, a string replaces the first choice's text and keeps the rest of the recorded completion. Upstream of the fork steps the run must still match its recording: a mismatch raises, and so does a fork step the run never reaches. Pass `on_divergence="live"` to allow code changes as well.

The fork is logged as its own run (`<source>.fork-<n>`, mode `MODE_FORK`), with a `Diverged` event of reason `EDITED` or `RERUN` at each fork step. Its log holds the edited outcomes, so it replays strictly like any recording.

One limitation, for replays and forks alike: step keys don't carry the parent graph's superstep, so calls in a different namespace from a divergence or fork step (inside or around a subgraph) can't be placed relative to it. Every such call made after the divergence goes live, which is safe but can run more live calls than needed in graphs with subgraphs.

## Errors

A call that raised while recording raises again on replay, as the same exception class with the same args, so `except ValueError:` in a node behaves the same both times. Replay only looks the class up among modules that are already imported; it never imports anything because the log names it. A class whose constructor needs something else (say, `openai.APIConnectionError`, which wants `request=`) is created without running its `__init__`: `isinstance` and `str()` hold, but attributes set in `__init__` are missing. If the class isn't loaded at all, replay raises `ledger.ReplayedError` instead. Every replayed exception has `__ledger_replayed__ = True`.

## Property tests

`tests/test_properties.py` uses Hypothesis to generate layered graphs: random fan-outs, sync or async nodes, and in each node a random mix of tool calls, failing tool calls, and clock, uuid and rng reads, with random delays so branches finish in a different order each run. For every graph it checks three properties:

- a strict replay reproduces every call (same step, request and outcome) and the same output, with no tool body run;
- a fork at a random step runs live exactly the calls after it (later calls in its task, and later layers), with every other call identical to the recording, and the fork replays strictly in turn;
- a replay of a graph with one node's code edited goes live from that node's first changed request, and only downstream of it.

The expected live calls come from the generated graph's layers, not from Ledger's own downstream rule.

## Clock, ids and randomness

`ledger.time()`, `ledger.now()`, `ledger.uuid4()` and `ledger.rng()` read live values while recording and logged values while replaying; outside a session they behave like the stdlib. `ledger.rng()` logs one seed per task and returns a `random.Random`, so any number of draws costs one event. They are explicit calls, not patches of `time`, `uuid` and `random`, because LangGraph, httpx and the OpenAI client read the clock too, a varying number of times per run.

## Storage

Events go to SQLite in WAL mode (`$LEDGER_DB`, default `ledger.db`), one fsynced transaction per event. Payloads over 4 KB go to a content-addressed blob store next to it (`ledger.db.blobs/`), named by their SHA-256, written and fsynced before the event that refers to them. A recording and its replays share blobs, and reads check every blob against its digest. `EventStore.run_storage(run_id)` reports a run's bytes.

## CLI

```sh
ledger record -g examples.toy_pipeline:graph_from_env --input '{"topic": "fencing tokens"}'
ledger runs                                    # every run: mode, source, status, live calls
ledger replay RUN_ID                           # strict; exit 1 unless identical
ledger replay --all --live                     # report divergences instead of failing
ledger steps RUN_ID [--node write]             # the calls a fork can target, by index
ledger fork RUN_ID --edit 4='a different summary' --edit-json 3='{"results": []}' --rerun 11
```

`-g` names the graph as `module:attr`: a compiled graph, or a function with no arguments that returns one. `record` logs it with the run, so `replay` and `fork` find the graph again. They fall back to `$LEDGER_GRAPH` for runs recorded from Python, and an explicit `-g` overrides both. Replays and forks take their input from the recording. A run that failed when recorded replays identically if it fails with the same error. `--db` picks the event store (else `$LEDGER_DB`, else `ledger.db`). `python -m ledger` is the same command.

## Try it

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11 to 3.13.

```sh
uv sync
uv run pytest                                  # includes the replay gate with sockets blocked
HYPOTHESIS_PROFILE=thorough uv run pytest tests/test_properties.py   # 1000 random graphs per property

# Local: fake OpenAI-compatible server, record, stop server, replay
uv run python -m ledger.testing.fake_llm --port 8787 &
uv run ledger record -g examples.toy_pipeline:graph_from_env --input '{"topic": "fencing tokens"}' --runs 5
kill %1
uv run ledger replay --all

# Docker: replay runs with network_mode: none
docker compose -f deploy/docker-compose.yml run --rm record
docker compose -f deploy/docker-compose.yml run --rm replay
```

## Layout

| Path | What |
| --- | --- |
| `proto/events.proto` | Event schema (`ledger.v1`). Regenerate with `scripts/gen_proto.sh` |
| `ledger/core/` | Canonical payloads, SQLite WAL event store, blob store, request normalization, clock/uuid/rng shims, sessions and step keys |
| `ledger/intercept/` | OpenAI client wrapper, `@tool`, LangGraph `instrument` |
| `ledger/replay/` | Replay index, divergence detection, `replay`, and `fork` with `steps` |
| `ledger/cli.py` | The `ledger` command: `record`, `replay`, `fork`, `steps`, `runs` |
| `ledger/testing/fake_llm.py` | Nondeterministic fake LLM and search server |
| `examples/toy_pipeline.py` | 5-node pipeline with a 3-way parallel fan-out |
| `deploy/` | Dockerfile and Compose file |

Payloads are stored as canonical JSON inside protobuf events, because protobuf serialization is not canonical and would break content hashing.
