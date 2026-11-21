# Replay, fork and storage

How Ledger records a run, finds each call again on replay, handles edited code, forks runs, and stores the log.

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

A call that raised while recording raises again on replay, as the same exception class with the same args, so `except ValueError:` in a node behaves the same both times. Replay only looks the class up among modules that are already imported; it never imports anything because the log names it. A class whose constructor needs something else (say, `openai.APIConnectionError`, which wants `request=`) is created without running its `__init__`: `isinstance` and `str()` hold, but attributes set in `__init__` are missing. If its `__str__` then fails or says something else (Stripe's errors read attributes their `__init__` sets), the exception is an instance of a subclass that keeps the recorded text. If the class isn't loaded at all, replay raises `ledger.ReplayedError` instead. Every replayed exception has `__ledger_replayed__ = True`.

## Clock, ids and randomness

`ledger.time()`, `ledger.now()`, `ledger.uuid4()` and `ledger.rng()` read live values while recording and logged values while replaying; outside a session they behave like the stdlib. `ledger.rng()` logs one seed per task and returns a `random.Random`, so any number of draws costs one event. They are explicit calls, not patches of `time`, `uuid` and `random`, because LangGraph, httpx and the OpenAI client read the clock too, a varying number of times per run.

## Storage

Events go to SQLite in WAL mode (`$LEDGER_DB`, default `ledger.db`), one fsynced transaction per event. Payloads over 4 KB go to a content-addressed blob store next to it (`ledger.db.blobs/`), named by their SHA-256, written and fsynced before the event that refers to them. A recording and its replays share blobs, and reads check every blob against its digest. `EventStore.run_storage(run_id)` reports a run's bytes.

Payloads are stored as canonical JSON inside protobuf events, because protobuf serialization is not canonical and would break content hashing. The schema is `proto/events.proto` (`ledger.v1`); regenerate the Python code with `scripts/gen_proto.sh`.

## Property tests

`tests/test_properties.py` uses Hypothesis to generate layered graphs: random fan-outs, sync or async nodes, and in each node a random mix of tool calls, failing tool calls, and clock, uuid and rng reads, with random delays so branches finish in a different order each run. For every graph it checks three properties:

- a strict replay reproduces every call (same step, request and outcome) and the same output, with no tool body run;
- a fork at a random step runs live exactly the calls after it (later calls in its task, and later layers), with every other call identical to the recording, and the fork replays strictly in turn;
- a replay of a graph with one node's code edited goes live from that node's first changed request, and only downstream of it.

The expected live calls come from the generated graph's layers, not from Ledger's own downstream rule.
