# Ledger

A Python runtime under LangGraph that makes agent runs replayable, crash-safe and self-diagnosing.

**Status: M1 (deterministic replay), week 2.** Runs record and replay end to end with zero live calls, including clock reads, ids and randomness; large payloads live in a content-addressed blob store; requests are matched after masking timestamps and ids. Divergence detection, `ledger fork` and the CLI come next. Exactly-once effects, blame and the UI come in later milestones (see `Ledger Detailed Build Plan.md`).

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

Replay is strict for now: a changed request raises `ReplayMismatch`, and recorded calls that were never made raise `ReplayDivergence`.

## Clock, ids and randomness

`ledger.time()`, `ledger.now()`, `ledger.uuid4()` and `ledger.rng()` read live values while recording and logged values while replaying; outside a session they behave like the stdlib. `ledger.rng()` logs one seed per task and returns a `random.Random`, so any number of draws costs one event. They are explicit calls, not patches of `time`, `uuid` and `random`, because LangGraph, httpx and the OpenAI client read the clock too, a varying number of times per run.

## Storage

Events go to SQLite in WAL mode (`$LEDGER_DB`, default `ledger.db`), one fsynced transaction per event. Payloads over 4 KB go to a content-addressed blob store next to it (`ledger.db.blobs/`), named by their SHA-256, written and fsynced before the event that refers to them. A recording and its replays share blobs, and reads check every blob against its digest. `EventStore.run_storage(run_id)` reports a run's bytes.

## Try it

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11 to 3.13.

```sh
uv sync
uv run pytest                                  # includes the replay gate with sockets blocked

# Local: fake OpenAI-compatible server, record, stop server, replay
uv run python -m ledger.testing.fake_llm --port 8787 &
uv run python -m examples.toy_pipeline record --runs 5
kill %1
uv run python -m examples.toy_pipeline replay --all

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
| `ledger/replay/` | Replay index and the `replay` context manager |
| `ledger/testing/fake_llm.py` | Nondeterministic fake LLM and search server |
| `examples/toy_pipeline.py` | 5-node pipeline with a 3-way parallel fan-out |
| `deploy/` | Dockerfile and Compose file |

Payloads are stored as canonical JSON inside protobuf events, because protobuf serialization is not canonical and would break content hashing.
