# Ledger

A Python runtime under LangGraph that makes agent runs replayable, crash-safe and self-diagnosing.

**Status: M0 (spike).** One graph records and replays end to end with zero live calls. Exactly-once effects, blame and the UI come in later milestones (see `Ledger Detailed Build Plan.md`).

## Integration

```python
import ledger
from openai import OpenAI

llm = ledger.wrap(OpenAI())                    # any OpenAI-compatible endpoint
graph = ledger.instrument(builder.compile())

@ledger.tool
def search(query: str) -> dict: ...

with ledger.record() as run:                  # calls go live and are logged
    graph.invoke({"topic": "fencing tokens"})

with ledger.replay(run.run_id) as session:    # calls are answered from the log
    graph.invoke({"topic": "fencing tokens"})
```

Calls made outside a `record`/`replay` block pass straight through. `graph.invoke` outside any block records a new run automatically.

## How replay finds a call

Each intercepted call is keyed by the LangGraph task that made it: parent namespace (task ids stripped), superstep, node, task path, and a per-task call counter. Replay looks calls up by that key, so parallel branches that finish in a different order still get their own responses. In M0 replay is strict: a changed request (by SHA-256 of its canonical JSON) raises `ReplayMismatch`, and recorded calls that were never made raise `ReplayDivergence`.

## Try it

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11 to 3.13.

```sh
uv sync
uv run pytest                                  # includes the M0 gate with sockets blocked

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
| `ledger/core/` | Canonical payloads, SQLite WAL event store, sessions and step keys |
| `ledger/intercept/` | OpenAI client wrapper, `@tool`, LangGraph `instrument` |
| `ledger/replay/` | Replay index and the `replay` context manager |
| `ledger/testing/fake_llm.py` | Nondeterministic fake LLM and search server |
| `examples/toy_pipeline.py` | 5-node pipeline with a 3-way parallel fan-out |
| `deploy/` | Dockerfile and Compose file |

Payloads are stored as canonical JSON inside protobuf events, because protobuf serialization is not canonical and would break content hashing.
