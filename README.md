# Ledger

**Replayable, crash-safe, self-diagnosing agent runs for LangGraph.**

[![PyPI](https://img.shields.io/pypi/v/ledger-runtime)](https://pypi.org/project/ledger-runtime/)
[![Python](https://img.shields.io/pypi/pyversions/ledger-runtime)](https://pypi.org/project/ledger-runtime/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Ledger sits under a LangGraph graph and logs every LLM call, tool call, clock read and random draw. From that log it can replay a run exactly, resume a crashed run without repeating its side effects, and name the step that made a run fail.

| Guarantee | What it means | Measured |
| --- | --- | --- |
| **Deterministic replay** | A recorded run re-executes with zero live calls, and can be forked from any step with edits | 200 / 200 runs replay identically, network off |
| **Effectively-once effects** | A crash in the middle of a refund never refunds twice; the run resumes where it stopped | 0 duplicate refunds in 10,000 crash-injected runs, vs 182 for stock LangGraph |
| **Automatic blame** | Names the step that broke a failed run by rerunning counterfactual forks of it | Right step in 99.4% of 163 failed runs on a real model, vs 68.7% for an LLM judge |

Recording adds 0.20 ms per step at p50. How each number was measured: [benchmarks](docs/benchmarks.md).

## Install

```sh
pip install ledger-runtime                  # you import ledger
pip install 'ledger-runtime[examples]'      # adds Stripe, for the example refund agent
```

Python 3.11 to 3.13.

## Quickstart

```python
import ledger
from openai import OpenAI

llm = ledger.wrap(OpenAI())                    # any OpenAI-compatible endpoint
graph = ledger.instrument(builder.compile())

@ledger.tool
def search(query: str) -> dict: ...

@ledger.effect                                 # side effects: once per run step
def refund(charge_id: str, amount: int, *, idempotency_key: str) -> dict: ...

with ledger.record() as run:                   # calls go live and are logged
    graph.invoke({"topic": "fencing tokens"})

with ledger.replay(run.run_id):                # calls are answered from the log
    graph.invoke({"topic": "fencing tokens"})
```

Inside nodes, read the clock, ids and randomness with `ledger.now()`, `ledger.uuid4()` and `ledger.rng()` so replays see the recorded values. Then, from Python or the shell:

```python
with ledger.fork(run_id, at=12, edit="a different summary"):   # replay with one call changed
    graph.invoke(inputs)

with ledger.resume(run_id):                                    # finish a crashed run, same keys
    graph.invoke(inputs)

d = ledger.diagnose(run_id, graph, ok)                         # d.step: the call that broke it
```

```sh
ledger runs                         # every run: mode, status, live calls
ledger replay --all                 # exit 1 unless every replay is identical
ledger fork RUN_ID --rerun 11       # rerun one call live, replay the rest
ledger resume --all                 # finish recordings that crashed or failed
ledger diagnose RUN_ID --check app:passed
ledger ui --open                    # the trace UI: live timeline, run diff, blame
```

## Try it

The refund agent against the bundled fake Stripe and LLM servers, then replayed from its log:

```sh
python -m ledger.testing.fake_stripe --port 12111 &
python -m ledger.testing.fake_llm --port 8787 &
python -m ledger.examples.refund_agent      # seeds a charge, records one refund
ledger replay --all                         # live=0, output=match
ledger ui --open                            # see the run, and the replay against it
```

## How it works

- **One interceptor.** Every call is logged and keyed by the LangGraph task that made it, so parallel branches replay correctly whatever order they finish in. Requests match after masking timestamps and ids.
- **Divergence and forks.** When edited code makes a call that no longer matches, replay can go live from that call and only downstream of it. A fork does the same on purpose, from the calls you choose.
- **Effects.** `@ledger.effect` writes a `PENDING` record before the call and `CONFIRMED` after, with an idempotency key derived from the run and step. Retries and resumes reuse the key, and effects with a `compensate` function are undone if the run fails for good.
- **Blame.** `ledger.diagnose` bisects over forked replays: everything before the rerun point replays free from the log, so a step d calls from the end takes about 2·log2(d) forks.
- **Trace UI.** `ledger ui` (in the package) and `ledger-server` (a Go binary) serve the same API and UI: runs as live timelines, a call-by-call diff of a replay or fork against its recording, and a diagnosis drawn as the bisection it ran.
- **Storage.** SQLite in WAL mode, one fsynced transaction per event, with payloads over 4 KB in a content-addressed blob store.

| Page | Covers |
| --- | --- |
| [Replay, fork and storage](docs/replay.md) | Matching rules, divergence, forks, replayed errors, property tests |
| [Effects, sagas and resume](docs/effects.md) | The write-ahead protocol, compensation, resume, the refund agent |
| [Blame](docs/blame.md) | How `diagnose` searches, noise and budgets, fault injection, prior work |
| [Benchmarks](docs/benchmarks.md) | Replay fidelity, chaos, overhead, blame and Who&When, with baselines and caveats |
| [Trace UI](docs/ui.md) | The three views, both servers, the Docker image, working on the UI |
| [CLI](docs/cli.md) | Every `ledger` command and flag |

## Status

| Milestone | State |
| --- | --- |
| M0 · Spike | Done |
| M1 · Deterministic replay | Done: 200 / 200 identical replays |
| M2 · Effectively-once effects | Done: 0 duplicates in 10,000 chaos runs |
| M3 · Counterfactual blame | Gate passed on a real model |
| M4 · UI and launch | In progress: `ledger diagnose`, PyPI 0.1.0 and the trace UI are done; the demo, hosted UI and writeups are next |

## Development

```sh
uv sync
uv run pytest                                                        # includes the replay gate, sockets blocked
make test                                                            # Python, Go and UI tests (the UI needs Node 22.17+)
make ui && make server                                               # build the UI into both servers; bin/ledger-server
HYPOTHESIS_PROFILE=thorough uv run pytest tests/test_properties.py   # 1,000 random graphs per property
docker compose -f deploy/docker-compose.yml run --rm record          # record runs in Docker
docker compose -f deploy/docker-compose.yml run --rm replay          # replay them with network_mode: none
```

| Path | What |
| --- | --- |
| `ledger/core/` | Event store, blob store, request matching, clock and rng shims |
| `ledger/intercept/` | OpenAI client wrapper, `@tool`, LangGraph `instrument` |
| `ledger/replay/` | Replay, divergence, `fork`, `steps`, `resume` |
| `ledger/effects/` | `@ledger.effect`: idempotency keys, write-ahead records, sagas |
| `ledger/blame/` | `diagnose`: counterfactual blame by bisection |
| `ledger/examples/` | Toy pipeline, refund agent, order pipeline |
| `ledger/testing/` | Fake LLM and Stripe servers, fault injection |
| `bench/`, `chaos/` | Benchmarks and the chaos harness; results in `bench/results/` and `chaos/results/` |
| `ledger/ui/` | `ledger ui`: the trace API over Connect with JSON, and the built UI |
| `server/` | `ledger-server`: the Go trace server, with the UI embedded |
| `ui/` | The trace UI (SvelteKit): runs, timeline, diff and blame views |
| `proto/events.proto` | Event schema (`ledger.v1`); regenerate with `scripts/gen_proto.sh` |
| `proto/trace_service.proto` | The trace API both servers answer |
| `scripts/make_demo_db.py` | A demo store with one of each kind of run |
| `deploy/`, `Makefile` | Dockerfiles and Compose file; `make ui`, `server`, `demo`, `dist`, `test`, `image` |

## License

MIT. See [LICENSE](LICENSE).
