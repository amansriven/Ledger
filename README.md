# Ledger

A Python runtime under LangGraph that makes agent runs replayable, crash-safe and self-diagnosing.

**Status: M1 (deterministic replay) done; M2 (exactly-once effects) done.** Runs record and replay end to end with zero live calls, including clock reads, ids and randomness; large payloads live in a content-addressed blob store; requests are matched after masking timestamps and ids. A replay of edited code can go live from the first call that no longer matches, and only downstream of it. `ledger.fork` replays a run with chosen calls edited or rerun live, and the `ledger` command records, replays and forks runs from the shell. Hypothesis property tests check replay, fork and divergence on randomly generated graphs, and the replay-fidelity benchmark passes the M1 gate: 200 of 200 recorded runs replay identically with the network off. `@ledger.effect` makes side-effecting calls such as Stripe refunds happen once per run step: idempotency keys from the run and step, a write-ahead pending/confirmed record, and retries with the same key; an example refund agent uses it. `ledger.resume` picks a crashed run up in the same run and log, settling any refund it left pending, and effects with a `compensate` function are undone, newest first, when a run fails for good. A chaos harness kills the worker and cuts its network while it works through refund tickets, and the M2 gate passes: in 10,000 crash-injected runs Ledger made no duplicate refund, where stock LangGraph with a checkpointer and node retries made 182. Recording adds 0.20 ms per step at p50 (0.41 ms at p95) on the 5-node pipeline. M3 (blame) has started: `ledger.diagnose` finds the step that broke a failed run by bisecting over forked replays, and `ledger.testing.faults` makes failed runs with a known guilty step. `bench/blame_eval.py` scores it against a naive rerun scan and random pick on injected faults; `bench/who_and_when.py` loads the Who&When logs and scores log-only methods on them. The LLM-judge baseline needs a model and comes next. The UI comes later (see `Ledger Detailed Build Plan.md`).

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

A call that raised while recording raises again on replay, as the same exception class with the same args, so `except ValueError:` in a node behaves the same both times. Replay only looks the class up among modules that are already imported; it never imports anything because the log names it. A class whose constructor needs something else (say, `openai.APIConnectionError`, which wants `request=`) is created without running its `__init__`: `isinstance` and `str()` hold, but attributes set in `__init__` are missing. If its `__str__` then fails or says something else (Stripe's errors read attributes their `__init__` sets), the exception is an instance of a subclass that keeps the recorded text. If the class isn't loaded at all, replay raises `ledger.ReplayedError` instead. Every replayed exception has `__ledger_replayed__ = True`.

## Property tests

`tests/test_properties.py` uses Hypothesis to generate layered graphs: random fan-outs, sync or async nodes, and in each node a random mix of tool calls, failing tool calls, and clock, uuid and rng reads, with random delays so branches finish in a different order each run. For every graph it checks three properties:

- a strict replay reproduces every call (same step, request and outcome) and the same output, with no tool body run;
- a fork at a random step runs live exactly the calls after it (later calls in its task, and later layers), with every other call identical to the recording, and the fork replays strictly in turn;
- a replay of a graph with one node's code edited goes live from that node's first changed request, and only downstream of it.

The expected live calls come from the generated graph's layers, not from Ledger's own downstream rule.

## Replay-fidelity benchmark

`bench/replay_fidelity.py` records 200 runs of the 5-node pipeline against the nondeterministic fake server, then replays every one in a container with `network_mode: none`. A replay counts as identical only if it finishes with the same output, makes no live call (LLM, tool, clock, uuid or rng), finds no divergence, and logs the same calls as the recording, step for step: same request byte for byte, same outcome, each served from the log.

The baseline is LangGraph's own checkpoint replay: the same pipeline compiled with a `SqliteSaver`, replayed from its first checkpoint with `graph.invoke(None, checkpoint_config)`. LangGraph re-executes every node after that checkpoint, so it calls the LLM and the tool again.

| 200 recorded runs | Identical | Storage per run (KB, p50) | Replay time (ms, p50) |
| --- | --- | --- | --- |
| Ledger, network off | **200 / 200** | 9.3 | 4.1 |
| LangGraph checkpoint replay, network off | 0 / 200 (connection error at the first call) | 6.2 | 1.2 |
| LangGraph checkpoint replay, network on | 0 / 200 (new answers, different output) | 6.2 | 95.5 |

Measured in Docker on an Apple-silicon Mac; full per-run results are in `bench/results/replay_fidelity.json`. Ledger stores about 3 KB more per run than LangGraph's checkpoints because it keeps every request and response, not just the state between steps. Reading back the final checkpoint would reproduce the output, but that runs nothing, so it isn't counted as a replay.

```sh
uv run python -m bench.replay_fidelity run          # one process: sockets blocked for the replay
docker compose -f deploy/docker-compose.yml run --rm bench-record
docker compose -f deploy/docker-compose.yml run --rm bench-replay   # no network; refuses if fakellm is reachable
```

Exit status is 0 only if every Ledger replay was identical.

## Effects

A tool that changes the world (a refund, a charge, an email) gets `@ledger.effect` instead of `@ledger.tool`:

```python
@ledger.effect(name="stripe.refunds.create", retry_on=(stripe.APIConnectionError, stripe.APIError))
def create_refund(charge_id: str, amount: int, *, idempotency_key: str) -> dict:
    return client.v1.refunds.create(
        {"charge": charge_id, "amount": amount}, {"idempotency_key": idempotency_key}
    ).to_dict()
```

While recording, each call goes through a write-ahead protocol:

1. A `PENDING` event is fsynced to the log before the call starts, with the call's args and its idempotency key.
2. The function runs with `idempotency_key=` set to a key derived from the run id and the step (`ledger_` plus 40 hex digits of SHA-256). The same run reaching the same call always gets the same key.
3. An error in `retry_on` means the outcome is unknown, so the call is retried with the same key (`attempts=3` by default, with backoff). Stripe answers a repeated key with the first response, so a refund whose answer was lost isn't made twice.
4. The outcome is logged at the same step: `CONFIRMED` with the result, or `FAILED` when the API refused (any exception not in `retry_on`), which means the effect didn't happen.

If every attempt fails in a way that leaves the outcome unknown, the step stays `PENDING` and the call raises `ledger.EffectInDoubt`. Ledger doesn't guess: the effect may or may not have happened. Resuming the run settles it (see Resume).

Replays answer effects from the log and never call the function. A replay or fork that would have to run an effect live, because it diverged upstream or a fork reruns the effect, raises `ledger.LiveEffectRefused` unless it was started with `live_effects=True`. A live effect there belongs to the new run and gets that run's keys. A fork can still edit an effect's outcome, which never calls it.

This is effectively once, not exactly once: it relies on the API honouring idempotency keys, which Stripe does for 24 hours. For longer outages the key is also put in the refund's metadata, so it can be looked up.

Give an effect a `reconcile` function to settle it without relying on the API still remembering the key: it takes the effect's arguments and `idempotency_key`, and returns the effect's result if the API shows it happened, or `None` if it didn't.

## Sagas

An effect can say how to undo it. If the run later fails for good, Ledger undoes the run's confirmed effects, newest first, before logging the run as failed and raising the error:

```python
def release_claim(claimed, charge_id, ticket_id, *, idempotency_key):
    return client.v1.charges.update(charge_id, {"metadata": {"refund_claim": ""}},
                                    {"idempotency_key": idempotency_key}).to_dict()

@ledger.effect(compensate=release_claim)
def claim_charge(charge_id, ticket_id, *, idempotency_key): ...
```

The compensation gets the effect's result and its arguments. It runs as an effect itself: logged `PENDING` then `CONFIRMED` (or `FAILED`) at a step of its own (the effect's node with `:compensate` added), with its own idempotency key and the effect's retries. So a crash during compensation is resumed like any crash: the resume fails the same way again, compensations already confirmed are answered from the log, one left pending is settled, and the rest run. A replay of a compensated run answers the compensations from the log.

"Fails for good" means an exception that resuming can't fix. These don't count:
- `EffectInDoubt`: a resume may still settle the effect, and the run go on;
- Ledger's own replay errors;
- anything that isn't an `Exception`: a `KeyboardInterrupt` is a stop, not a verdict.

If a compensation fails, the others still run, and the run fails with `ledger.SagaIncomplete`, chained from the original error. A compensation left in doubt is settled by resuming. One the API refused is refused again on every resume, since the log answers it, so that run needs a person.

## Resume

A run that crashed (the process died) or failed (say, an effect ended in doubt) is picked up again with `ledger.resume`:

```python
with ledger.resume(run_id) as session:
    graph.invoke(inputs)            # the input the run started with
```

The resume continues the same run in the same log, so every effect keeps its idempotency key. The graph runs from the start, as in a replay:

- calls already in the log are answered from it, strictly, and aren't logged again;
- an effect the log left `PENDING` is settled first: its `reconcile` function looks the outcome up, and if there's none (or it finds nothing) the effect is called again with the same key;
- calls past the end of the log run live and are appended after a `RunResumed` event.

The finished log is a complete recording: it replays strictly like any other. A run that finished cleanly can't be resumed; one that failed can, and a resume that itself crashes is resumed again. `ledger resume --all` resumes the recordings a resume can help: those that never finished or were interrupted, and failed ones with an effect still `PENDING` (in doubt). A run that failed for good with every effect settled is final; `ledger runs` shows how often each run was resumed. Resume assumes one worker per run; leases for several workers come with M5.

`tests/test_resume.py` kills runs partway, with every later write to the log failing like a `kill -9`, then resumes them: effects with and without `reconcile`, a resume that itself crashes, the toy pipeline's parallel branches killed at several points, and `ledger resume --all`. Calls logged before the kill keep their answers, and every finished log replays strictly. `tests/test_refund_agent.py` kills the refund agent after each event of its log. Every time, there's exactly one refund at Stripe. Killed just after Stripe made the refund but before `CONFIRMED` was logged, the resume finds the refund through the agent's `reconcile` (by the key in its metadata) and doesn't ask Stripe again.

### Refund agent

`examples/refund_agent.py` turns a support ticket into a Stripe refund: `lookup` reads the charge (a tool), and `decide` applies the refund policy in code, leaving a charge another ticket has claimed to that ticket. `refund` first claims the charge for this ticket in its Stripe metadata (an effect whose compensation releases the claim), then creates the refund (an effect whose `reconcile` finds a refund by the key in its metadata). Last, `reply` has the LLM draft the answer. If Stripe refuses the refund, the saga releases the claim, so another ticket or a person can take over. It runs with the official `stripe` SDK against `ledger.testing.fake_stripe`, a fake Stripe API that honours idempotency keys and can drop a connection before or after applying a request, or answer 500. To point it at Stripe itself, set `STRIPE_API_BASE=` (empty) and put a test key in `STRIPE_SECRET_KEY`, in the environment or in `.env` at the repo root (git ignores it). Only that one variable is read from `.env`, live keys are refused, and the fake server never gets the real key.

```sh
uv run python -m ledger.testing.fake_stripe --port 12111 &
uv run python -m ledger.testing.fake_llm --port 8787 &
uv run python -m examples.refund_agent          # seeds a charge, records one refund
```

`tests/test_refund_agent.py` covers:
- a dropped or failed attempt (before or after Stripe applied it, and a 500) still ends in exactly one refund;
- a refund lost every time ends in doubt, with still one refund at Stripe;
- a refused refund is logged as `FAILED`, the claim is released, and a replay answers the release from the log;
- a replay with Stripe gone makes no request.

`tests/test_saga.py` checks sagas on a booking graph with an idempotent fake API:
- the compensations run newest first, each with its own key;
- `EffectInDoubt` isn't compensated;
- a refused compensation ends in `SagaIncomplete`, and one in doubt is settled by resume;
- a crash after each event of the compensation phase is resumed without undoing anything twice;
- async compensations work.

## Chaos benchmark

`chaos/` attacks the refund agent from outside. The harness works through a list of refund tickets with a worker process, kills the worker at random moments with SIGKILL, restarts it, and cuts its network to Stripe. Then it checks what Stripe holds. Two systems run the same agent code against the same faults:

- **Ledger:** one run per ticket (`ticket-<id>`). A restarted worker resumes every run that was cut off or left an effect pending (`ledger.resume`).
- **Stock LangGraph:** a `SqliteSaver` checkpointer with `durability="sync"`, one thread per ticket, and node retries on every node including the refund. A restarted worker invokes unfinished threads again from their last checkpoint. Without Ledger, each refund attempt gets a fresh idempotency key, as with a client that makes one per request.

Each ticket asks for a partial refund of its own fresh charge. That way Stripe would accept a second refund, so a duplicate is possible, as in real use. About one ticket in ten is over the automatic limit and expects no refund. The invariants (`chaos/invariants.py`):
- **duplicate:** refunds beyond the one expected;
- **missing:** an expected refund that isn't at Stripe at the end;
- for Ledger, the log is also reconciled against Stripe: **unlogged** (a refund at Stripe that no confirmed effect in the log carries), **phantom** (the reverse), and **unfinished** (a run that didn't settle).

```sh
uv run python -m chaos.harness --mode local --runs 500          # kill -9 a subprocess; fake Stripe drops requests
caffeinate -i uv run python -m chaos.harness --mode docker --runs 10000   # docker kill; Toxiproxy cuts connections
```

In local mode the fake Stripe server loses a request now and then: dropped before it's applied, dropped after (the answer is lost), or answered with a 500. Docker mode (`chaos/docker-compose.yml`) puts Toxiproxy between the worker container and Stripe, and cuts 3% of connections in each direction with a `timeout` toxic. Results go to `chaos/results/`. The harness exits 0 only if Ledger breaks no invariant.

**M2 gate passed: 0 duplicate charges in 10,000 crash-injected runs.** Docker mode, 10,000 tickets per system, on an Apple-silicon Mac (`chaos/results/docker.json`):

| System | Tickets | Kills | Refunds | Duplicate | Missing | Unlogged / phantom / unfinished | Resume p50 / p95 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Ledger | 10,000 | 558 | 8,988 | **0** | 0 | 0 / 0 / 0 | 0.46 / 0.99 s |
| stock LangGraph | 10,000 | 626 | 9,167 | **182** | 0 | n/a | 0.65 / 1.16 s |

Every refund the baseline made twice came from rerunning the refund node: after a kill, from the last checkpoint, or after a lost answer, by the node retry. Each rerun sends a new request with a new idempotency key. Ledger's retries and resumes reuse the key from the run and step, and the reconcile lookup settles a refund whose answer was lost. Resume time runs from a restart to the first cut-off ticket finishing, including container and process start. Each session took about half an hour.

The baseline counts a thread as finished when `get_state` has no tasks left. `state.next` alone isn't enough: a kill after a node's writes are saved, but before the next checkpoint, leaves `next` empty with a task still pending, and invoking the thread again carries on from it. An earlier baseline run that checked `next` skipped 7 such tickets; it was rerun with the fix.

Shorter sessions from development, with the same settings:

| Session | System | Tickets | Kills | Refunds | Duplicate | Missing | Unlogged or unfinished |
| --- | --- | --- | --- | --- | --- | --- | --- |
| local, 91 / 124 lost requests | Ledger | 500 | 8 | 445 | **0** | 0 | 0 |
| | stock LangGraph | 500 | 27 | 462 | **10** | 0 | n/a |
| Docker, Toxiproxy 3% | Ledger | 300 | 14 | 266 | **0** | 0 | 0 |
| | stock LangGraph | 300 | 16 | 272 | **5** | 0 | n/a |

A Mac that sleeps pauses the whole session. On battery `caffeinate -i` didn't stop it sleeping, so keep the Mac plugged in, or awake with the lid open.

## Overhead benchmark

`bench/overhead.py` times the 5-node pipeline 1,000 times in each of three modes, interleaved, against the fake server answering with no added delay:
- **off:** plain LangGraph with an unwrapped OpenAI client;
- **record:** instrumented, recording to SQLite on local disk with every event fsynced;
- **replay:** the run just recorded, served from its log.

Per step is a run's time over its 5 nodes. Three of them run in parallel, so it's a share of wall time, not one node's time. Added latency is the difference from **off** at each percentile.

```sh
uv run python -m bench.overhead        # writes bench/results/overhead.json
```

On an Apple M6 laptop, on battery (`bench/results/overhead.json`):

| Mode | Run p50 / p95 | Per step p50 / p95 | Added per step p50 / p95 |
| --- | --- | --- | --- |
| off | 7.36 / 10.12 ms | 1.47 / 2.02 ms | |
| record | 8.34 / 12.18 ms | 1.67 / 2.44 ms | **0.20 / 0.41 ms** |
| replay | 1.58 / 3.00 ms | 0.32 / 0.60 ms | −1.16 / −1.42 ms |

- Recording adds 0.12 ms per LLM or tool call at p50 (0.26 ms at p95), well within the target of a few ms. Repeat runs vary by about 0.05 ms per step.
- A run's log is 13 events and 9.3 KB: 8 LLM and tool calls, 3 clock, uuid and rng reads, and the run's start and finish.
- Replay is faster than the live run, since nothing goes over the network.
- The fsync is SQLite's default on macOS, not `F_FULLFSYNC`, so the numbers don't include flushing the drive's own cache.
- Against a real model, whose calls take hundreds of milliseconds, the added time is a rounding error.

The benchmark also turned up a replay cost that grew with the store. Naming a replay run (`<run>.replay-N`) used to parse the first event of every run in the store; at 2,000 runs that took 4.5 ms of a 6 ms replay. It now counts earlier replays with a range query on the primary key.

## Blame

`ledger.diagnose` names the step that broke a failed run. It reruns versions of the run where chosen calls are drawn live again, each a fork of the recording, and checks which ones pass.

```python
def ok(out) -> bool:                          # what success means for this graph
    return out["draft"].startswith("Report")

d = ledger.diagnose(run_id, graph, ok)
d.step                                        # StepInfo of the decisive step, or None
d.steps, d.complete                           # every step needed, and whether rerunning them alone fixed it
d.forks, d.live_calls, d.dollars              # what the diagnosis cost
d.status                                      # "found", "no_step", "budget" or "inconsistent"

ledger.diagnose(run_id, graph, ok, repeats=5, threshold=0,           # noisy reruns
                budget=ledger.blame.Budget(forks=30, dollars=0.25),
                pricing=ledger.blame.Pricing(input_per_mtok=0.15, output_per_mtok=0.60))
```

Rerunning from step k redraws every call from k on in log order and replays the ones before it from the log at no cost. If that fixes the run and rerunning from the next step doesn't, k's recorded outcome is needed for the failure. That is the decisive step as [Who&When](https://arxiv.org/abs/2505.00212) defines it: the earliest step whose correction turns failure into success. With one bad step, rerunning from k fixes the run for every k up to it and none after, so the step can be searched for.

Live calls are what a counterfactual costs, so the search starts at the end. It reruns the last call, then the last 2, 4, 8 and so on until a rerun fixes the run, then bisects inside the last gap. A step d calls from the end takes about 2·log2(d) forks, and none of them reruns much more than 2d calls. One more fork checks that rerunning the step alone (with what depends on it) is enough. It's skipped when every later call depends on the step, since the fork that found it already was that check. Rerunning everything is tried only when nothing shorter fixes the run.

- **Several bad steps:** if the first step found isn't enough alone, the search goes on over the steps after it, with the found ones always rerun.
- **No step:** if rerunning everything still fails, `d.step` is None. The cause is then in the code or the input, or the failure comes back whenever its step is redrawn.
- **Candidates:** LLM calls, tool calls, and clock, uuid and rng reads. Effects are not candidates, and a fork that would run one live raises `LiveEffectRefused` unless `live_effects=True`.
- **Noise:** `repeats=n` runs each counterfactual up to n times. It counts as a fix when more than `threshold` of the runs pass, and it stops as soon as that's settled. Each counterfactual reports its pass rate with a 95% Wilson interval.
  - The default threshold, 0.5, is a majority vote. That suits runs that sometimes pass even with the fault kept.
  - When they essentially never do, and noise only makes good runs fail, use `threshold=0`: one pass is enough. That covers a flaky call later on, or a model that repeats its mistake when redrawn. There a real fix can pass well under half the time, as the benchmark below shows.
- **Budget:** `budget` caps forks, live calls or dollars per diagnosis. Dollars come from the token usage the live LLM calls report, priced by `pricing`. A diagnosis that runs out ends with status `"budget"`: it keeps the steps found so far, and `d.narrowed` lists the candidates the step it was looking for must be among.

`ledger.testing.faults` makes failed runs with a known cause. It corrupts the outcome of one call of a recording and keeps the step it hit as ground truth. The corrupted outcome is logged as the call's own, so the log looks like any recording and nothing in it gives the answer away:

```python
from ledger.testing import faults

fault = faults.Fault(node="research_web", kind="llm_call", corrupt=faults.replace_text("a wrong summary"))
with ledger.record(store) as rec, faults.inject(fault):
    graph.invoke(inputs)
fault.step                                    # the guilty step's key
```

In the tests, one fault at each of the 8 calls of a parallel tool graph and at the LLM calls of the toy pipeline is found every time, and so are two faults in different branches. That is with the fake server, where a redrawn call never repeats the fault. Accuracy on runs where it can, and against baselines, is the week-7 benchmark.

**Prior work.** [Who&When](https://arxiv.org/abs/2505.00212)'s methods and [AgenTracer](https://arxiv.org/abs/2509.03312)-8B attribute a failure from its log alone; AgenTracer uses counterfactual replay only to label its training data. [FAMAS](https://arxiv.org/abs/2509.13782) reruns the system many times and scores steps by spectrum analysis. Rerunning counterfactuals at diagnosis time has been done too, for example by [Causal Agent Replay](https://arxiv.org/abs/2606.08275). What Ledger does that they don't: it reruns counterfactuals inside the runtime that recorded the run, so everything before the rerun step replays exactly from the log instead of being run again or simulated, a bisection keeps the reruns to about log2 of the step count, and no counterfactual repeats a side effect.

## Blame benchmark

`bench/blame_eval.py` makes failed runs with a known cause and scores each method on them. Each trial records a clean run, corrupts one of its LLM or tool calls at random with `ledger.testing.faults`, and records the run again. Every method then names a step of the failed run.

- **ledger:** `ledger.diagnose`.
- **scan:** the naive counterfactual. It reruns one step at a time (with what depends on it), last first, until a rerun fixes the run. It gives the same answers as `ledger` on clean runs, so it's there to compare costs.
- **random:** a random LLM or tool call.
- **judge:** an LLM reading the whole trace and naming the step, like Who&When's all-at-once method. It runs only when given a model (`--judge-model`, `--judge-base-url`, and a key in `LEDGER_JUDGE_API_KEY` or `OPENAI_API_KEY`, from the environment or `.env`). It has not been run yet.

Step accuracy counts the exact step; agent accuracy counts its node. The workloads run on the fake LLM server and local tools:

- **toy:** the 5-node pipeline, 11 candidate calls, with faults in its LLM calls.
- **chain-N:** N tool calls in sequence, to show how cost grows with the step count.

```bash
python -m bench.blame_eval --runs 100
```

On clean reruns, where a redrawn call never repeats the fault, each method has 100 runs per workload. Accuracy intervals are 95% Wilson; costs are means per diagnosis:

| Workload | ledger step acc | scan step acc | random step acc | ledger forks | scan forks | ledger live calls | scan live calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| toy | 1.00 (0.96–1.00) | 1.00 | 0.14 (0.09–0.22) | 5.1 | 5.4 | 15.6 | 10.3 |
| chain-8 | 1.00 (0.96–1.00) | 1.00 | 0.13 | 4.3 | 4.3 | 16.1 | 14.0 |
| chain-16 | 1.00 (0.96–1.00) | 1.00 | 0.07 | 6.2 | 8.2 | 40.6 | 47.7 |
| chain-32 | 1.00 (0.96–1.00) | 1.00 | 0.06 | 8.0 | 15.8 | 97.4 | 173.5 |
| chain-64 | 1.00 (0.96–1.00) | 1.00 | 0.03 | 9.9 | 31.1 | 226.3 | 659.4 |

- **Forks** grow with the log of the step count for `ledger` and linearly for `scan`. At 64 steps `ledger` makes 2.9 times fewer live calls.
- **Small graphs:** below about 16 steps, and on the parallel toy pipeline, `scan` makes fewer live calls. It reruns one branch and leaves its siblings replayed, while every `ledger` fork reruns everything after a point in the log.
- **Accuracy:** 100% here shows the search is correct. It does not show that real faults get found: on the fake server a redrawn call is always clean.

`--recur p` makes the guilty call go wrong again with probability p when it's rerun, and `--flake q` makes any live call go wrong with probability q. With recur 0.3, flake 0.05, 5 repeats and `threshold=0` (`bench/results/blame-noisy.json`):

| Workload | ledger step acc | scan step acc | ledger forks | scan forks | ledger live calls | scan live calls |
| --- | --- | --- | --- | --- | --- | --- |
| toy | 0.95 (0.89–0.98) | 1.00 (0.96–1.00) | 19.2 | 23.8 | 50.4 | 42.2 |
| chain-16 | 0.84 (0.76–0.90) | 0.95 (0.89–0.98) | 24.2 | 38.3 | 140.5 | 218.7 |
| chain-64 | 0.29 (0.21–0.39) | 0.57 (0.47–0.66) | 34.8 | 159.2 | 582.3 | 3,557.9 |

Noise costs `ledger` more accuracy than `scan`. A bisection has several forks that must pass, and each can fail for no reason, so each is a chance to go wrong; `scan` has only one fork that must pass. At 64 steps both break down, because a rerun of 64 calls at 5% flake each comes out clean about 4% of the time.

Two things follow. A majority vote is the wrong rule for this kind of noise. In 60-run sessions with the same noise and 5 repeats, `threshold=0.5` scored 0.63 on toy and 0.20 on chain-16, against 0.97 and 0.73 with `threshold=0`. And `ledger` isn't more accurate than the scan, only cheaper. Whether noisy reruns matter depends on how often a real model repeats or adds a mistake when a call is redrawn, which needs a real model to measure.

## Who&When

[Who&When](https://arxiv.org/abs/2505.00212) is 184 failure logs of LLM multi-agent systems: 126 from systems CaptainAgent generated and 58 from hand-crafted ones such as Magentic-One. Each is labelled with the agent responsible and the decisive step. The logs are recordings, not runnable systems, so counterfactual blame can't run on them; `bench/who_and_when.py` is where log-only methods are compared.

```bash
python -m bench.who_and_when download         # the JSON logs, about 7 MB, into bench/data/ (gitignored)
python -m bench.who_and_when stats
python -m bench.who_and_when run --judge-model MODEL --judge-base-url URL --judge-price IN OUT
```

The download is pinned to a dataset revision. The card states no license, so the data stays out of the repo.

The two halves are formatted differently, and the loader evens them out:

- **Speaker field:** a message's speaker is its `name` in the generated logs and its `role` in the hand-crafted ones.
- **Agent names:** the Orchestrator speaks as "Orchestrator (thought)" or "Orchestrator (-> WebSurfer)" while labels say "Orchestrator", and names differ in case and underscores ("Websurfer", "VerificationExpert"). Agents are therefore compared without the parenthesized suffix, lowercased, letters and digits only.

  With that, the speaker at the labelled step is the labelled agent in 178 of 184 logs. In the other 6 the label names someone else, such as a terminal's output blamed on the expert who ran the code.
- **Steps:** labels are 0-based indices into the message history.

The generated logs run 5 to 10 messages; the hand-crafted ones run 5 to 130, median 33. A judge reading every log in full reads about 1.3M tokens (by a 4-characters-per-token estimate), 0.93M of them in the hand-crafted half. `--max-chars` cuts long messages, and `--max-dollars` stops the judge once it has spent that much.

| Method | Agent accuracy | Step accuracy |
| --- | --- | --- |
| random, all 184 | 0.332 | 0.095 |
| random, algorithm-generated | 0.291 | 0.120 |
| random, hand-crafted | 0.422 | 0.042 |

`random` is the exact expected accuracy of picking one of the log's agents (not the human who set the task) and one of its steps uniformly. `judge` is Who&When's all-at-once method: an LLM given the task, optionally the right answer (`--with-ground-truth`), and the whole log, naming the agent and step. It needs a model and has not been run yet. Who&When's best method reached 53.5% agent-level and 14.2% step-level accuracy.

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
ledger resume --all                            # finish recordings that crashed or failed
```

`-g` names the graph as `module:attr`: a compiled graph, or a function with no arguments that returns one. `record` logs it with the run, so `replay` and `fork` find the graph again. They fall back to `$LEDGER_GRAPH` for runs recorded from Python, and an explicit `-g` overrides both. Replays and forks take their input from the recording. A run that failed when recorded replays identically if it fails with the same error. `--db` picks the event store (else `$LEDGER_DB`, else `ledger.db`). `python -m ledger` is the same command.

## Try it

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11 to 3.13.

```sh
uv sync
uv run pytest                                  # includes the replay gate with sockets blocked
HYPOTHESIS_PROFILE=thorough uv run pytest tests/test_properties.py   # 1000 random graphs per property
uv run python -m bench.replay_fidelity run       # the 200-run replay benchmark (about a minute)

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
| `ledger/replay/` | Replay index, divergence detection, `replay`, `fork` with `steps`, and `resume` |
| `ledger/blame/` | `diagnose`: counterfactual blame by bisection over forked replays |
| `ledger/effects/` | `@ledger.effect`: idempotency keys, write-ahead pending/confirmed records, retries; sagas |
| `ledger/cli.py` | The `ledger` command: `record`, `replay`, `fork`, `resume`, `steps`, `runs` |
| `ledger/testing/fake_llm.py` | Nondeterministic fake LLM and search server |
| `ledger/testing/faults.py` | Fault injection into recordings, with the guilty step as ground truth |
| `ledger/testing/fake_stripe.py` | Fake Stripe API (charges, refunds, idempotency keys) with injectable faults |
| `examples/refund_agent.py` | Support-ticket refund agent; the M2 workload |
| `chaos/` | Chaos benchmark: worker, harness (local `kill -9` or `docker kill` with Toxiproxy), invariants |
| `examples/toy_pipeline.py` | 5-node pipeline with a 3-way parallel fan-out |
| `bench/replay_fidelity.py` | Replay-fidelity benchmark against LangGraph checkpoint replay; results in `bench/results/` |
| `bench/blame_eval.py` | Blame benchmark: injected faults, scored for `diagnose`, a rerun scan, random pick and an LLM judge |
| `bench/who_and_when.py` | Who&When: download, loader, and log-only methods (random, an LLM judge) scored on it |
| `bench/overhead.py` | Overhead benchmark: added latency per step with Ledger recording and replaying |
| `deploy/` | Dockerfile and Compose file |

Payloads are stored as canonical JSON inside protobuf events, because protobuf serialization is not canonical and would break content hashing.
