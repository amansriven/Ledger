# Ledger: Detailed Build Plan

Nov 11, 2026 · @Aman Sriven

Ledger is a Python runtime under LangGraph that makes agent runs replayable, crash-safe and self-diagnosing. At about 15 hours a week it is resume-worthy after roughly 5 weeks and complete after roughly 8. This plan expands the build plan into components, weekly tasks, gates and measurements.

## At a glance

&#91;embedded content: Ledger phases by week · 6 phases, each with a gate\]

Replay and exactly-once effects fill weeks 1 to 5 and make the project resume-worthy. Blame and the launch take weeks 6 to 8, and the AWS stretch can start any time after M2.

## Goals

Ledger has to prove each guarantee with a number measured against the tool people would use today. The targets below are set; none has been measured yet.

| Guarantee | Metric | Target | Baseline |
| --- | --- | --- | --- |
| Deterministic replay | Recorded runs that replay identically with the network off | 100% of 200 runs | LangGraph's built-in checkpoint replay |
| Exactly-once effects | Duplicate and missing charges per 10,000 crash-injected runs | 0 | Stock LangGraph checkpointer with naive retry |
| Automatic blame | Agent-level and step-level attribution accuracy on injected faults | Beat the LLM-judge baseline | Random pick; LLM judge over the full trace; AgenTracer-8B if it runs |
| Overhead | Added latency per step at p50 and p95 | A few ms | The same run with Ledger off |
| Distributed exactly-once (stretch) | Duplicate and missing charges across 3 or more workers | 0 | Stock LangGraph with a shared Postgres checkpointer |

The project is resume-worthy once replay and exactly-once effects pass their gates (end of week 5). Blame and the launch come next because they make it stand out. The minimum version leaves out the UI, blame, speculation and multi-machine runs.

## Components

Every LLM call, tool call and source of randomness passes through one interceptor, and every other part is built on the log it writes. The LangGraph agent, the LLM APIs and Stripe stay unchanged; the parts below are what you build.

### Interceptor

Wraps the LLM client, tool functions, the clock and random, so the agent code does not change.

- In record mode it sends each call live and appends the request and response to the event log. In replay mode it answers from the log.
- All model calls go through one OpenAI-compatible client, so Nemotron on NVIDIA NIM or any other provider plugs in the same way.
- A two-line LangGraph integration hooks it into an existing graph.
- Repo: `ledger/intercept/` (`llm.py`, `tools.py`, `langgraph.py`). Built in M0.

### Event log

An append-only record of every call, input and output in each run.

- SQLite in WAL mode first, Postgres later. Large payloads live on disk as blobs keyed by SHA-256.
- Events are Protobuf schemas in `proto/events.proto`, shared by the runtime, the Go server and the UI.
- Crash-safe from day one: a write lands whole or not at all.
- Repo: `ledger/core/` (`events.py`, `store.py`). Built in M0, made content-addressed in M1.

### Nondeterminism shims

Context-scoped replacements for time, random and uuid, plus the recorded completion order of parallel branches.

- Every value read during record is logged and served back in the same order during replay.
- Parallel branches that finish in a different order produce different state, so their completion order is recorded and enforced.
- Repo: `ledger/core/clock.py`. Built in M1.

### Replay engine

Re-executes a recorded run step for step with zero live API calls.

- Requests are normalized before hashing (timestamps, ids, dict ordering), so the same request finds its log entry.
- It detects the first step where a call no longer matches the log and switches to live calls from there.
- Fork reuses that mechanism: start at any step with edited state, replay the prefix, run the rest live.
- Repo: `ledger/replay/` (`engine.py`, `fork.py`, `diverge.py`). Built in M1.

### Effect manager

Makes tool calls with side effects happen effectively once, even across crashes.

- Idempotency keys come from the run id and step id, so a retried Stripe call is a no-op.
- A write-ahead record marks each effect pending, then confirmed. On resume, pending effects are reconciled against the API.
- Saga compensation undoes earlier effects when a later step fails for good.
- The first class of tool is payments and refunds against Stripe in test mode.
- Repo: `ledger/effects/` (`manager.py`, `idempotency.py`, `saga.py`). Built in M2.

### Chaos harness

Attacks the whole runtime from outside with real failures, not simulated exceptions.

- Kills the worker container with `docker kill` at a random step, then restarts it.
- Uses Toxiproxy to drop, delay and duplicate network traffic.
- After each run, checks the invariants: the Stripe test-mode ledger matches the event log, with no duplicate or missing charges.
- Repo: `chaos/` (`harness.py`, `invariants.py`). Built in M2.

### Blame engine

Names the step that caused a failed run by rerunning counterfactual versions of it.

- Delta debugging over forked replays: change a step's output, rerun from there, and check whether the run now succeeds.
- Binary search over steps keeps reruns growing with the log of the step count. The recorded prefix is replayed, not paid for again.
- Each counterfactual runs several times and reports a success rate with a confidence interval.
- A budget caps reruns and dollars per diagnosis.
- Repo: `ledger/blame/` (`delta.py`, `budget.py`). Built in M3.

### Trace server and UI

A Go server streams events over Connect (gRPC) to a SvelteKit front end.

- Views: a live timeline, a run diff, and blame with the guilty step highlighted.
- Repo: `server/`, `ui/`, `proto/trace_service.proto`. Built in M4.

### CLI, containers and tests

- Three commands: `ledger record`, `ledger replay`, `ledger fork`.
- Docker Compose starts the worker, Go server, UI, Postgres and Toxiproxy with one command. Code-running tools get throwaway containers, so replays never touch the host.
- pytest plus Hypothesis property tests check the invariants.
- Repo: `ledger/cli.py`, `deploy/docker-compose.yml`.

## Phase plan

The five phases run about 8 weeks at roughly 15 hours a week, with multi-worker AWS chaos as a stretch phase. Don't start a phase until the gate before it holds.

### M0 · Spike (week 1)

Get one run recorded and replayed end to end, however crudely.

- [ ] Build a toy 5-node LangGraph pipeline with a parallel fan-out, so ordering problems show up from the start
- [ ] Write the first Protobuf event types (LLM call, tool call) in `proto/events.proto`
- [ ] Interceptor v0: wrap the OpenAI-compatible client and the tool functions, append every call to SQLite in WAL mode
- [ ] Naive replay that serves logged responses in order
- [ ] Docker Compose file for the services in use so far

**Gate:** one run replays with zero live API calls, checked with the network off.

### M1 · Deterministic replay (weeks 2 to 3)

Make replay exact for every run, not just the easy one.

Week 2:

- [ ] Content-addressed blob store keyed by SHA-256
- [ ] Request normalization before hashing: strip timestamps and ids, sort dict keys
- [ ] Shims for time, random and uuid
- [ ] Record and enforce the completion order of parallel branches

Week 3:

- [ ] Divergence detector that switches to live calls at the first mismatch
- [ ] `ledger record`, `ledger replay` and `ledger fork` (fork from any step with edited state)
- [ ] Hypothesis property test: a replayed run produces the same events as its recording
- [ ] Replay-fidelity benchmark: record 200 runs, replay each with the network off, log storage per run

**Gate:** 100% identical replay on 200 recorded runs.

### M2 · Exactly-once effects (weeks 4 to 5)

Prove that a crash in the middle of a refund never charges twice. The project is resume-worthy when this gate passes.

Week 4:

- [ ] Example refund agent against Stripe in test mode
- [ ] Effect manager: idempotency keys from run id and step id, plus a pending/confirmed write-ahead record
- [ ] Resume after a crash: reload the log, replay the prefix, reconcile pending effects against Stripe
- [ ] Saga compensation for the refund flow

Week 5:

- [ ] Chaos harness: `docker kill` the worker at a random step, restart it; Toxiproxy for network faults
- [ ] Invariant checker that reconciles the event log against the Stripe test-mode ledger
- [ ] Same faults against the baseline: stock LangGraph checkpointer with naive retry
- [ ] 10,000 crash-injected runs for each; record duplicates, missing charges and time to resume
- [ ] Overhead microbenchmark at p50 and p95

**Gate:** 0 duplicate charges in 10,000 chaos runs. Fill in the first and third resume bullets with the measured numbers.

### M3 · Counterfactual blame (weeks 6 to 7)

Turn deterministic replay into a diagnosis: which step broke this run.

Week 6:

- [ ] Read AgenTracer and the spectrum-based methods; write one sentence on what Ledger does that they don't
- [ ] Fault injector for your own pipelines that records the guilty step as ground truth
- [ ] Delta debugging over forked replays, with binary search over steps

Week 7:

- [ ] Rerun budget per diagnosis; repeat each counterfactual and report a success rate with a confidence interval
- [ ] Baselines: random pick, LLM judge over the full trace, AgenTracer-8B if it runs
- [ ] Compare log-only methods head to head on Who&When
- [ ] Measure agent-level and step-level accuracy, reruns and dollars per diagnosis

**Gate:** beats the LLM-judge baseline on attribution accuracy.

### M4 · UI and launch (week 8)

Ship it so a stranger can install it in two lines and see the guarantees for themselves.

- [ ] Go trace server streaming events over Connect (gRPC)
- [ ] SvelteKit UI: live timeline, run diff, blame view
- [ ] PyPI release with the two-line LangGraph integration and the example refund agent
- [ ] 90-second demo video
- [ ] Read-only hosted UI with recorded runs on one small EC2 or Lightsail instance
- [ ] Writeup posts, one per guarantee

**Gate:** writeup live and a first outside user.

### M5 · Multi-worker chaos on AWS (stretch, any time after M2)

Show that exactly-once holds when several workers share one log.

- [ ] Move the event log to Postgres on RDS
- [ ] Per-run lease with a fencing token; reject writes that carry a stale token
- [ ] 3 or more ECS Fargate workers; random task kills and network faults between them
- [ ] Baseline: stock LangGraph with a shared Postgres checkpointer
- [ ] Scripted setup and teardown so nothing runs between benchmark sessions

**Gate:** 0 duplicate charges across 3 workers.

Further stretch ideas, in no order: speculative tool execution, a TLA+ spec of the effect protocol, and OpenTelemetry export.

## Testing and benchmarks

Each benchmark runs Ledger and its baseline (see Goals) on the same workload and the same faults, from one script anyone can rerun with Docker Compose.

| Benchmark | Workload | Faults | Reported as | Script |
| --- | --- | --- | --- | --- |
| Replay fidelity | 200 recorded runs of the 5-node pipeline | None; replay with the network off | Share replayed identically; storage per run in KB | `bench/replay_fidelity.py` |
| Exactly-once effects | Refund agent, 10,000 runs per system | Worker killed at a random step; Toxiproxy network faults | Duplicate and missing charges; time to resume | `bench/exactly_once.py` |
| Blame | Your own pipelines with injected faults; Who&When logs | One injected fault per run, guilty step known | Agent-level and step-level accuracy with confidence intervals; reruns and dollars per diagnosis | `bench/blame_eval.py` |
| Overhead | Microbenchmark on the 5-node pipeline | None | Added latency per step at p50 and p95; log size | `bench/overhead.py` |
| Distributed exactly-once (stretch) | ECS Fargate workers sharing one RDS event log | Random task kills; network faults between workers | Duplicate and missing charges; resume time after a lost task | To add with M5 |
| Speculation (stretch) | Multi-hop tasks with read-only tools | None | End-to-end p50 and p95 latency; hit rate; wasted calls | To add if built |

The blame bar is low and documented. On [Who&When](https://arxiv.org/abs/2505.00212), failure logs from 127 multi-agent systems, the best method in the original paper found the responsible agent 53.5% of the time but the decisive step only 14.2% of the time. Who&When holds recorded logs, not runnable systems, so counterfactual reruns can't run on it. The main blame result comes from your own pipelines; Who&When is where log-only methods are compared head to head.

Alongside the benchmarks, pytest and Hypothesis property tests run on every change: replay matches the recording, no effect is confirmed twice, and every pending effect is either confirmed or compensated after resume.

## Risks

The hardest parts are nondeterminism in replay and being honest about "exactly once". The parallel fan-out in the week-1 pipeline brings both out early.

| Risk | How it shows up | Mitigation | Phase |
| --- | --- | --- | --- |
| Parallel ordering | Runs that look identical drift on replay because branches finished in a different order | Record the completion order and enforce it on replay | M0 to M1 |
| Cache keys | Replay misses the log because prompts carry timestamps, ids and dict orderings | Normalize before hashing; define explicitly what counts as "the same request" | M1 |
| Divergence after a code change | After a node is edited, later calls no longer match the log | Detect the first mismatch and go live from there; fork uses the same code | M1 |
| Overclaiming "exactly once" | Infra reviewers point out that external APIs only allow effectively-once | Idempotency keys where the API supports them, a pending/confirmed record and reconciliation where it doesn't; say so plainly in the writeup | M2 |
| Blame cost | Each counterfactual rerun spends LLM calls | Replay the recorded prefix, binary-search over steps, cap the budget per diagnosis | M3 |
| Sampling noise in blame | One successful rerun turns out to be luck | Run each counterfactual several times; report a success rate with a confidence interval | M3 |
| Prior work on blame | Ledger looks like a copy of AgenTracer or spectrum-based methods | Read them before week 6; state the difference: online, built into the runtime, on executable systems | M3 |
| Ownership across workers | Two workers both claim a step after a task dies | Per-run lease with a fencing token in Postgres; reject writes with a stale token | M5 |
| Cloud cost | Fargate and RDS left running burn credits | Scripted setup and teardown; run only during benchmark sessions | M5 |

## Launch

Ship Ledger as something a stranger can install in two lines, then let the measured numbers make the case.

- **Demo, 90 seconds:** an agent crashes mid-refund and restarts, and the Stripe dashboard shows exactly one charge. Then a failing run opens in the UI with the guilty step highlighted.
- **Hosted UI:** a read-only copy with recorded runs on one small EC2 or Lightsail instance, so recruiters can click through without installing anything.
- **Writeup:** one post per guarantee, each with its chart, its baseline and one thing that broke along the way.
- **Distribution:** `pip install`, a two-line LangGraph integration, the example refund agent in the repo, and a post wherever LangGraph users gather.

The pitch lands differently by audience: Stripe will recognize idempotency keys and ledger invariants under failure, Databricks the tracing and evaluation work, NVIDIA a runtime serving Nemotron, and big-tech infra teams durable execution, chaos testing and delta debugging.

Resume bullets (fill the brackets with measured numbers only):

- Built Ledger, a durable Python runtime for LLM agents with deterministic record/replay and exactly-once side effects; 0 duplicate charges across \[10,000\] crash-injected runs vs \[N\] with stock LangGraph checkpointing.
- Designed counterfactual failure attribution via delta debugging over replayed traces; located the failure-causing step in \[X\]% of injected-fault runs vs \[Y\]% for an LLM-judge baseline, using \[Z\] reruns per diagnosis.
- Kept overhead to \[A\] ms p50 per step while replaying \[200\] recorded runs with 100% fidelity.

## Open questions

- [ ] Week 8 holds the Go server, the SvelteKit UI, the PyPI release, the demo and the writeup. Is that one week, or should M4 get two?
- [ ] Do Stripe's test-mode rate limits allow 10,000 runs per system in one week-5 session, or does the benchmark need to be spread out?
- [ ] Which model records the 200 runs and serves as the LLM judge, and what is the dollar budget for recording and blame reruns?
- [ ] Can AgenTracer-8B run on hardware you have, or is the baseline limited to random pick and the LLM judge?
- [ ] Does M5 start straight after M2 or after the launch?
- [ ] Is "Ledger" the final name, and is it free on PyPI?

## Sources

- [Durable Agent Runtime: Build Plan](https://claude.ai/artifact/CxGGZYzPx6y3zwaQ5tx4wG), the plan this one expands
- [Which Agent Causes Task Failures and When? (Who&When), arXiv 2505.00212](https://arxiv.org/abs/2505.00212)
- [AgenTracer: Who Is Inducing Failure in the LLM Agentic Systems?, ICLR 2026](https://iclr.cc/virtual/2026/poster/10007726)
