# Benchmarks

Every number in the README, how it was measured, against which baseline, and what it does not show.

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

## Blame benchmark

`bench/blame_eval.py` makes failed runs with a known cause and scores each method on them. Each trial records a clean run, corrupts one of its LLM or tool calls at random with `ledger.testing.faults`, and records the run again. Every method then names a step of the failed run.

- **ledger:** `ledger.diagnose`.
- **scan:** the naive counterfactual. It reruns one step at a time (with what depends on it), last first, until a rerun fixes the run. On clean reruns it gives the same answers as `ledger`, so it's there to compare costs.
- **random:** a random LLM or tool call.
- **judge:** an LLM reading the whole trace and naming the step, like Who&When's all-at-once method. It runs only when given a model (`--judge-model`, `--judge-base-url`, and a key in `LEDGER_JUDGE_API_KEY` or `OPENAI_API_KEY`, from the environment or `.env`).

Step accuracy counts the exact step; agent accuracy counts its node. Accuracy intervals are 95% Wilson; costs are means per diagnosis. Each trial draws its fault and its random pick from its own seeds, over the calls in a fixed order rather than log order (calls of parallel nodes reach the log in thread order), so `--workers N` runs trials in parallel without changing which faults are drawn or what `random` picks. Fork and live-call counts can still differ a little between runs, since `diagnose` bisects in log order.

### On a real model

The M3 gate, beating the LLM-judge baseline on attribution accuracy, is measured on the `orders` workload, `ledger/examples/order_pipeline.py`:

- **The pipeline:** a customer asks in plain words for some items, shipped to a state, with a coupon. `intake` turns the request into a JSON order. Then `pricing` (a catalog lookup per item, then the model adds up the subtotal), `tax` and `coupon` run in parallel. `accountant` applies the discount and the tax, and `writer` answers the customer.
- **Success:** the reply quotes the right total to the cent. The right total follows from the catalog.
- **The model:** every LLM call runs on `gpt-6-luna` at low reasoning effort, so the model does the arithmetic, and reruns are as noisy as it is.
- **The faults:** each changes one number in a call's outcome, the way a bad lookup or a model's slip would, so the trace still reads plausibly. Nothing marks the bad value.

```bash
python -m bench.blame_eval --workloads orders --model gpt-6-luna --price 0.10 0.50 \
    --judge-model gpt-6.2-sol --judge-price 2 10 --runs 200 --repeats 3 --threshold 0 \
    --workers 8 --max-dollars 2
```

200 trials ran. In 36 the fault didn't change the total (say, a perturbed number the next step recomputed), and in 1 the clean run was already wrong. The other 163 had a known guilty step (`bench/results/blame-orders.json`):

| Method | Step accuracy (95% CI) | Agent accuracy | Forks | Live calls | $ per diagnosis |
| --- | --- | --- | --- | --- | --- |
| ledger (`gpt-6-luna` reruns) | **0.994 (0.97–1.00)** | 0.994 | 10.6 | 33.0 | 0.0008 |
| scan (`gpt-6-luna` reruns) | 0.994 (0.97–1.00) | 0.994 | 12.4 | 33.0 | 0.0009 |
| judge (`gpt-6.2-sol`) | 0.687 (0.61–0.75) | 0.687 | – | – | 0.0019 |
| random | 0.123 (0.08–0.18) | 0.233 | – | – | – |

The intervals of `ledger` and the judge don't overlap, so on this workload the gate passes. `ledger` named the injected step in 162 of 163 runs, the judge in 112. The judge, on a model 20 times the price per token, read the whole trace and still got the step wrong in 31% of runs. With no marker on the bad number, it has to work out which of several plausible values is the one that broke the arithmetic.

The counterfactual method reruns the run instead: a diagnosis took 10.6 forks of mostly replayed calls, and cost 40% of what the judge's single reading did.

The whole run cost $0.66 and took 25 minutes on 8 workers:

| Where | Cost |
| --- | --- |
| recording | $0.06 |
| `ledger` | $0.13 |
| scan | $0.15 |
| judge | $0.32 |

What this does not show:

- **One workload, our own:** results on other agent systems may differ.
- **Hard to repeat:** `gpt-6-luna` rarely got the arithmetic wrong on its own (1 clean failure in 200), so reruns rarely repeated or added mistakes. That noise, where `ledger` lost accuracy to the scan on the fake server, was barely exercised.
- **Injected faults:** the guilty step is the injected one by construction. A model's own mistakes, found in the wild, have no such ground truth.

### On the fake server

```bash
python -m bench.blame_eval --runs 100 --workers 4
```

On clean reruns, where a redrawn call never repeats the fault, there are 100 runs per workload:

| Workload | ledger step acc | scan step acc | random step acc | ledger forks | scan forks | ledger live calls | scan live calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| toy | 1.00 (0.96–1.00) | 1.00 | 0.09 | 4.6 | 4.8 | 13.9 | 9.1 |
| chain-8 | 1.00 (0.96–1.00) | 1.00 | 0.11 | 4.6 | 4.8 | 18.0 | 16.6 |
| chain-16 | 1.00 (0.96–1.00) | 1.00 | 0.07 | 6.5 | 9.2 | 45.5 | 57.6 |
| chain-32 | 1.00 (0.96–1.00) | 1.00 | 0.04 | 8.5 | 18.1 | 109.5 | 214.4 |
| chain-64 | 1.00 (0.96–1.00) | 1.00 | 0.01 | 10.5 | 35.7 | 255.2 | 821.5 |

The workloads:

- **toy:** the 5-node pipeline, 11 candidate calls, with faults in its LLM calls.
- **chain-N:** N tool calls in sequence, to show how cost grows with the step count.

What the table shows:

- **Forks** grow with the log of the step count for `ledger` and linearly for `scan`. At 64 steps `ledger` makes 3.2 times fewer live calls.
- **Small graphs:** below about 16 steps, and on the parallel toy pipeline, `scan` makes fewer live calls. It reruns one branch and leaves its siblings replayed, while every `ledger` fork reruns everything after a point in the log.
- **Accuracy:** 100% here shows the search is correct. It does not show that real faults get found, since on the fake server a redrawn call is always clean.

`--recur p` makes the guilty call go wrong again with probability p when it's rerun, and `--flake q` makes any live call go wrong with probability q. With recur 0.3, flake 0.05, 5 repeats and `threshold=0` (`bench/results/blame-noisy.json`):

| Workload | ledger step acc | scan step acc | ledger forks | scan forks | ledger live calls | scan live calls |
| --- | --- | --- | --- | --- | --- | --- |
| toy | 0.97 (0.92–0.99) | 0.99 (0.95–1.00) | 16.8 | 20.5 | 44.0 | 35.4 |
| chain-16 | 0.81 (0.72–0.88) | 0.96 (0.90–0.98) | 26.4 | 43.2 | 163.4 | 260.9 |
| chain-64 | 0.19 (0.13–0.28) | 0.55 (0.45–0.64) | 37.1 | 182.8 | 691.5 | 4,371.4 |

Noise costs `ledger` more accuracy than `scan`. A bisection has several forks that must pass, and each can fail for no reason, so each is a chance to go wrong; `scan` has only one fork that must pass. At 64 steps both break down, because a rerun of 64 calls at 5% flake each comes out clean about 4% of the time.

Two things follow:

- **Majority vote is the wrong rule for this noise.** In 60-run sessions with the same noise and 5 repeats, `threshold=0.5` scored 0.63 on toy and 0.20 on chain-16, against 0.97 and 0.73 with `threshold=0`.
- **`ledger` is cheaper, not more accurate.** Under this noise it isn't more accurate than the scan, only cheaper.

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

| Method | Logs | Agent accuracy (95% CI) | Step accuracy (95% CI) |
| --- | --- | --- | --- |
| random | all 184 | 0.332 | 0.095 |
| random | algorithm-generated (126) | 0.291 | 0.120 |
| random | hand-crafted (58) | 0.422 | 0.042 |
| judge, gpt-6.2-sol | all 184 | 0.603 (0.53–0.67) | 0.429 (0.36–0.50) |
| judge, gpt-6.2-sol | algorithm-generated | 0.619 (0.53–0.70) | 0.484 (0.40–0.57) |
| judge, gpt-6.2-sol | hand-crafted | 0.569 (0.44–0.69) | 0.310 (0.21–0.44) |
| judge, gpt-6.2-sol, given the right answer | all 184 | 0.592 (0.52–0.66) | 0.380 (0.31–0.45) |
| judge, gpt-6.2-sol, given the right answer | algorithm-generated | 0.603 (0.52–0.68) | 0.444 (0.36–0.53) |
| judge, gpt-6.2-sol, given the right answer | hand-crafted | 0.569 (0.44–0.69) | 0.241 (0.15–0.36) |

- **random:** the exact expected accuracy of picking one of the log's agents (not the human who set the task) and one of its steps uniformly.
- **judge:** Who&When's all-at-once method. An LLM gets the task, optionally the right answer (`--with-ground-truth`), and the whole log, and names the agent and step. It ran on `gpt-6.2-sol` at its default reasoning effort, with no custom temperature, since the GPT-6.2 models reject one. That makes each number one sample, not a fixed answer.
- **Cost and logs:** each pass read about 1.39M input tokens and cost about $3.00 (`bench/results/who_and_when.json` and `who_and_when-ground-truth.json`). The per-log answers are in the same files.

For comparison, the best method in the Who&When paper reached 53.5% agent-level and 14.2% step-level accuracy, with the models of its time. Two findings:

- **Agent accuracy:** today's judge is close to that agent-level figure (0.60 against 0.535).
- **Step accuracy:** it is three times higher (0.43 against 0.142).

Being given the right answer did not help: step accuracy was 0.38 with it, against 0.43 without. The intervals overlap, so the honest reading is that the right answer adds nothing measurable here, not that it hurts.

On Who&When this judge can only be compared with other log-only methods, because counterfactual blame needs a system it can rerun. The head-to-head with `ledger.diagnose` is the blame benchmark on a real model, above, where the same judge scores 0.69 against `ledger`'s 0.99.
