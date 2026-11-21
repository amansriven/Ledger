# Blame

How `ledger.diagnose` finds the step that broke a failed run, and how to make failed runs with a known cause.

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
  - When they essentially never do, and noise only makes good runs fail, use `threshold=0`: one pass is enough. That covers a flaky call later on, or a model that repeats its mistake when redrawn. There a real fix can pass well under half the time, as the [noisy blame benchmark](benchmarks.md#on-the-fake-server) shows.
- **Budget:** `budget` caps forks, live calls or dollars per diagnosis. Dollars come from the token usage the live LLM calls report, priced by `pricing`. A diagnosis that runs out ends with status `"budget"`: it keeps the steps found so far, and `d.narrowed` lists the candidates the step it was looking for must be among.

`ledger.testing.faults` makes failed runs with a known cause. It corrupts the outcome of one call of a recording and keeps the step it hit as ground truth. The corrupted outcome is logged as the call's own, so the log looks like any recording and nothing in it gives the answer away:

```python
from ledger.testing import faults

fault = faults.Fault(node="research_web", kind="llm_call", corrupt=faults.replace_text("a wrong summary"))
with ledger.record(store) as rec, faults.inject(fault):
    graph.invoke(inputs)
fault.step                                    # the guilty step's key
```

In the tests, one fault at each of the 8 calls of a parallel tool graph and at the LLM calls of the toy pipeline is found every time, and so are two faults in different branches. That is with the fake server, where a redrawn call never repeats the fault. Accuracy on runs where it can, and against baselines, is in the [blame benchmark](benchmarks.md#blame-benchmark).

**Prior work.** [Who&When](https://arxiv.org/abs/2505.00212)'s methods and [AgenTracer](https://arxiv.org/abs/2509.03312)-8B attribute a failure from its log alone; AgenTracer uses counterfactual replay only to label its training data. [FAMAS](https://arxiv.org/abs/2509.13782) reruns the system many times and scores steps by spectrum analysis. Rerunning counterfactuals at diagnosis time has been done too, for example by [Causal Agent Replay](https://arxiv.org/abs/2606.08275). What Ledger does that they don't: it reruns counterfactuals inside the runtime that recorded the run, so everything before the rerun step replays exactly from the log instead of being run again or simulated, a bisection keeps the reruns to about log2 of the step count, and no counterfactual repeats a side effect.
