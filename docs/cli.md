# The `ledger` command

Record, replay, fork, resume and diagnose runs from the shell.

```sh
ledger record -g ledger.examples.toy_pipeline:graph_from_env --input '{"topic": "fencing tokens"}'
ledger runs                                    # every run: mode, source, status, live calls
ledger replay RUN_ID                           # strict; exit 1 unless identical
ledger replay --all --live                     # report divergences instead of failing
ledger steps RUN_ID [--node write]             # the calls a fork can target, by index
ledger fork RUN_ID --edit 4='a different summary' --edit-json 3='{"results": []}' --rerun 11
ledger resume --all                            # finish recordings that crashed or failed
ledger diagnose RUN_ID --check app:passed -o diagnosis.json   # which step broke a failed run
ledger ui [--port 8700] [--open]               # the trace UI and its API, read-only
```

`-g` names the graph as `module:attr`: a compiled graph, or a function with no arguments that returns one. `record` logs it with the run, so `replay` and `fork` find the graph again. They fall back to `$LEDGER_GRAPH` for runs recorded from Python, and an explicit `-g` overrides both. Replays and forks take their input from the recording. A run that failed when recorded replays identically if it fails with the same error. `--db` picks the event store (else `$LEDGER_DB`, else `ledger.db`). `python -m ledger` is the same command.

`diagnose` runs `ledger.diagnose` on a failed run. `--check` (else `$LEDGER_CHECK`) names a function that takes the run's output, or its output and input, and returns whether the run passed. The diagnosis goes to stdout or `--out` as JSON (schema `ledger.diagnosis/1`): the status (`found`, `no_step`, `budget` or `inconsistent`), the guilty steps, every counterfactual with its pass rate and Wilson interval, and what was spent. A one-line verdict goes to stderr. `--repeats`, `--threshold`, `--kind`, `--max-forks`, `--max-live-calls`, `--max-dollars` with `--price IN OUT`, and `--live-effects` map onto the Python arguments described under [Blame](blame.md). The exit status is 0 when it found the step, 1 when it didn't, and 2 on an error, such as a run that passes its check. The diagnosis is also saved in the store, where the trace UI's blame view shows it; `--no-save` skips that.

`ui` serves the trace API (`ledger.v1.TraceService` in `proto/trace_service.proto`, over the [Connect](https://connectrpc.com/docs/protocol) protocol with JSON) and the built trace UI on `127.0.0.1:8700`. It opens the store read-only, so it can run beside a process that is recording, and `WatchRun` streams a run's events as they are logged. See [Trace UI](ui.md) for the views and for `ledger-server`, the Go server for the hosted UI.
