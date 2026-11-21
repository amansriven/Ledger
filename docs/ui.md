# Trace UI

The trace UI reads the event store and has three views:

- **Runs and timeline.** Every run, with replays and forks under their recording. A run opens as a timeline: one lane per node, one bar per call, effects from their pending record to their outcome, and markers where the run was resumed or finished. A run that is still being recorded fills in as it goes. Clicking a call shows its step key, latency, idempotency key, and request and response.
- **Diff** (`/diff?b=<run>`). A replay or fork against the run it came from, or any two runs, matched call by call on step keys the way replay matches them: same, changed, edited, rerun, diverged (with the reason), or only in one run. Opening a row shows a line diff of the request and the outcome, with LLM prompts shown as text.
- **Blame** (`/runs/<run>/blame`). A diagnosis saved by [`ledger diagnose`](cli.md): the verdict, then one row per counterfactual fork showing which calls it drew again and whether that fixed the run, then the failed run's timeline with the guilty call highlighted.

Two servers answer the same API (`ledger.v1.TraceService` in `proto/trace_service.proto`) and serve the same UI:

```sh
ledger ui --db ledger.db --open                 # in the pip package: Python, no other dependencies
make server && bin/ledger-server --db ledger.db  # a Go binary with the UI embedded
make image && docker run --rm -p 8700:8700 ledger-trace   # the hosted read-only demo
docker compose -f deploy/docker-compose.yml up -d ui      # beside the Compose jobs, on their store
```

Both only read the store, so they can run beside a process that is recording. The Docker image serves a demo store (`scripts/make_demo_db.py`: recordings, a replay, an edited fork, a diagnosed failure and a crashed-then-resumed refund) with `--immutable`, which needs no write access at all. `tests/conformance.py` sends both servers the same requests over one store and checks they answer identically.

To work on the UI (`ui/`, SvelteKit with Svelte 5; Node 22.17 or newer, `ui/.nvmrc` says 24), run a server on port 8700, then `npm run dev` in `ui/`, which forwards API calls to it. `make ui` builds the UI into both servers, `make test` runs the Python, Go and UI tests, and `scripts/gen_proto.sh` regenerates the Python, Go and TypeScript code from `proto/`.
