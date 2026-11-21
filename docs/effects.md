# Effects, sagas and resume

How Ledger makes side effects happen once per run step, undoes them when a run fails for good, and picks crashed runs back up.

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

`ledger/examples/refund_agent.py` turns a support ticket into a Stripe refund: `lookup` reads the charge (a tool), and `decide` applies the refund policy in code, leaving a charge another ticket has claimed to that ticket. `refund` first claims the charge for this ticket in its Stripe metadata (an effect whose compensation releases the claim), then creates the refund (an effect whose `reconcile` finds a refund by the key in its metadata). Last, `reply` has the LLM draft the answer. If Stripe refuses the refund, the saga releases the claim, so another ticket or a person can take over. It runs with the official `stripe` SDK against `ledger.testing.fake_stripe`, a fake Stripe API that honours idempotency keys and can drop a connection before or after applying a request, or answer 500. To point it at Stripe itself, set `STRIPE_API_BASE=` (empty) and put a test key in `STRIPE_SECRET_KEY`, in the environment or in `.env` at the repo root (git ignores it). Only that one variable is read from `.env`, live keys are refused, and the fake server never gets the real key.

```sh
uv run python -m ledger.testing.fake_stripe --port 12111 &
uv run python -m ledger.testing.fake_llm --port 8787 &
uv run python -m ledger.examples.refund_agent          # seeds a charge, records one refund
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
