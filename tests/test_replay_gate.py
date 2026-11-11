"""M0 gate: recorded runs replay with zero live API calls, network off."""

from __future__ import annotations

import ledger
from examples.toy_pipeline import recorded_runs
from ledger.core.events import body_kind, load_payload

N_RUNS = 10
TOPICS = ["vector clocks", "fencing tokens", "delta debugging"]


def _record(graph, store, n=N_RUNS) -> dict[str, dict]:
    outputs = {}
    for i in range(n):
        with ledger.record(store) as session:
            outputs[session.run_id] = graph.invoke({"topic": TOPICS[i % len(TOPICS)]})
        assert session.stats.live_calls == 8
    return outputs


def _call_signature(store, run_id):
    """(step key, kind, request digest, response digest) for every call, by step."""
    sig = []
    for ev in store.events(run_id):
        kind = body_kind(ev)
        if kind == "llm_call":
            sig.append((ev.step.SerializeToString(), kind, ev.llm_call.request.sha256, ev.llm_call.response.sha256))
        elif kind == "tool_call":
            sig.append((ev.step.SerializeToString(), kind, ev.tool_call.args.sha256, ev.tool_call.result.sha256))
    return sorted(sig)


def test_replay_with_network_off_makes_zero_live_calls(fake_server, toy_graph, store, request):
    recorded = _record(toy_graph, store)
    assert fake_server.stats() == {"chat": 5 * N_RUNS, "search": 3 * N_RUNS}

    # Take the server down and block every socket before replaying.
    fake_server.stop()
    request.getfixturevalue("network_off")

    assert recorded_runs(store) == list(recorded)
    for run_id, output in recorded.items():
        with ledger.replay(run_id, store) as session:
            replayed = toy_graph.invoke(load_payload(session.replay_index.input))
        assert replayed == output
        assert session.output_sha256 == session.replay_index.output_sha256
        assert session.stats.live_calls == 0
        assert session.stats.replayed_calls == 8
        # The replay's own log carries the same calls as the recording.
        assert _call_signature(store, session.run_id) == _call_signature(store, run_id)

    assert fake_server.stats() == {"chat": 5 * N_RUNS, "search": 3 * N_RUNS}


def test_parallel_branches_finish_out_of_order_but_state_is_stable(toy_graph, store):
    """Branches complete in varying order, yet LangGraph applies their writes by task path."""
    recorded = _record(toy_graph, store, n=20)
    completion_orders = set()
    for run_id in recorded:
        order = tuple(
            ev.step.node
            for ev in store.events(run_id)
            if body_kind(ev) == "llm_call" and ev.step.node.startswith("research_")
        )
        completion_orders.add(order)
    assert len(completion_orders) > 1, "fake latency should reorder branch completion"

    note_orders = {tuple(n.split(":")[0] for n in out["notes"]) for out in recorded.values()}
    assert note_orders == {("code", "docs", "web")}
