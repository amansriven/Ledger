from __future__ import annotations

import sqlite3

import pytest
from openai import OpenAI

import ledger
from ledger.core.events import body_kind, load_payload, pb
from ledger.core.normalize import DEFAULT, LLM_DEFAULT


@pytest.mark.parametrize(
    "text",
    [
        "at 2026-11-12T10:32:01Z",
        "at 2026-11-12T10:32:01.123456+05:30",
        "at 2026-11-12 10:32",
        "at 2026-11-12T10:32:01-0800",
    ],
)
def test_timestamps_are_masked(text):
    assert DEFAULT.normalize(text) == "at <timestamp>"


def test_ids_are_masked_but_dates_and_other_hex_are_not():
    u = "3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90"
    run_id = "0123456789ab4def8123456789abcdef"  # uuid4().hex shape
    md5 = "d41d8cd98f00b204e9800998ecf8427e"  # 32 hex, not a UUID4
    assert DEFAULT.normalize(f"run {u} / {run_id}") == "run <uuid> / <uuid>"
    assert DEFAULT.normalize(f"report for 2026-11-12, file {md5}") == (
        f"report for 2026-11-12, file {md5}"
    )


def test_strings_are_masked_anywhere_and_keys_drop_only_at_top_level():
    req = {
        "model": "m",
        "timeout": 30,
        "messages": [{"role": "user", "content": "now is 2026-11-12T10:00:00Z"}],
        "extra_body": {"timeout": 5},
    }
    assert LLM_DEFAULT.normalize(req) == {
        "model": "m",
        "messages": [{"role": "user", "content": "now is <timestamp>"}],
        "extra_body": {"timeout": 5},
    }


def test_key_ignores_volatile_parts_only():
    a = {"messages": [{"content": "hi at 2026-11-12T10:00:00Z"}], "model": "m"}
    b = {"model": "m", "messages": [{"content": "hi at 2026-11-13T09:15:42Z"}]}
    c = {"model": "m", "messages": [{"content": "bye at 2026-11-12T10:00:00Z"}]}
    assert DEFAULT.key(a) == DEFAULT.key(b)
    assert DEFAULT.key(a) != DEFAULT.key(c)
    assert ledger.EXACT.key(a) != ledger.EXACT.key(b)


def test_extend_adds_rules():
    n = DEFAULT.extend(patterns=[(r"ticket-\d+", "<ticket>")], ignore_keys=["seed"])
    assert n.normalize({"seed": 1, "q": "ticket-42"}) == {"q": "<ticket>"}


def _ask(llm, when: str, **kwargs):
    return llm.chat.completions.create(
        model="m", messages=[{"role": "user", "content": f"summarize as of {when}"}], **kwargs
    )


def test_replay_matches_a_request_whose_timestamp_moved(fake_server, store, request):
    llm = ledger.wrap(OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))
    with ledger.record(store) as rec:
        recorded = _ask(llm, "2026-11-12T10:00:00Z")
    request.getfixturevalue("network_off")
    with ledger.replay(rec.run_id, store) as rep:
        replayed = _ask(llm, "2026-11-13T08:30:00Z", timeout=5)
    assert replayed.choices[0].message.content == recorded.choices[0].message.content
    assert rep.stats.live_calls == 0

    # The replay's log keeps the request as it was actually made.
    (rec_call,) = [e for e in store.events(rec.run_id) if body_kind(e) == "llm_call"]
    (rep_call,) = [e for e in store.events(rep.run_id) if body_kind(e) == "llm_call"]
    assert rep_call.llm_call.request_key == rec_call.llm_call.request_key
    assert "2026-11-13" in load_payload(rep_call.llm_call.request)["messages"][0]["content"]
    assert rep_call.llm_call.response.sha256 == rec_call.llm_call.response.sha256


def test_exact_normalizer_treats_a_moved_timestamp_as_a_change(fake_server, store):
    llm = ledger.wrap(
        OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0),
        normalizer=ledger.EXACT,
    )
    with ledger.record(store) as rec:
        _ask(llm, "2026-11-12T10:00:00Z")
    with pytest.raises(ledger.ReplayMismatch, match="changed"):
        with ledger.replay(rec.run_id, store):
            _ask(llm, "2026-11-13T08:30:00Z")


def test_tool_args_are_normalized_too(store):
    @ledger.tool(name="lookup")
    def lookup(q: str) -> str:
        return q.upper()

    with ledger.record(store) as rec:
        lookup("orders since 2026-11-12T00:00:00Z")
    with ledger.replay(rec.run_id, store) as rep:
        assert lookup("orders since 2026-11-13T00:00:00Z") == "ORDERS SINCE 2026-11-12T00:00:00Z"
    assert rep.stats.replayed_calls == 1


def _strip_match_keys(store: ledger.EventStore, run_id: str) -> None:
    """Rewrite a run as an M0 log: no request_key / args_key."""
    with sqlite3.connect(store.path) as c:
        rows = c.execute("SELECT idx, data FROM events WHERE run_id = ?", (run_id,)).fetchall()
        for idx, data in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            if body_kind(ev) == "llm_call":
                ev.llm_call.ClearField("request_key")
            c.execute(
                "UPDATE events SET data = ? WHERE run_id = ? AND idx = ?",
                (ev.SerializeToString(), run_id, idx),
            )


def test_m0_logs_without_match_keys_compare_raw_requests(fake_server, store):
    llm = ledger.wrap(OpenAI(base_url=f"{fake_server.base_url}/v1", api_key="x", max_retries=0))
    with ledger.record(store) as rec:
        _ask(llm, "2026-11-12T10:00:00Z")
    _strip_match_keys(store, rec.run_id)

    with ledger.replay(rec.run_id, store) as rep:
        _ask(llm, "2026-11-12T10:00:00Z")
    assert rep.stats.replayed_calls == 1
    with pytest.raises(ledger.ReplayMismatch):
        with ledger.replay(rec.run_id, store):
            _ask(llm, "2026-11-13T08:30:00Z")
