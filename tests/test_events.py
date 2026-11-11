from openai.types.chat import ChatCompletionMessage

from ledger.core.events import canonical_json, load_payload, make_payload


def test_canonical_json_ignores_key_order():
    a = {"b": 1, "a": {"y": [1, 2], "x": "é"}}
    b = {"a": {"x": "é", "y": [1, 2]}, "b": 1}
    assert canonical_json(a) == canonical_json(b)
    assert canonical_json(a) == '{"a":{"x":"é","y":[1,2]},"b":1}'.encode()


def test_payload_digest_matches_content():
    p1, p2 = make_payload({"k": 1}), make_payload({"k": 2})
    assert p1.sha256 != p2.sha256
    assert make_payload({"k": 1}).sha256 == p1.sha256
    assert load_payload(p1) == {"k": 1}


def test_pydantic_models_are_logged_as_json():
    msg = ChatCompletionMessage(role="assistant", content="hi")
    assert load_payload(make_payload({"m": msg}))["m"]["content"] == "hi"
