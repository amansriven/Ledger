"""The Who&When loader and the log-only methods, on small fixtures and the real data."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from bench import who_and_when as ww
from ledger.blame import Pricing


def write(root, subset, n, payload):
    folder = root / ww.SUBSETS[subset]
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{n}.json").write_text(json.dumps(payload))


@pytest.fixture
def data(tmp_path):
    root = tmp_path / "wnw"
    write(root, "algorithm-generated", 1, {
        "question": "How many clients?", "ground_truth": "8", "is_correct": False,
        "mistake_agent": "Excel_Expert", "mistake_step": "1", "mistake_reason": "bad parse",
        "history": [
            {"name": "Planner_Expert", "role": "user", "content": "plan it"},
            {"name": "Excel_Expert", "role": "user", "content": "count = 7"},
            {"name": "Computer_terminal", "role": "user", "content": "7"},
            {"name": "VerificationExpert", "role": "user", "content": "looks right"},
        ],
    })
    write(root, "algorithm-generated", 10, {
        "question": "q2", "ground_truth": "a2", "mistake_agent": "Verification_Expert",
        "mistake_step": "0", "mistake_reason": "r",
        "history": [{"name": "VerificationExpert", "content": "x"},
                    {"name": "Computer_terminal", "content": "y"}],
    })
    write(root, "hand-crafted", 2, {
        "question": "Find the paper", "groundtruth": "1999", "is_corrected": False,
        "mistake_agent": "Websurfer", "mistake_step": "3", "mistake_reason": "wrong page",
        "history": [
            {"role": "human", "content": "Find the paper"},
            {"role": "Orchestrator (thought)", "content": "search for it"},
            {"role": "Orchestrator (-> WebSurfer)", "content": "please search"},
            {"role": "WebSurfer", "content": "found 2001 " + "z" * 50},
            {"role": "Orchestrator (termination condition)", "content": "done"},
        ],
    })
    return root


def test_agent_names_compare_without_suffixes_case_or_underscores():
    assert ww.agent_key("Orchestrator (-> WebSurfer)") == "orchestrator"
    assert ww.agent_key("Orchestrator (thought)") == ww.agent_key("orchestrator")
    assert ww.agent_key("Verification_Expert") == ww.agent_key("VerificationExpert")
    assert ww.agent_key("WebSurfer") == ww.agent_key("Websurfer")
    assert ww.agent_key("") == ""


def test_both_formats_load_the_same_way(data):
    cases = ww.load(data)
    assert [c.id for c in cases] == ["algorithm-generated/1", "algorithm-generated/10",
                                     "hand-crafted/2"]
    ag, ag10, hc = cases
    assert (ag.answer, hc.answer) == ("8", "1999")
    assert ag.messages[1].speaker == "Excel_Expert" and ag.mistake_step == 1
    assert hc.messages[2].speaker == "Orchestrator (-> WebSurfer)"
    assert hc.agents == ["orchestrator", "websurfer"]  # the human is not an agent
    assert all(c.label_matches_speaker for c in cases)
    assert ww.load(data, ["hand-crafted"]) == [hc]


def test_bad_or_missing_data(tmp_path, data):
    with pytest.raises(FileNotFoundError, match="download"):
        ww.load(tmp_path / "nowhere")
    write(data, "hand-crafted", 3, {"mistake_agent": "X", "mistake_step": "9",
                                    "history": [{"role": "X", "content": ""}]})
    with pytest.raises(ValueError, match="outside"):
        ww.load(data)


def test_random_is_scored_exactly(data):
    cases = ww.load(data)
    assert ww.random_expectation(cases[0]) == (1 / 4, 1 / 4)
    assert ww.random_expectation(cases[2]) == (1 / 2, 1 / 5)
    s = ww.score(cases, ["random"])["random"]
    assert s["all"].n == 3 and s["hand-crafted"].n == 1
    assert s["all"].agent == pytest.approx(1 / 4 + 1 / 2 + 1 / 2)


def test_judge_answers_are_parsed_leniently(data):
    case = ww.load(data)[2]
    assert ww.parse_answer('Sure: {"agent": "WebSurfer", "step": 3}', case) == ("websurfer", 3)
    assert ww.parse_answer('{"agent": "Orchestrator (thought)", "step": "1"}', case) == (
        "orchestrator", 1)
    assert ww.parse_answer('{"agent": "WebSurfer", "step": 99}', case) == ("websurfer", None)
    assert ww.parse_answer('{"step": true}', case) == (None, None)
    assert ww.parse_answer("I think it was the web surfer.", case) == (None, None)
    assert ww.parse_answer("[1, 2]", case) == (None, None)


class StubClient:
    """Answers like a judge that always names the labelled step."""

    def __init__(self, cases, wrong=()):
        self.by_question = {c.question: c for c in cases}
        self.wrong = set(wrong)
        self.prompts = []
        self.chat = SimpleNamespace(completions=self)

    def create(self, *, model, messages, **options):
        prompt = messages[0]["content"]
        self.prompts.append(prompt)
        self.options = options
        case = next(c for q, c in self.by_question.items() if f"Task: {q}\n" in prompt)
        step = case.mistake_step + (1 if case.id in self.wrong else 0)
        text = json.dumps({"agent": case.mistake_agent, "step": step, "reason": "r"})
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
            usage=SimpleNamespace(prompt_tokens=1000, completion_tokens=50),
        )


def test_the_judge_reads_the_whole_log_and_is_scored(data):
    cases = ww.load(data)
    client = StubClient(cases, wrong={"hand-crafted/2"})
    judge = ww.Judge(client, "m", Pricing(1.0, 2.0), with_ground_truth=True, max_chars=20)
    s = ww.score(cases, ["judge"], judge)["judge"]
    assert client.options == {}  # no temperature unless asked: reasoning models reject it
    assert (s["all"].agent, s["all"].step) == (3, 2)
    assert s["hand-crafted"].summary()["step_accuracy"]["rate"] == 0.0
    assert judge.spent == pytest.approx(3 * (1000 + 2 * 50) / 1e6)
    hc_prompt = client.prompts[2]
    assert "Step 2 - Orchestrator (-> WebSurfer):" in hc_prompt
    assert "The right answer is: 1999" in hc_prompt
    assert "[... 41 more characters]" in hc_prompt  # cut at max_chars
    assert s["all"].by_case[2] == {"id": "hand-crafted/2", "agent": "websurfer", "step": 4,
                                   "truth": ["Websurfer", 3]}
    without = ww.judge_prompt(cases[2], with_ground_truth=False, max_chars=None)
    assert "right answer" not in without and "z" * 50 in without


def test_the_judge_stops_at_its_dollar_cap(data):
    cases = ww.load(data)
    judge = ww.Judge(StubClient(cases), "m", Pricing(1000.0, 0.0))  # $1 a call
    s = ww.score(cases, ["judge"], judge, max_dollars=1.5)["judge"]
    assert s["all"].n == 2 and judge.spent == pytest.approx(2.0)


def test_main_writes_a_report(data, tmp_path, capsys):
    out = tmp_path / "r.json"
    assert ww.main(["--data", str(data), "run", "--methods", "random,judge",
                    "--out", str(out)]) == 0
    rep = json.loads(out.read_text())
    assert rep["cases"] == 3 and rep["judge"] is None
    assert set(rep["methods"]) == {"random"}
    assert set(rep["methods"]["random"]) == {"all", "algorithm-generated", "hand-crafted"}
    assert "judge: not run" in capsys.readouterr().err
    assert ww.main(["--data", str(data), "stats"]) == 0
    st = json.loads(capsys.readouterr().out)
    assert st["hand-crafted"]["label_matches_speaker"] == 1
    assert ww.main(["--data", str(data), "run", "--limit", "1", "--subset", "algorithm-generated",
                    "--methods", "random", "--out", str(out)]) == 0
    assert json.loads(out.read_text())["cases"] == 1


@pytest.mark.skipif(not (ww.DATA / "Hand-Crafted").is_dir(), reason="Who&When not downloaded")
def test_the_downloaded_data():
    cases = ww.load()
    st = ww.stats(cases)
    assert {k: v["cases"] for k, v in st.items()} == {"algorithm-generated": 126,
                                                     "hand-crafted": 58}
    # With names compared by agent_key, the speaker at the labelled step is the
    # labelled agent in all but 6 logs.
    assert sum(v["label_matches_speaker"] for v in st.values()) == 178
    assert all(c.agents for c in cases)


def test_download_fetches_the_pinned_logs(tmp_path, monkeypatch):
    import io
    import urllib.request

    tree = [
        {"type": "file", "path": "README.md"},
        {"type": "file", "path": "Hand-Crafted.parquet"},
        {"type": "directory", "path": "Who&When/Hand-Crafted"},
        {"type": "file", "path": "Who&When/Hand-Crafted/1.json"},
        {"type": "file", "path": "Who&When/Algorithm-Generated/7.json"},
    ]
    asked = []

    def urlopen(url, timeout):
        asked.append(url)
        body = json.dumps(tree).encode() if "/api/" in url else url.encode()
        return io.BytesIO(body)

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    root = tmp_path / "wnw"
    assert ww.download(root) == 2
    assert (root / "Hand-Crafted" / "1.json").read_text().endswith(
        f"/resolve/{ww.REVISION}/Who%26When/Hand-Crafted/1.json")
    assert (root / "REVISION").read_text().strip() == ww.REVISION
    assert not any("parquet" in u for u in asked)
    with pytest.raises(FileExistsError):
        ww.download(root)


def test_judge_options_are_sent_only_when_given():
    from bench.blame_eval import judge_options

    assert judge_options(None, None) == {}
    assert judge_options(0.0, "high") == {"temperature": 0.0, "reasoning_effort": "high"}


def test_a_failed_judge_call_counts_as_unanswered(data):
    cases = ww.load(data)

    class Flaky(StubClient):
        def create(self, **kw):
            if "Find the paper" in kw["messages"][0]["content"]:
                raise TimeoutError("read timed out")
            return super().create(**kw)

    s = ww.score(cases, ["judge"], ww.Judge(Flaky(cases), "m"))["judge"]
    assert s["all"].n == 3 and s["all"].unanswered == 1 and s["all"].step == 2
    assert s["all"].by_case[2]["error"] == "TimeoutError: read timed out"
