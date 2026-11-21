"""Who&When: failure logs of LLM multi-agent systems, with the cause labelled.

184 failed runs (arXiv 2505.00212): 126 from systems CaptainAgent generated
and 58 from hand-crafted ones such as Magentic-One, each labelled with the
agent responsible and the decisive step, the earliest step whose correction
would have made the run succeed. The logs are recordings, not runnable
systems, so counterfactual blame can't run on them; this is where log-only
methods are compared.

    python -m bench.who_and_when download     # 184 JSON logs, about 7 MB
    python -m bench.who_and_when stats
    python -m bench.who_and_when run [--methods random,judge] [--subset hand-crafted]
        [--judge-model M --judge-base-url URL --judge-price IN OUT]
        [--with-ground-truth] [--limit N] [--max-dollars D]

The data lives in ``bench/data/who_and_when/`` (gitignored), one JSON file
per log under ``Algorithm-Generated/`` and ``Hand-Crafted/``, from the
Hugging Face dataset ``Kevin355/Who_and_When`` at the revision in
``REVISION``.

The two halves are formatted differently, and ``load`` evens them out:

* the speaker of a message is its ``name`` (algorithm-generated) or its
  ``role`` (hand-crafted);
* the hand-crafted Orchestrator speaks as "Orchestrator (thought)",
  "Orchestrator (-> WebSurfer)" and so on, while labels say "Orchestrator";
* labels and speakers differ in case and underscores ("Websurfer",
  "VerificationExpert").

So agents are compared by ``agent_key``: the name without a parenthesized
suffix, lowercased, letters and digits only. Steps are 0-based indices into
the history, as in the labels.

Methods:

random
    The expected accuracy of picking an agent of the log (not the human who
    set the task) and a step of the log uniformly at random, computed exactly
    rather than sampled.
judge
    An LLM given the question (and with ``--with-ground-truth`` the right
    answer) and the whole log, naming the agent and step, as in Who&When's
    all-at-once method. Needs an OpenAI-compatible model; the key comes from
    ``LEDGER_JUDGE_API_KEY`` or ``OPENAI_API_KEY``, in the environment or
    ``.env``. ``--max-chars`` cuts long messages; ``--max-dollars`` stops
    the run once the judge has spent that much.

Writes ``bench/results/who_and_when.json`` (or ``--out``) and prints a table.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ledger.blame import Pricing, wilson

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "bench" / "data" / "who_and_when"
RESULTS = ROOT / "bench" / "results"
SUBSETS = {"algorithm-generated": "Algorithm-Generated", "hand-crafted": "Hand-Crafted"}
DATASET = "Kevin355/Who_and_When"
REVISION = "59b9fcba1aaed7bbf206b5f4d3c68b8face2f49c"
HF = "https://huggingface.co"
METHODS = ("random", "judge")
CHARS_PER_TOKEN = 4  # rough, for cost estimates only

_SUFFIX = re.compile(r"\s*\(.*\)\s*$")
# Speakers that are not agents: the hand-crafted logs open with the human's task.
NOT_AGENTS = {"human"}


def agent_key(name: str) -> str:
    """The form agents are compared in: "Orchestrator (thought)" -> "orchestrator"."""
    return re.sub(r"[^0-9a-z]", "", _SUFFIX.sub("", name or "").casefold())


@dataclass(frozen=True)
class Message:
    index: int
    speaker: str  # as logged, e.g. "Orchestrator (-> WebSurfer)"
    content: str

    @property
    def agent(self) -> str:
        return agent_key(self.speaker)


@dataclass(frozen=True)
class Case:
    id: str  # "algorithm-generated/12"
    subset: str
    question: str
    answer: str  # the task's right answer
    messages: tuple[Message, ...]
    mistake_agent: str  # as labelled
    mistake_step: int
    mistake_reason: str

    @property
    def agents(self) -> list[str]:
        """Distinct agents (by ``agent_key``) that speak in the log, in order;
        the human who set the task is not one."""
        return list(dict.fromkeys(m.agent for m in self.messages if m.agent not in NOT_AGENTS))

    @property
    def label_matches_speaker(self) -> bool:
        """Whether the labelled agent is the one speaking at the labelled step."""
        return self.messages[self.mistake_step].agent == agent_key(self.mistake_agent)

    @property
    def chars(self) -> int:
        return sum(len(m.content) for m in self.messages)


def _case(subset: str, path: Path) -> Case:
    d = json.loads(path.read_text())
    field_name = "name" if subset == "algorithm-generated" else "role"
    messages = tuple(
        Message(i, str(m.get(field_name) or m.get("role") or ""), str(m.get("content") or ""))
        for i, m in enumerate(d["history"])
    )
    step = int(d["mistake_step"])
    if not 0 <= step < len(messages):
        raise ValueError(f"{path}: mistake_step {step} is outside its {len(messages)} messages")
    return Case(
        id=f"{subset}/{path.stem}", subset=subset, question=str(d.get("question") or ""),
        answer=str(d.get("ground_truth", d.get("groundtruth", ""))),
        messages=messages, mistake_agent=str(d["mistake_agent"]), mistake_step=step,
        mistake_reason=str(d.get("mistake_reason") or ""),
    )


def load(root: Path = DATA, subsets: Iterable[str] = SUBSETS) -> list[Case]:
    """Every log of the chosen subsets, in file-number order."""
    cases = []
    for subset in subsets:
        folder = root / SUBSETS[subset]
        if not folder.is_dir():
            raise FileNotFoundError(
                f"no Who&When data at {folder}; download it first (see the README)"
            )
        files = sorted(folder.glob("*.json"), key=lambda p: (len(p.stem), p.stem))
        cases.extend(_case(subset, f) for f in files)
    return cases


def download(root: Path = DATA, revision: str = REVISION) -> int:
    """Fetch the JSON logs and the dataset card at ``revision`` into ``root``.

    Refuses to write into a folder that already has files. Returns the number
    of logs fetched.
    """
    from urllib.parse import quote
    from urllib.request import urlopen

    if root.exists() and any(root.iterdir()):
        raise FileExistsError(f"{root} is not empty; remove it to download again")

    def get(url: str) -> bytes:
        with urlopen(url, timeout=60) as resp:  # noqa: S310 (fixed https host)
            return resp.read()

    tree = json.loads(get(f"{HF}/api/datasets/{DATASET}/tree/{revision}?recursive=true"))
    files = [e["path"] for e in tree
             if e.get("type") == "file" and e["path"].startswith("Who&When/")
             and e["path"].endswith(".json")]
    root.mkdir(parents=True, exist_ok=True)
    for name in ["README.md", *files]:
        rel = Path(name.removeprefix("Who&When/"))
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"unexpected path in the dataset: {name!r}")
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(get(f"{HF}/datasets/{DATASET}/resolve/{revision}/{quote(name)}"))
    (root / "REVISION").write_text(revision + "\n")
    return len(files)


# Methods


def random_expectation(case: Case) -> tuple[float, float]:
    """(agent, step) accuracy of a uniform random pick, in expectation."""
    agents = case.agents
    agent = 1 / len(agents) if agent_key(case.mistake_agent) in agents else 0.0
    return agent, 1 / len(case.messages)


JUDGE_PROMPT = """A multi-agent system tried to solve the task below and failed.
Find the agent responsible and the decisive error step: the earliest step
that, had it been done correctly, would have let the run succeed.

Task: {question}
{answer}
The conversation, one numbered step per message:

{log}

Answer with JSON only: {{"agent": "<agent name>", "step": <step number>, "reason": "<one sentence>"}}"""


def judge_prompt(case: Case, *, with_ground_truth: bool, max_chars: int | None) -> str:
    def text(m: Message) -> str:
        c = m.content
        if max_chars is not None and len(c) > max_chars:
            c = c[:max_chars] + f" [... {len(m.content) - max_chars} more characters]"
        return f"Step {m.index} - {m.speaker}:\n{c}"

    answer = f"The right answer is: {case.answer}\n" if with_ground_truth else ""
    return JUDGE_PROMPT.format(question=case.question, answer=answer,
                               log="\n\n".join(text(m) for m in case.messages))


def parse_answer(text: str, case: Case) -> tuple[str | None, int | None]:
    """(agent key, step) from the judge's reply; None for what it didn't give."""
    m = re.search(r"\{.*\}", text or "", re.S)
    try:
        answer = json.loads(m.group(0)) if m else {}
    except json.JSONDecodeError:
        answer = {}
    if not isinstance(answer, dict):
        answer = {}
    agent = answer.get("agent")
    step = answer.get("step")
    if isinstance(step, str) and step.strip().isdigit():
        step = int(step)
    if not isinstance(step, int) or isinstance(step, bool) or not 0 <= step < len(case.messages):
        step = None
    return (agent_key(agent) if isinstance(agent, str) and agent else None), step


@dataclass
class Judge:
    client: Any
    model: str
    pricing: Pricing | None = None
    with_ground_truth: bool = False
    max_chars: int | None = None
    spent: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0

    def __call__(self, case: Case) -> tuple[str | None, int | None]:
        prompt = judge_prompt(case, with_ground_truth=self.with_ground_truth,
                              max_chars=self.max_chars)
        resp = self.client.chat.completions.create(
            model=self.model, temperature=0, messages=[{"role": "user", "content": prompt}],
        )
        if resp.usage:
            self.tokens_in += resp.usage.prompt_tokens or 0
            self.tokens_out += resp.usage.completion_tokens or 0
            if self.pricing:
                self.spent += self.pricing.dollars(resp.usage.prompt_tokens or 0,
                                                   resp.usage.completion_tokens or 0)
        return parse_answer(resp.choices[0].message.content or "", case)


# Scoring


@dataclass
class Score:
    agent: float = 0.0  # summed accuracy (fractional for random's expectation)
    step: float = 0.0
    n: int = 0
    unanswered: int = 0
    by_case: list[dict[str, Any]] = field(default_factory=list)

    def add(self, case: Case, agent_ok: float, step_ok: float, **extra: Any) -> None:
        self.n += 1
        self.agent += agent_ok
        self.step += step_ok
        if extra:
            self.by_case.append({"id": case.id, **extra})

    def summary(self) -> dict[str, Any]:
        def acc(total: float) -> dict[str, Any]:
            rate = total / self.n if self.n else 0.0
            lo, hi = wilson(round(total), self.n)
            return {"rate": round(rate, 4), "ci95": [round(lo, 3), round(hi, 3)]}

        out = {"cases": self.n, "agent_accuracy": acc(self.agent),
               "step_accuracy": acc(self.step)}
        if self.unanswered:
            out["unanswered"] = self.unanswered
        return out


def score(cases: list[Case], methods: Iterable[str], judge: Judge | None = None,
          max_dollars: float | None = None) -> dict[str, dict[str, Score]]:
    """Scores per method and subset ("all" for both together)."""
    out: dict[str, dict[str, Score]] = {}
    for method in methods:
        scores = {s: Score() for s in ["all", *sorted({c.subset for c in cases})]}
        for case in cases:
            if method == "random":
                a, s = random_expectation(case)
                extra: dict[str, Any] = {}
            elif method == "judge":
                if judge is None:
                    raise ValueError("the judge method needs a judge")
                if max_dollars is not None and judge.spent >= max_dollars:
                    break
                agent, step = judge(case)
                a = float(agent == agent_key(case.mistake_agent))
                s = float(step == case.mistake_step)
                extra = {"agent": agent, "step": step, "truth": [case.mistake_agent,
                                                                case.mistake_step]}
                if agent is None and step is None:
                    for sc in (scores["all"], scores[case.subset]):
                        sc.unanswered += 1
            else:
                raise ValueError(f"unknown method {method!r}")
            for key in ("all", case.subset):
                scores[key].add(case, a, s, **extra)
        out[method] = scores
    return out


def stats(cases: list[Case]) -> dict[str, Any]:
    out = {}
    for subset in sorted({c.subset for c in cases}):
        cs = [c for c in cases if c.subset == subset]
        steps = sorted(len(c.messages) for c in cs)
        out[subset] = {
            "cases": len(cs),
            "steps": {"min": steps[0], "p50": statistics.median(steps), "max": steps[-1]},
            "agents_per_log": round(statistics.fmean(len(c.agents) for c in cs), 2),
            "label_matches_speaker": sum(c.label_matches_speaker for c in cs),
            "decisive_step_is_first": sum(c.mistake_step == 0 for c in cs),
            "est_tokens_full_logs": sum(c.chars for c in cs) // CHARS_PER_TOKEN,
        }
    return out


def render(rep: dict[str, Any]) -> str:
    lines = [f"Who&When: {rep['cases']} logs" + (", judge given the right answer"
                                                 if rep.get("with_ground_truth") else ""),
             f"{'method':<8}{'subset':<22}{'agent acc (95% CI)':>24}{'step acc (95% CI)':>24}"]
    for method, subsets in rep["methods"].items():
        for subset, s in subsets.items():
            a, st = s["agent_accuracy"], s["step_accuracy"]
            lines.append(
                f"{method:<8}{subset:<22}"
                f"{a['rate']:>9.3f} ({a['ci95'][0]:.2f}-{a['ci95'][1]:.2f}){'':>3}"
                f"{st['rate']:>9.3f} ({st['ci95'][0]:.2f}-{st['ci95'][1]:.2f})"
            )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.who_and_when", description=__doc__.splitlines()[0])
    p.add_argument("--data", type=Path, default=DATA)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download", help=f"fetch the logs from Hugging Face ({DATASET})")
    sub.add_parser("stats", help="what the logs hold, and what a full judge pass would read")
    run = sub.add_parser("run", help="score log-only methods")
    run.add_argument("--methods", default="random,judge")
    run.add_argument("--subset", choices=sorted(SUBSETS), action="append")
    run.add_argument("--limit", type=int, help="first N logs of each subset")
    run.add_argument("--with-ground-truth", action="store_true")
    run.add_argument("--max-chars", type=int, help="cut messages longer than this for the judge")
    run.add_argument("--max-dollars", type=float, help="stop the judge once it has spent this")
    run.add_argument("--judge-model")
    run.add_argument("--judge-base-url")
    run.add_argument("--judge-price", type=float, nargs=2, metavar=("IN", "OUT"),
                     help="judge dollars per million input and output tokens")
    run.add_argument("--out", type=Path)
    args = p.parse_args(argv)

    if args.cmd == "download":
        n = download(args.data)
        print(f"fetched {n} logs into {args.data}")
        return 0
    subsets = (args.subset if getattr(args, "subset", None) else list(SUBSETS))
    cases = load(args.data, subsets)
    if args.cmd == "stats":
        print(json.dumps(stats(cases), indent=1))
        return 0

    if args.limit:
        cases = [c for s in subsets for c in [x for x in cases if x.subset == s][: args.limit]]
    methods = [m for m in args.methods.split(",") if m]
    judge = None
    if "judge" in methods:
        if not args.judge_model:
            methods.remove("judge")
            print("judge: not run (no --judge-model)", file=sys.stderr)
        else:
            from openai import OpenAI

            from bench.blame_eval import _judge_key

            judge = Judge(
                OpenAI(base_url=args.judge_base_url, api_key=_judge_key() or "none",
                       max_retries=2),
                args.judge_model,
                Pricing(*args.judge_price) if args.judge_price else None,
                with_ground_truth=args.with_ground_truth, max_chars=args.max_chars,
            )
    started = time.monotonic()
    scores = score(cases, methods, judge, args.max_dollars)
    rep: dict[str, Any] = {
        "benchmark": "who_and_when",
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "revision": (args.data / "REVISION").read_text().strip()
        if (args.data / "REVISION").exists() else None,
        "cases": len(cases),
        "with_ground_truth": args.with_ground_truth,
        "max_chars": args.max_chars,
        "judge": {"model": args.judge_model, "base_url": args.judge_base_url,
                  "dollars": round(judge.spent, 4), "tokens_in": judge.tokens_in,
                  "tokens_out": judge.tokens_out} if judge else None,
        "wall_s": round(time.monotonic() - started, 1),
        "methods": {m: {s: sc.summary() for s, sc in subs.items()}
                    for m, subs in scores.items()},
        "answers": {m: subs["all"].by_case for m, subs in scores.items()
                    if subs["all"].by_case},
    }
    out = args.out or RESULTS / "who_and_when.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1) + "\n")
    print(render(rep))
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
