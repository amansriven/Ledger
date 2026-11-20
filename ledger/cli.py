"""The ``ledger`` command: record, replay and fork runs of a LangGraph graph.

    ledger record -g examples.toy_pipeline:graph_from_env --input '{"topic": "fencing tokens"}'
    ledger runs
    ledger replay RUN_ID                      # strict; exit 1 unless identical
    ledger replay --all --live                # report divergences instead of failing
    ledger steps RUN_ID                       # the calls a fork can target
    ledger fork RUN_ID --edit 12='a different summary' --rerun 20

``-g/--graph`` names the graph as ``module:attr``: a compiled graph, or a
function with no arguments that returns one. The current directory is on the
import path. ``record`` logs the reference with the run, so ``replay`` and
``fork`` find the graph again without ``-g``. Replays and forks take their
input from the recording.

The event store is ``--db``, else ``$LEDGER_DB``, else ``ledger.db``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from collections.abc import Sequence
from typing import Any

from ledger.core.events import body_kind, load_payload, make_error, pb
from ledger.core.session import Session, record
from ledger.core.store import EventStore
from ledger.intercept.langgraph import InstrumentedGraph, instrument
from ledger.replay.engine import RERUN, ReplayDivergence, ReplayMismatch, replay
from ledger.replay.fork import fork, steps

__all__ = ["main", "load_graph", "RunInfo", "list_runs"]

MODES = {
    pb.MODE_RECORD: "record",
    pb.MODE_REPLAY: "replay",
    pb.MODE_FORK: "fork",
}


class CLIError(Exception):
    """A problem to report in one line, exit status 2."""


# Graphs


def load_graph(ref: str) -> InstrumentedGraph:
    """Import ``module:attr`` and return it as an instrumented graph."""
    module_name, sep, attr = ref.partition(":")
    if not sep or not module_name or not attr:
        raise CLIError(f"--graph wants module:attr, not {ref!r}")
    if "" not in sys.path:
        sys.path.insert(0, "")
    try:
        obj: Any = importlib.import_module(module_name)
    except ImportError as exc:
        raise CLIError(f"cannot import {module_name!r}: {exc}") from exc
    for part in attr.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            raise CLIError(f"{module_name!r} has no attribute {attr!r}") from None
    if not hasattr(obj, "invoke") and callable(obj):
        obj = obj()
    if not hasattr(obj, "invoke"):
        raise CLIError(f"{ref} is not a graph or a function that returns one")
    return instrument(obj)


def _graph_for(args: argparse.Namespace, store: EventStore, source_run_id: str) -> tuple[str, Any]:
    ref = args.graph or _run_started(store, source_run_id).graph or _env_graph()
    if not ref:
        raise CLIError(
            f"run {source_run_id} does not say which graph ran (it was not "
            "recorded by the ledger CLI); pass -g module:attr"
        )
    return ref, load_graph(ref)


def _env_graph() -> str:
    return os.environ.get("LEDGER_GRAPH", "")


# Runs


def _is_call(event: pb.Event) -> bool:
    """An LLM call, tool call or effect outcome (not an effect's PENDING record)."""
    kind = body_kind(event)
    if kind == "effect":
        return event.effect.phase != pb.EffectCall.PHASE_PENDING
    return kind in ("llm_call", "tool_call")


class RunInfo:
    """One run in the store, as ``ledger runs`` shows it."""

    def __init__(self, run_id: str, events: list[pb.Event]) -> None:
        self.run_id = run_id
        self.events = len(events)
        first = events[0] if events else pb.Event()
        last = events[-1] if events else pb.Event()
        started = first.run_started if body_kind(first) == "run_started" else pb.RunStarted()
        self.mode = MODES.get(started.mode, "?")
        self.source = started.source_run_id
        self.graph = started.graph
        self.calls = sum(_is_call(e) for e in events)
        self.live = sum(_is_call(e) and not e.from_log for e in events)
        self.diverged = sum(body_kind(e) == "diverged" for e in events)
        if body_kind(last) != "run_finished":
            self.status = "unfinished"
        elif last.run_finished.HasField("error"):
            self.status = "error"
        else:
            self.status = "ok"

    def __str__(self) -> str:
        source = f" of {self.source}" if self.source else ""
        diverged = f" diverged={self.diverged}" if self.diverged else ""
        return (
            f"{self.run_id}  {self.mode}{source}  {self.status}  "
            f"calls={self.calls} live={self.live}{diverged}"
        )


def list_runs(store: EventStore) -> list[RunInfo]:
    return [RunInfo(r, list(store.events(r, hydrate=False))) for r in store.run_ids()]


def _run_started(store: EventStore, run_id: str) -> pb.RunStarted:
    first = next(iter(store.events(run_id)), None)
    if first is None:
        raise CLIError(f"no run {run_id!r} in {store.path}")
    if body_kind(first) != "run_started":
        raise CLIError(f"run {run_id} has no run_started event")
    return first.run_started


def _input_of(store: EventStore, run_id: str) -> Any:
    return load_payload(_run_started(store, run_id).input)


# Output


def _stats(session: Session) -> str:
    s = session.stats
    parts = [f"live={s.live_calls}", f"replayed={s.replayed_calls}"]
    if s.edited:
        parts.append(f"edited={s.edited}")
    return " ".join(parts)


def _show(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def _divergence_lines(session: Session) -> list[str]:
    return [f"    {p}" for p in session.divergences]


# Commands


def cmd_record(args: argparse.Namespace, store: EventStore) -> int:
    ref = args.graph or _env_graph()
    if not ref:
        raise CLIError("record needs -g module:attr (or $LEDGER_GRAPH)")
    graph = load_graph(ref)
    graph_input = _read_input(args)
    failures = 0
    for _ in range(args.runs):
        with record(store) as session:
            session.graph_ref = ref
            try:
                output = graph.invoke(graph_input)
            except Exception as exc:
                failures += 1
                print(f"FAIL {session.run_id}  {type(exc).__name__}: {exc}")
                continue
        print(f"recorded {session.run_id}  calls={session.stats.live_calls}")
        if args.show_output:
            print(_show(output))
    return 1 if failures else 0


def cmd_replay(args: argparse.Namespace, store: EventStore) -> int:
    if args.all:
        run_ids = [r.run_id for r in list_runs(store) if r.mode == "record"]
        if not run_ids:
            raise CLIError(f"no recorded runs in {store.path}")
    elif args.run_ids:
        run_ids = args.run_ids
    else:
        raise CLIError("give run ids or --all")
    on_divergence = "live" if args.live else "raise"
    identical = 0
    for run_id in run_ids:
        ref, graph = _graph_for(args, store, run_id)
        graph_input = _input_of(store, run_id)
        session: Session | None = None
        output: Any = None
        error: BaseException | None = None
        try:
            with replay(run_id, store, on_divergence=on_divergence) as session:
                session.graph_ref = ref
                try:
                    output = graph.invoke(graph_input)
                except (ReplayMismatch, ReplayDivergence):
                    raise
                except Exception as exc:
                    # Only a run that failed when recorded may fail on replay;
                    # anything else is reported below as a failed replay.
                    if session.replay_index.output_error is None:
                        raise
                    error = exc
        except Exception as exc:  # the replay itself failed; report and go on
            print(f"FAIL {run_id}  {type(exc).__name__}: {exc}")
            continue
        assert session is not None and session.replay_index is not None
        recorded_error = session.replay_index.output_error
        if recorded_error is not None or error is not None:
            # A run that failed replays identically if it fails the same way.
            got = make_error(error) if error is not None else None
            matched = got is not None and recorded_error is not None and (
                (got.type, got.message) == (recorded_error.type, recorded_error.message)
            )
            outcome = f"error={'match' if matched else 'MISMATCH'}"
            if error is not None:
                outcome += f" ({type(error).__name__}: {error})"
        else:
            matched = session.output_sha256 == session.replay_index.output_sha256
            outcome = f"output={'match' if matched else 'MISMATCH'}"
        ok = matched and session.stats.live_calls == 0 and not session.divergences
        identical += ok
        status = "OK  " if ok else "DIFF"
        print(f"{status} {run_id} -> {session.run_id}  {_stats(session)} {outcome}")
        for line in _divergence_lines(session):
            print(line)
        if args.show_output:
            print(_show(output))
    if len(run_ids) > 1:
        print(f"{identical}/{len(run_ids)} replayed identically")
    return 0 if identical == len(run_ids) else 1


def _parse_step(text: str) -> str | int:
    return int(text) if text.isdigit() else text


def _split_edit(text: str, flag: str) -> tuple[str, str]:
    step, sep, value = text.partition("=")
    if not sep or not step:
        raise CLIError(f"{flag} wants STEP=VALUE, not {text!r}")
    return step, value


def cmd_fork(args: argparse.Namespace, store: EventStore) -> int:
    edits: dict[str | int, Any] = {}

    def add(step: str, value: Any) -> None:
        key = _parse_step(step)
        if key in edits:
            raise CLIError(f"step {step} is given twice")
        edits[key] = value

    for text in args.edit:
        add(*_split_edit(text, "--edit"))
    for text in args.edit_json:
        step, raw = _split_edit(text, "--edit-json")
        try:
            add(step, json.loads(raw))
        except json.JSONDecodeError as exc:
            raise CLIError(f"--edit-json {step}: {exc}") from None
    for step in args.rerun:
        add(step, RERUN)
    if not edits:
        raise CLIError("fork needs at least one --edit, --edit-json or --rerun")

    ref, graph = _graph_for(args, store, args.run_id)
    graph_input = _input_of(store, args.run_id)
    running = False
    try:
        with fork(
            args.run_id, edits=edits, store=store,
            on_divergence="live" if args.live else "raise",
        ) as session:
            session.graph_ref = ref
            running = True
            output = graph.invoke(graph_input)
    except KeyError as exc:
        if running:  # raised by the graph, not by an unknown step
            print(f"FAIL fork of {args.run_id}  KeyError: {exc}")
            return 1
        raise CLIError(str(exc.args[0]) if exc.args else str(exc)) from None
    except Exception as exc:
        print(f"FAIL fork of {args.run_id}  {type(exc).__name__}: {exc}")
        return 1
    print(f"forked {args.run_id} -> {session.run_id}  {_stats(session)}")
    for line in _divergence_lines(session):
        print(line)
    if args.show_output:
        print(_show(output))
    return 0


def cmd_steps(args: argparse.Namespace, store: EventStore) -> int:
    _run_started(store, args.run_id)  # a clear error for an unknown run
    for s in steps(args.run_id, store):
        if args.node and s.node != args.node:
            continue
        mark = "" if not s.from_log else "  (from log)"
        print(f"{s}{mark}")
    return 0


def cmd_runs(args: argparse.Namespace, store: EventStore) -> int:
    for info in list_runs(store):
        if args.mode and info.mode != args.mode:
            continue
        print(info)
    return 0


# Arguments


def _read_input(args: argparse.Namespace) -> Any:
    if args.input is not None and args.input_file is not None:
        raise CLIError("give --input or --input-file, not both")
    if args.input_file is not None:
        if args.input_file == "-":
            text = sys.stdin.read()
        else:
            with open(args.input_file, encoding="utf-8") as f:
                text = f.read()
    elif args.input is not None:
        text = args.input
    else:
        raise CLIError("record needs the graph's input: --input JSON or --input-file PATH")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise CLIError(f"input is not JSON: {exc}") from None


def build_parser() -> argparse.ArgumentParser:
    db_help = "event store (default: $LEDGER_DB or ledger.db)"
    # --db works before or after the command. SUPPRESS keeps a command's
    # unset --db from overwriting one given before it.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help=db_help)
    graph = argparse.ArgumentParser(add_help=False)
    graph.add_argument(
        "-g", "--graph", default=None,
        help="module:attr of the graph, or of a function that returns it. "
        "Replay and fork default to the graph the run was recorded with, "
        "then $LEDGER_GRAPH; record defaults to $LEDGER_GRAPH",
    )
    output = argparse.ArgumentParser(add_help=False)
    output.add_argument("--show-output", action="store_true", help="print the graph's output")
    live = argparse.ArgumentParser(add_help=False)
    live.add_argument(
        "--live", action="store_true",
        help="run mismatching calls (and what is downstream of them) live "
        "instead of failing",
    )

    p = argparse.ArgumentParser(
        prog="ledger",
        description="Record, replay and fork LangGraph runs.",
    )
    p.add_argument("--db", default=None, help=db_help)
    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    rec = sub.add_parser(
        "record", parents=[common, graph, output], help="run the graph and record it"
    )
    rec.add_argument("--input", help="the graph's input, as JSON")
    rec.add_argument("--input-file", help="read the input JSON from a file ('-' for stdin)")
    rec.add_argument("--runs", type=int, default=1, help="record this many runs (default 1)")
    rec.set_defaults(func=cmd_record)

    rp = sub.add_parser(
        "replay", parents=[common, graph, output, live],
        help="replay runs from the log; exit 1 unless every one is identical",
    )
    rp.add_argument("run_ids", nargs="*", metavar="RUN_ID")
    rp.add_argument("--all", action="store_true", help="replay every recorded run")
    rp.set_defaults(func=cmd_replay)

    fk = sub.add_parser(
        "fork", parents=[common, graph, output, live],
        help="replay a run with chosen calls edited or rerun live",
        description="STEP is a call's index (see `ledger steps`) or its step key.",
    )
    fk.add_argument("run_id", metavar="RUN_ID")
    fk.add_argument(
        "--edit", action="append", default=[], metavar="STEP=TEXT",
        help="give the call this outcome as a string (for an LLM call: its reply text)",
    )
    fk.add_argument(
        "--edit-json", action="append", default=[], metavar="STEP=JSON",
        help="give the call this outcome, parsed as JSON",
    )
    fk.add_argument(
        "--rerun", action="append", default=[], metavar="STEP", help="run the call live again"
    )
    fk.set_defaults(func=cmd_fork)

    st = sub.add_parser("steps", parents=[common], help="list a run's calls")
    st.add_argument("run_id", metavar="RUN_ID")
    st.add_argument("--node", help="only calls made by this node")
    st.set_defaults(func=cmd_steps)

    rs = sub.add_parser("runs", parents=[common], help="list the runs in the store")
    rs.add_argument("--mode", choices=sorted(set(MODES.values())), help="only runs of this mode")
    rs.set_defaults(func=cmd_runs)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        with EventStore(args.db) as store:
            return args.func(args, store)
    except CLIError as exc:
        print(f"ledger {args.cmd}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
