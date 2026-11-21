"""``ledger.v1.TraceService`` over the Connect protocol, on the standard library.

Speaks Connect with the JSON codec only (https://connectrpc.com/docs/protocol),
which is what the UI's connect-web client sends:

* unary RPCs (ListRuns, GetRun, GetPayload, ListDiagnoses) are
  ``POST /ledger.v1.TraceService/<Method>`` with ``Content-Type:
  application/json``; the body and the response are the messages in proto3
  JSON. A failure is a non-200 status with ``{"code": ..., "message": ...}``.
* WatchRun streams: ``Content-Type: application/connect+json``, and every
  message is an envelope (a flags byte, a 4-byte big-endian length, the
  JSON). The response is always 200; it ends with an end-of-stream envelope
  (flags 0x02) holding ``{}`` or ``{"error": {...}}``.

The server only reads the log. RunSummary comes from the same code as
``ledger runs`` (``RunInfo``, ``run_status``), and payloads are filled in
from the blob store up to ``INLINE_LIMIT``; bigger ones are left for
GetPayload. Other GET paths serve the built UI from ``ledger/ui/static/``.
"""

from __future__ import annotations

import json
import mimetypes
import re
import select
import socket
import sqlite3
import struct
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from google.protobuf import json_format
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp

from ledger._proto import trace_service_pb2 as ts
from ledger.cli import MODES, RunInfo, list_runs
from ledger.core.blobs import BlobCorrupt, BlobMissing
from ledger.core.events import body_kind, pb
from ledger.core.store import EventStore, payloads
from ledger.replay.resume import run_status

__all__ = ["INLINE_LIMIT", "ConnectError", "TraceServer", "TraceService", "STATIC_DIR"]

SERVICE = "ledger.v1.TraceService"
PREFIX = f"/{SERVICE}/"
# Payloads bigger than this are left out of events; see trace_service.proto.
INLINE_LIMIT = 256 * 1024
POLL_INTERVAL = 0.2
STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_REQUEST = 1 << 20

_DIGEST = re.compile(r"[0-9a-f]{64}")
_MODE = {name: value for value, name in MODES.items()}
_STATUS = {
    "ok": ts.RUN_STATUS_OK,
    "error": ts.RUN_STATUS_ERROR,
    "unfinished": ts.RUN_STATUS_UNFINISHED,
}

# Connect error codes and the HTTP status each maps to.
HTTP_STATUS = {
    "canceled": 499,
    "unknown": 500,
    "invalid_argument": 400,
    "deadline_exceeded": 504,
    "not_found": 404,
    "already_exists": 409,
    "permission_denied": 403,
    "resource_exhausted": 429,
    "failed_precondition": 400,
    "aborted": 409,
    "out_of_range": 400,
    "unimplemented": 501,
    "internal": 500,
    "unavailable": 503,
    "data_loss": 500,
    "unauthenticated": 401,
}

_END_STREAM = 0x02
_COMPRESSED = 0x01


class ConnectError(Exception):
    """An RPC failure, sent as a Connect error."""

    def __init__(self, code: str, message: str) -> None:
        assert code in HTTP_STATUS, code
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


class _ClientGone(Exception):
    """The client closed the connection mid-stream."""


def _to_json(msg: Message) -> bytes:
    return json.dumps(
        json_format.MessageToDict(msg), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _parse(data: bytes, msg: Message) -> None:
    """Fill ``msg`` from proto3 JSON. Unknown fields are ignored, as connect-go does."""
    try:
        text = data.decode("utf-8") if data else "{}"
        json_format.Parse(text, msg, ignore_unknown_fields=True)
    except (UnicodeDecodeError, json_format.ParseError) as exc:
        raise ConnectError("invalid_argument", f"bad request body: {exc}") from None


# The service


class TraceService:
    """The RPCs, over a store opened read-only. Thread-safe."""

    def __init__(
        self,
        store: EventStore,
        *,
        inline_limit: int = INLINE_LIMIT,
        poll_interval: float = POLL_INTERVAL,
        stopping: threading.Event | None = None,
    ) -> None:
        self.store = store
        self.inline_limit = inline_limit
        self.poll_interval = poll_interval
        self.stopping = stopping or threading.Event()

    # Building blocks

    def _events(self, run_id: str) -> list[pb.Event]:
        events = list(self.store.events(run_id, hydrate=False))
        if not events:
            raise ConnectError("not_found", f"no run {run_id!r}")
        return events

    def _inline(self, event: pb.Event) -> pb.Event:
        """Fill in payloads from the blob store, leaving out those over the limit."""
        for p in payloads(event):
            if p.json:
                if len(p.json) > self.inline_limit:
                    p.json = b""
                continue
            if not p.sha256:
                continue
            try:
                if self.store.blobs.size(p.sha256) > self.inline_limit:
                    continue
                p.json = self.store.blobs.get(p.sha256)
            except (FileNotFoundError, BlobMissing):
                raise ConnectError(
                    "data_loss", f"run {event.run_id} event {event.index}: blob {p.sha256} is missing"
                ) from None
            except BlobCorrupt as exc:
                raise ConnectError("data_loss", str(exc)) from None
        return event

    def summary(self, run_id: str, events: list[pb.Event]) -> ts.RunSummary:
        info = RunInfo(run_id, events)
        status, error = run_status(events)
        s = ts.RunSummary(
            run_id=run_id,
            mode=_MODE.get(info.mode, pb.MODE_UNSPECIFIED),
            source_run_id=info.source,
            graph=info.graph,
            status=_STATUS[status],
            calls=info.calls,
            live=info.live,
            diverged=info.diverged,
            resumed=info.resumed,
            events=info.events,
            diagnoses=len(self.store.diagnoses(run_id)),
        )
        if error is not None:
            s.error.CopyFrom(error)
        if events:
            s.started_at.CopyFrom(events[0].recorded_at)
        if status != "unfinished":
            last = next(e for e in reversed(events) if body_kind(e) == "run_finished")
            s.finished_at.CopyFrom(last.recorded_at)
        return s

    # RPCs

    def ListRuns(self, req: ts.ListRunsRequest) -> ts.ListRunsResponse:
        return ts.ListRunsResponse(
            runs=[self.summary(info.run_id, list(self.store.events(info.run_id, hydrate=False)))
                  for info in list_runs(self.store)]
        )

    def GetRun(self, req: ts.GetRunRequest) -> ts.GetRunResponse:
        events = self._events(req.run_id)
        summary = self.summary(req.run_id, events)
        return ts.GetRunResponse(summary=summary, events=[self._inline(e) for e in events])

    def GetPayload(self, req: ts.GetPayloadRequest) -> ts.GetPayloadResponse:
        digest = req.sha256
        # Anything but a lowercase hex digest names no payload (and never
        # reaches the filesystem).
        if not _DIGEST.fullmatch(digest):
            raise ConnectError("not_found", f"no payload {digest!r}")
        try:
            data = self.store.blobs.get(digest)
        except BlobMissing:
            raise ConnectError("not_found", f"no payload {digest}") from None
        except BlobCorrupt as exc:
            raise ConnectError("data_loss", str(exc)) from None
        return ts.GetPayloadResponse(payload=pb.Payload(sha256=digest, json=data))

    def ListDiagnoses(self, req: ts.ListDiagnosesRequest) -> ts.ListDiagnosesResponse:
        if req.run_id not in self.store.run_ids_with_prefix(req.run_id):
            raise ConnectError("not_found", f"no run {req.run_id!r}")
        out = ts.ListDiagnosesResponse()
        for d in self.store.diagnoses(req.run_id):
            created = Timestamp()
            created.FromJsonString(d.created_at)
            out.diagnoses.add(
                id=d.id, run_id=d.run_id, created_at=created,
                json=json.dumps(d.data, ensure_ascii=False),
            )
        return out

    def WatchRun(
        self, req: ts.WatchRunRequest, cancelled: Callable[[], bool] = lambda: False,
        deadline: float | None = None,
    ) -> Iterator[ts.WatchRunResponse]:
        """The run's events from ``from_index``: the backlog, then new ones as
        they are committed, until the run finishes.

        Polls ``PRAGMA data_version`` on a connection of its own every
        ``poll_interval`` and queries only when it changed, that is when
        another connection committed. ``cancelled`` is checked between polls
        (the client went away); past ``deadline`` (``time.monotonic()``) the
        stream ends with deadline_exceeded.
        """
        if req.from_index < 0:
            raise ConnectError("invalid_argument", f"from_index {req.from_index} is negative")
        conn = self.store.open_reader()
        try:
            seen: int | None = None
            next_idx = 0
            finished = False
            while True:
                version = conn.execute("PRAGMA data_version").fetchone()[0]
                if version != seen:
                    seen = version
                    batch = self._read_from(conn, req.run_id, next_idx)
                    if next_idx == 0 and not batch:
                        raise ConnectError("not_found", f"no run {req.run_id!r}")
                    if batch:
                        next_idx = batch[-1].index + 1
                    # run_status folds over the log, and only a RunResumed or
                    # a RunFinished changes it: a batch with one of them
                    # decides the status, a batch without leaves it as it was.
                    if any(body_kind(e) in ("run_resumed", "run_finished") for e in batch):
                        finished = run_status(batch)[0] != "unfinished"
                    for ev in batch:
                        if ev.index >= req.from_index:
                            yield ts.WatchRunResponse(event=self._inline(ev))
                    if finished:
                        return
                if cancelled():
                    raise _ClientGone
                if deadline is not None and time.monotonic() >= deadline:
                    raise ConnectError("deadline_exceeded", "the watch reached its deadline")
                if self.stopping.wait(self.poll_interval):
                    raise ConnectError("unavailable", "the server is shutting down")
        finally:
            conn.close()

    @staticmethod
    def _read_from(conn: sqlite3.Connection, run_id: str, start: int) -> list[pb.Event]:
        rows = conn.execute(
            "SELECT data FROM events WHERE run_id = ? AND idx >= ? ORDER BY idx", (run_id, start)
        ).fetchall()
        out = []
        for (data,) in rows:
            ev = pb.Event()
            ev.ParseFromString(data)
            out.append(ev)
        return out


UNARY: dict[str, type[Message]] = {
    "ListRuns": ts.ListRunsRequest,
    "GetRun": ts.GetRunRequest,
    "GetPayload": ts.GetPayloadRequest,
    "ListDiagnoses": ts.ListDiagnosesRequest,
}
STREAMING: dict[str, type[Message]] = {"WatchRun": ts.WatchRunRequest}


# HTTP


_NOT_BUILT = b"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Ledger</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font:16px/1.5 system-ui,sans-serif;max-width:40rem;margin:4rem auto;padding:0 1rem}
code{background:#8881;padding:.1em .3em;border-radius:3px}</style></head>
<body><h1>Ledger trace API is up</h1>
<p>The UI is not built into this install, so there is nothing to show here yet.
The <code>ledger.v1.TraceService</code> API is answering at
<code>/ledger.v1.TraceService/</code>.</p>
<p>In a checkout, run <code>make ui</code> to build the UI into
<code>ledger/ui/static/</code>, or run the SvelteKit dev server in <code>ui/</code>
against this server.</p></body></html>
"""

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_ALLOW_HEADERS = (
    "Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, "
    "Connect-Content-Encoding, Connect-Accept-Encoding, X-User-Agent"
)
_EXPOSE_HEADERS = "Content-Encoding, Connect-Content-Encoding"


def _local_origin(origin: str) -> bool:
    try:
        parts = urlsplit(origin)
        host = parts.hostname or ""
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and (host in _LOCAL_HOSTS or host.endswith(".localhost"))


def _envelope(flags: int, data: bytes) -> bytes:
    return struct.pack(">BI", flags, len(data)) + data


def _content_type(value: str | None) -> tuple[str, bool]:
    """(media type, whether its charset, if any, is utf-8)."""
    media, _, params = (value or "").partition(";")
    charset_ok = True
    for param in params.split(";"):
        key, _, val = param.strip().partition("=")
        if key.strip().lower() == "charset":
            charset_ok = val.strip().strip('"').lower() in ("utf-8", "utf8")
    return media.strip().lower(), charset_ok


class _BadRequest(Exception):
    """A request we can't read at all: answered with a bare HTTP status."""

    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        super().__init__(status)
        self.status = status
        self.headers = headers or {}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: TraceServer
    server_version = "ledger-ui"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        if self.server.verbose:
            super().log_message(format, *args)

    # Plumbing

    def _read_body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return bytes(body)
                body += self.rfile.read(size)
                self.rfile.readline()
                if len(body) > MAX_REQUEST:
                    self.close_connection = True
                    raise _BadRequest(413)
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_REQUEST:
            self.close_connection = True
            raise _BadRequest(413)
        return self.rfile.read(length) if length else b""

    def _cors(self, preflight: bool = False) -> None:
        origin = self.headers.get("Origin")
        if not origin or not _local_origin(origin):
            return
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Vary", "Origin")
        if preflight:
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            requested = self.headers.get("Access-Control-Request-Headers")
            self.send_header("Access-Control-Allow-Headers", requested or _ALLOW_HEADERS)
            self.send_header("Access-Control-Max-Age", "7200")
        else:
            self.send_header("Access-Control-Expose-Headers", _EXPOSE_HEADERS)

    def _send(self, status: int, body: bytes, content_type: str | None,
              headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        if content_type:
            self.send_header("Content-Type", content_type)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_error(self, err: ConnectError) -> None:
        body = json.dumps(err.to_dict(), ensure_ascii=False).encode("utf-8")
        self._send(HTTP_STATUS[err.code], body, "application/json")

    def _check_protocol_headers(self, encoding_header: str) -> None:
        version = self.headers.get("Connect-Protocol-Version")
        if version is not None and version.strip() != "1":
            raise ConnectError("invalid_argument", f"unsupported Connect-Protocol-Version {version!r}")
        encoding = (self.headers.get(encoding_header) or "identity").strip().lower()
        if encoding != "identity":
            raise ConnectError(
                "unimplemented", f"unsupported {encoding_header} {encoding!r}; supported: identity"
            )

    def _deadline(self) -> float | None:
        value = self.headers.get("Connect-Timeout-Ms")
        if value is None:
            return None
        value = value.strip()
        if not value.isdigit() or len(value) > 10:
            raise ConnectError("invalid_argument", f"bad Connect-Timeout-Ms {value!r}")
        if int(value) == 0:
            raise ConnectError("deadline_exceeded", "Connect-Timeout-Ms is 0")
        return time.monotonic() + int(value) / 1000

    def _client_gone(self) -> bool:
        """True once the client has closed its end of the connection."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            if not readable:
                return False
            return self.connection.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True

    # Methods

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._cors(preflight=True)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path.startswith(PREFIX):
            # Connect's GET form of unary calls is optional; we take POST only.
            self._send(405, b"", None, {"Allow": "POST"})
            return
        self._static(path)

    do_HEAD = do_GET

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        try:
            body = self._read_body()
        except _BadRequest as exc:
            self._send(exc.status, b"", None)
            return
        except ValueError:
            self.close_connection = True
            self._send(400, b"", None)
            return
        if not path.startswith(PREFIX):
            self._send(404, b"not found\n", "text/plain; charset=utf-8")
            return
        method = path[len(PREFIX):]
        if method in STREAMING:
            self._stream(method, body)
        elif method in UNARY:
            self._unary(method, body)
        else:
            # connect-go's mux answers an unknown procedure with a 404; we
            # add a Connect error body, which clients read as unimplemented.
            err = ConnectError("unimplemented", f"no method {SERVICE}/{method}")
            self._send(404, json.dumps(err.to_dict()).encode(), "application/json")

    def _unary(self, method: str, body: bytes) -> None:
        media, charset_ok = _content_type(self.headers.get("Content-Type"))
        if media != "application/json" or not charset_ok:
            self._send(415, b"", None, {"Accept-Post": "application/json"})
            return
        try:
            self._check_protocol_headers("Content-Encoding")
            self._deadline()
            req = UNARY[method]()
            _parse(body, req)
            resp = getattr(self.server.service, method)(req)
            self._send(200, _to_json(resp), "application/json")
        except ConnectError as err:
            self._send_error(err)
        except Exception as exc:  # a bug: report it, don't take the server down
            traceback.print_exc(file=sys.stderr)
            self._send_error(ConnectError("internal", f"{type(exc).__name__}: {exc}"))

    def _stream(self, method: str, body: bytes) -> None:
        media, charset_ok = _content_type(self.headers.get("Content-Type"))
        if media != "application/connect+json" or not charset_ok:
            self._send(415, b"", None, {"Accept-Post": "application/connect+json"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/connect+json")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        end: dict[str, Any] = {}
        try:
            self._check_protocol_headers("Connect-Content-Encoding")
            deadline = self._deadline()
            req = STREAMING[method]()
            _parse(self._unenvelope(body), req)
            for msg in self.server.service.WatchRun(req, self._client_gone, deadline):
                self._chunk(_envelope(0, _to_json(msg)))
        except ConnectError as err:
            end = {"error": err.to_dict()}
        except (_ClientGone, BrokenPipeError, ConnectionResetError):
            self.close_connection = True
            return
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            end = {"error": ConnectError("internal", f"{type(exc).__name__}: {exc}").to_dict()}
        try:
            self._chunk(_envelope(_END_STREAM, json.dumps(end).encode("utf-8")))
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    @staticmethod
    def _unenvelope(body: bytes) -> bytes:
        if len(body) < 5:
            raise ConnectError("invalid_argument", "the request stream has no message")
        flags, length = struct.unpack(">BI", body[:5])
        if flags & _COMPRESSED:
            raise ConnectError("internal", "the request message is compressed with no encoding")
        if flags & _END_STREAM:
            raise ConnectError("invalid_argument", "a request stream must not send end-of-stream")
        if len(body) != 5 + length:
            raise ConnectError("invalid_argument", "a server-streaming request has exactly one message")
        return body[5:]

    def _chunk(self, data: bytes) -> None:
        self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
        self.wfile.flush()

    # The built UI

    def _static(self, path: str) -> None:
        root = self.server.static_dir
        if root is None or not root.is_dir():
            self._send(200, _NOT_BUILT, "text/html; charset=utf-8", {"Cache-Control": "no-cache"})
            return
        root = root.resolve()
        rel = unquote(path).lstrip("/")
        candidates = [root / rel, root / rel / "index.html", root / f"{rel}.html"] if rel else []
        candidates.append(root / "index.html")  # the app's own router takes it from there
        for candidate in candidates:
            try:
                file = candidate.resolve()
            except (OSError, RuntimeError):
                continue
            if file.is_relative_to(root) and file.is_file():
                break
        else:
            self._send(404, b"not found\n", "text/plain; charset=utf-8")
            return
        ctype = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
            ctype += "; charset=utf-8"
        immutable = "/_app/immutable/" in f"/{file.relative_to(root).as_posix()}"
        cache = "public, max-age=31536000, immutable" if immutable else "no-cache"
        self._send(200, file.read_bytes(), ctype, {"Cache-Control": cache})


class TraceServer(ThreadingHTTPServer):
    """``ledger.v1.TraceService`` and the built UI over HTTP.

        with TraceServer(EventStore(path, read_only=True), port=0) as server:
            print(server.url)
    """

    daemon_threads = True

    def __init__(
        self,
        store: EventStore,
        host: str = "127.0.0.1",
        port: int = 8700,
        *,
        static_dir: Path | None = STATIC_DIR,
        inline_limit: int = INLINE_LIMIT,
        poll_interval: float = POLL_INTERVAL,
        verbose: bool = False,
    ) -> None:
        if ":" in host:
            self.address_family = socket.AF_INET6
        self.stopping = threading.Event()
        self.service = TraceService(
            store, inline_limit=inline_limit, poll_interval=poll_interval, stopping=self.stopping
        )
        self.static_dir = static_dir
        self.verbose = verbose
        self._thread: threading.Thread | None = None
        super().__init__((host, port), _Handler)

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        if ":" in host:
            host = f"[{host}]"
        return f"http://{host}:{port}"

    def handle_error(self, request: Any, client_address: Any) -> None:
        # A client that drops a kept-alive connection is routine, not an error.
        if isinstance(sys.exc_info()[1], ConnectionError):
            return
        super().handle_error(request, client_address)

    def start(self) -> TraceServer:
        """Serve on a background thread."""
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self.stopping.set()  # ends open watches
        if self._thread is not None:
            self.shutdown()
            self._thread.join()
            self._thread = None
        self.server_close()

    def __enter__(self) -> TraceServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()
