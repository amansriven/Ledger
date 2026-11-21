"""A fake Stripe API: charges, refunds and idempotency keys, plus injected faults.

Enough of Stripe's REST API for the official ``stripe`` SDK to talk to it
(form-encoded requests, Stripe's error format), so the refund agent runs the
same code against this server and against Stripe in test mode:

    GET  /v1/charges/{id}        a charge, with amount_refunded and refunded
    POST /v1/charges/{id}        metadata[k]= updates (an empty value removes k)
    POST /v1/refunds             charge=, amount= (default: what's left), reason=, metadata[k]=
    GET  /v1/refunds?charge=     refunds of a charge, newest first

POST requests honour ``Idempotency-Key`` like Stripe: the same key with the
same parameters answers with the first response (header
``Idempotent-Replayed: true``) and applies nothing; the same key with other
parameters is an ``idempotency_error``. Keys are kept for the server's life.

Test hooks, outside Stripe's API:

    POST /_charges               {"amount": 5000, "currency": "usd"} -> a new charge
    POST /_faults                {"path": "/v1/refunds", "mode": ..., "count": 1}
    GET  /_state                 every charge, refund and request count

A fault's path may end in ``*`` to match every path with that prefix
(``/v1/charges/*``). Fault modes: ``drop_before`` closes the connection
without applying the request; ``drop_after`` applies it, then closes the connection without an
answer (the case that leaves a caller in doubt); ``error_500`` answers 500
without applying it.

Run it with ``python -m ledger.testing.fake_stripe --port 12111``.
"""

from __future__ import annotations

import argparse
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

__all__ = ["FakeStripeServer", "main"]

FAULT_MODES = ("drop_before", "drop_after", "error_500")


def _form(body: bytes) -> dict[str, Any]:
    """Stripe's form encoding, with one level of ``key[sub]=v`` nesting."""
    out: dict[str, Any] = {}
    for key, values in parse_qs(body.decode(), keep_blank_values=True).items():
        value = values[-1]
        if "[" in key and key.endswith("]"):
            outer, inner = key[:-1].split("[", 1)
            out.setdefault(outer, {})[inner] = value
        else:
            out[key] = value
    return out


class _Abort(Exception):
    """Close the connection without answering."""


class _Handler(BaseHTTPRequestHandler):
    server: FakeStripeServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        if self.server.verbose:
            super().log_message(format, *args)

    def _send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Request-Id", f"req_{secrets.token_hex(7)}")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _authorized(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer sk_test_"):
            return True
        self._send(401, _error("invalid_request_error", "Invalid API Key provided."))
        return False

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/healthz":
            return self._send(200, {"ok": True})
        if url.path == "/_state":
            return self._send(200, self.server.state())
        if not self._authorized():
            return
        status, body = self.server.get(url.path, parse_qs(url.query))
        self._send(status, body)

    def do_POST(self) -> None:
        url = urlparse(self.path)
        raw = self._body()
        if url.path == "/_charges":
            spec = json.loads(raw or b"{}")
            return self._send(200, self.server.create_charge(**spec))
        if url.path == "/_faults":
            self.server.add_fault(**json.loads(raw or b"{}"))
            return self._send(200, {"ok": True})
        if not self._authorized():
            return
        key = self.headers.get("Idempotency-Key")
        try:
            status, body, replayed = self.server.post(url.path, _form(raw), key)
        except _Abort:
            self.close_connection = True
            return
        self._send(status, body, {"Idempotent-Replayed": "true"} if replayed else None)


def _error(kind: str, message: str, *, code: str = "", param: str = "") -> dict[str, Any]:
    err: dict[str, Any] = {"type": kind, "message": message}
    if code:
        err["code"] = code
    if param:
        err["param"] = param
    return {"error": err}


class FakeStripeServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, verbose: bool = False) -> None:
        super().__init__((host, port), _Handler)
        self.verbose = verbose
        self.charges: dict[str, dict[str, Any]] = {}
        self.refunds: dict[str, dict[str, Any]] = {}
        # key -> (path, params, status, body)
        self._idempotency: dict[str, tuple[str, dict[str, Any], int, Any]] = {}
        self._faults: list[dict[str, Any]] = []
        self._counts: dict[str, int] = {}
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    # Test hooks

    def create_charge(self, amount: int = 5000, currency: str = "usd", **extra: Any) -> dict:
        with self._lock:
            charge = {
                "id": f"ch_{secrets.token_hex(12)}",
                "object": "charge",
                "amount": int(amount),
                "amount_refunded": 0,
                "currency": currency,
                "refunded": False,
                "status": "succeeded",
                "created": int(time.time()),
                "metadata": dict(extra.get("metadata", {})),
            }
            self.charges[charge["id"]] = charge
            return dict(charge)

    def add_fault(self, path: str, mode: str, count: int = 1) -> None:
        if mode not in FAULT_MODES:
            raise ValueError(f"fault mode must be one of {FAULT_MODES}")
        with self._lock:
            self._faults.append({"path": path, "mode": mode, "count": count})

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "charges": [dict(c) for c in self.charges.values()],
                "refunds": [dict(r) for r in self.refunds.values()],
                "requests": dict(self._counts),
                "faults_pending": sum(f["count"] for f in self._faults),
            }

    def charge(self, charge_id: str) -> dict[str, Any]:
        with self._lock:
            charge = self.charges[charge_id]
            return dict(charge, metadata=dict(charge["metadata"]))

    def refunds_of(self, charge_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self.refunds.values() if r["charge"] == charge_id]

    # API

    def _fault(self, path: str) -> str:
        for fault in self._faults:
            target = fault["path"]
            hit = path.startswith(target[:-1]) if target.endswith("*") else path == target
            if hit and fault["count"] > 0:
                fault["count"] -= 1
                return fault["mode"]
        return ""

    def get(self, path: str, query: dict[str, list[str]]) -> tuple[int, Any]:
        with self._lock:
            self._counts[f"GET {path.rsplit('/', 1)[0]}"] = (
                self._counts.get(f"GET {path.rsplit('/', 1)[0]}", 0) + 1
            )
            if path.startswith("/v1/charges/"):
                charge = self.charges.get(path.rsplit("/", 1)[1])
                if charge is None:
                    return 404, _error(
                        "invalid_request_error", "No such charge", code="resource_missing",
                        param="id",
                    )
                return 200, dict(charge, metadata=dict(charge["metadata"]))
            if path == "/v1/refunds":
                charge_id = query.get("charge", [""])[0]
                items = [r for r in self.refunds.values() if not charge_id or r["charge"] == charge_id]
                items.sort(key=lambda r: r["created_seq"], reverse=True)
                return 200, {
                    "object": "list", "url": "/v1/refunds", "has_more": False,
                    "data": [_public(r) for r in items],
                }
        return 404, _error("invalid_request_error", f"Unrecognized request URL (GET: {path})")

    def post(
        self, path: str, params: dict[str, Any], key: str | None
    ) -> tuple[int, Any, bool]:
        with self._lock:
            self._counts[f"POST {path}"] = self._counts.get(f"POST {path}", 0) + 1
            fault = self._fault(path)
            if fault == "drop_before":
                raise _Abort
            if fault == "error_500":
                return 500, _error("api_error", "Something went wrong on Stripe's end."), False
            if key is not None and key in self._idempotency:
                first_path, first_params, status, body = self._idempotency[key]
                if (first_path, first_params) != (path, params):
                    return 400, _error(
                        "idempotency_error",
                        "Keys for idempotent requests can only be used with the same "
                        "parameters they were first used with.",
                    ), False
                if fault == "drop_after":
                    raise _Abort
                return status, body, True
            if path == "/v1/refunds":
                status, body = self._create_refund(params)
            elif path.startswith("/v1/charges/"):
                status, body = self._update_charge(path.rsplit("/", 1)[1], params)
            else:
                status, body = 404, _error(
                    "invalid_request_error", f"Unrecognized request URL (POST: {path})"
                )
            if key is not None and status != 500:
                self._idempotency[key] = (path, params, status, body)
            if fault == "drop_after":
                raise _Abort
            return status, body, False

    def _update_charge(self, charge_id: str, params: dict[str, Any]) -> tuple[int, Any]:
        charge = self.charges.get(charge_id)
        if charge is None:
            return 404, _error(
                "invalid_request_error", f"No such charge: '{charge_id}'",
                code="resource_missing", param="id",
            )
        for key, value in dict(params.get("metadata", {})).items():
            if value == "":
                charge["metadata"].pop(key, None)
            else:
                charge["metadata"][key] = value
        return 200, dict(charge, metadata=dict(charge["metadata"]))

    def _create_refund(self, params: dict[str, Any]) -> tuple[int, Any]:
        charge = self.charges.get(params.get("charge", ""))
        if charge is None:
            return 404, _error(
                "invalid_request_error", f"No such charge: '{params.get('charge', '')}'",
                code="resource_missing", param="charge",
            )
        left = charge["amount"] - charge["amount_refunded"]
        if left == 0:
            return 400, _error(
                "invalid_request_error",
                f"Charge {charge['id']} has already been refunded.",
                code="charge_already_refunded",
            )
        amount = int(params.get("amount", left))
        if amount <= 0 or amount > left:
            return 400, _error(
                "invalid_request_error",
                f"Refund amount ({amount}) is greater than unrefunded amount on charge ({left})",
                code="amount_too_large", param="amount",
            )
        refund = {
            "id": f"re_{secrets.token_hex(12)}",
            "object": "refund",
            "amount": amount,
            "charge": charge["id"],
            "currency": charge["currency"],
            "reason": params.get("reason"),
            "status": "succeeded",
            "metadata": dict(params.get("metadata", {})),
            "created": int(time.time()),
            "created_seq": len(self.refunds),
        }
        self.refunds[refund["id"]] = refund
        charge["amount_refunded"] += amount
        charge["refunded"] = charge["amount_refunded"] == charge["amount"]
        return 200, _public(refund)

    # Lifecycle

    def start(self) -> FakeStripeServer:
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self.shutdown()
        self.server_close()

    def __enter__(self) -> FakeStripeServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def _public(refund: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in refund.items() if k != "created_seq"}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=12111)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    server = FakeStripeServer(args.host, args.port, verbose=args.verbose)
    print(f"fake Stripe listening on {server.base_url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
