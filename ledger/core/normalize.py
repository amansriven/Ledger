"""What counts as "the same request" during replay.

Replay finds a recorded call by its step key, then checks that the request is
the one that was recorded. Comparing raw bytes is too strict: prompts often
carry a timestamp or a fresh id that changes on every run even though nothing
that matters has changed. So each request is normalized first, and replay
compares the SHA-256 of the normalized form (its *match key*).

The default rules, applied to every string anywhere in the request:

- an ISO 8601 date-time (``2026-11-12T10:32:01Z``, ``2026-11-12 10:32``)
  becomes ``<timestamp>``; a bare date is left alone, since it usually means
  something (``report for 2026-11-12``)
- a UUID in canonical form, or in 32-character hex form when it has a UUID4's
  version and variant digits (Ledger run ids look like that), becomes
  ``<uuid>``

and for LLM requests, these top-level keyword arguments are dropped because
they change how the request is sent, not what it asks: ``timeout``,
``extra_headers``.

Everything else must match exactly: model, messages, temperature, tools and
so on. Dict key order never matters (payloads are canonical JSON).

The raw request is still logged in full; only the comparison uses the
normalized form. Pass your own ``Normalizer`` to ``ledger.wrap`` or
``ledger.tool`` to add rules, or ``EXACT`` to turn normalization off.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from ledger.core.events import canonical_json, pb

__all__ = ["Normalizer", "DEFAULT", "LLM_DEFAULT", "EXACT", "TIMESTAMP", "UUID", "UUID4_HEX"]

TIMESTAMP = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)?(?![\d:])"
)
UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
UUID4_HEX = re.compile(r"\b[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}\b")

_DEFAULT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (TIMESTAMP, "<timestamp>"),
    (UUID, "<uuid>"),
    (UUID4_HEX, "<uuid>"),
)


@dataclass(frozen=True)
class Normalizer:
    """Masks volatile substrings and drops top-level keys before hashing.

    ``patterns`` are applied in order to every string in the value (dict keys
    included). ``ignore_keys`` are dropped from the top-level dict only, since
    the same key name deeper in a request may well matter.
    """

    patterns: tuple[tuple[re.Pattern[str], str], ...] = _DEFAULT_PATTERNS
    ignore_keys: frozenset[str] = field(default_factory=frozenset)

    def extend(
        self,
        patterns: Iterable[tuple[str | re.Pattern[str], str]] = (),
        ignore_keys: Iterable[str] = (),
    ) -> Normalizer:
        """A new normalizer with extra rules after these ones."""
        extra = tuple((re.compile(p) if isinstance(p, str) else p, r) for p, r in patterns)
        return Normalizer(self.patterns + extra, self.ignore_keys | frozenset(ignore_keys))

    def normalize(self, value: Any) -> Any:
        """``value`` as plain JSON data with the rules applied."""
        data = json.loads(canonical_json(value))
        if isinstance(data, dict) and self.ignore_keys:
            data = {k: v for k, v in data.items() if k not in self.ignore_keys}
        return self._walk(data)

    def _walk(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._mask(value)
        if isinstance(value, list):
            return [self._walk(v) for v in value]
        if isinstance(value, dict):
            return {self._mask(k): self._walk(v) for k, v in value.items()}
        return value

    def _mask(self, s: str) -> str:
        for pattern, replacement in self.patterns:
            s = pattern.sub(replacement, s)
        return s

    def key(self, value: Any) -> str:
        """SHA-256 of the normalized value's canonical JSON."""
        return hashlib.sha256(canonical_json(self.normalize(value))).hexdigest()

    def key_of(self, payload: pb.Payload) -> str:
        """Match key of an already-canonical payload."""
        return self.key(json.loads(payload.json))


DEFAULT = Normalizer()
LLM_DEFAULT = DEFAULT.extend(ignore_keys=("timeout", "extra_headers"))
EXACT = Normalizer(patterns=())
