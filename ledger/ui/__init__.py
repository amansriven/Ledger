"""The trace UI: ``ledger ui`` serves the event log over the Connect protocol.

``ledger.ui.server.TraceServer`` answers ``ledger.v1.TraceService``
(``proto/trace_service.proto``) and serves the built SvelteKit UI from
``ledger/ui/static/``. The Go trace server in ``server/`` answers the same
API; ``tests/conformance.py`` checks the two agree.
"""

from ledger.ui.server import INLINE_LIMIT, TraceServer, TraceService

__all__ = ["INLINE_LIMIT", "TraceServer", "TraceService"]
