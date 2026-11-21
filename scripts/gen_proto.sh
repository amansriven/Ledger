#!/usr/bin/env bash
# Regenerate code from proto/*.proto:
#   Python     -> ledger/_proto/        (grpc_tools; messages only)
#   Go         -> server/gen/ledgerv1/  (buf: protobuf-go, connect-go)
#   TypeScript -> ui/src/lib/gen/       (buf: protobuf-es)
# The generated files are committed, so `pip install`, `go build` and the UI
# build need no protoc.
set -euo pipefail
cd "$(dirname "$0")/.."
uv run python -m grpc_tools.protoc \
  -I proto \
  --python_out=ledger/_proto \
  --pyi_out=ledger/_proto \
  proto/events.proto proto/trace_service.proto
# protoc writes `import events_pb2`, which only works with ledger/_proto on
# sys.path; make it a package import.
sed -i.bak 's/^import events_pb2 as /from ledger._proto import events_pb2 as /' \
  ledger/_proto/trace_service_pb2.py ledger/_proto/trace_service_pb2.pyi
rm -f ledger/_proto/*.bak
echo "generated ledger/_proto/{events,trace_service}_pb2.py"
buf generate
echo "generated server/gen/ledgerv1/ and ui/src/lib/gen/"
