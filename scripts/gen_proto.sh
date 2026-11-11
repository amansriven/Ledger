#!/usr/bin/env bash
# Regenerate Python code from proto/*.proto into ledger/_proto/.
# The generated files are committed so `pip install` needs no protoc.
set -euo pipefail
cd "$(dirname "$0")/.."
uv run python -m grpc_tools.protoc \
  -I proto \
  --python_out=ledger/_proto \
  --pyi_out=ledger/_proto \
  proto/events.proto
echo "generated ledger/_proto/events_pb2.py"
