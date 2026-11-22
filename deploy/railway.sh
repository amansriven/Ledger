#!/bin/sh
# Redeploy the hosted read-only demo (https://ledger-trace.up.railway.app) to
# Railway: uploads the repo, builds deploy/Dockerfile.server there, and swaps
# the new container in once GET / answers. Needs the Railway CLI, signed in
# with access to the ledger-demo project. Run from anywhere in the repo:
#
#   deploy/railway.sh
#
# The image bakes in a fresh demo store at build time, so each redeploy has new
# run ids and timestamps. It holds no secrets and needs no variables or volume;
# the server listens on Railway's $PORT and takes no writes.
#
# One-time setup, already done for the live demo (project ledger-demo, service
# web, environment production):
#
#   railway init --name ledger-demo
#   railway add --service web --variables RAILWAY_DOCKERFILE_PATH=deploy/Dockerfile.server
#   railway domain --service web          # then rename it in the dashboard
#
# and, in the service's settings, healthcheck path / and restart on failure.
set -eu
cd "$(dirname "$0")/.."
project=5dd0877b-863a-47c6-b584-0ceea56716af  # ledger-demo
railway up --project "$project" --environment production --service web \
  --ci -m "Ledger demo: $(git rev-parse --short HEAD)"
