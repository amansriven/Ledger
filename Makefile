# Building the trace UI into both servers, and running every test suite.
#
#   make ui        build ui/ and copy it into ledger/ui/static (the wheel)
#                  and server/webui/dist (the Go binary's embed)
#   make server    build bin/ledger-server, with the UI embedded
#   make demo      (re)create demo.db, a store with one of each kind of run
#   make dist      make ui, then build the Python sdist and wheel
#   make test      Python, Go and UI tests
#   make image     the hosted read-only UI as a Docker image (ledger-trace)
#
# The UI needs Node 22.17 or newer (ui/.nvmrc says 24: `nvm use` in ui/).

UI_STATIC := ledger/ui/static
GO_EMBED  := server/webui/dist

.PHONY: ui server demo dist test test-py test-go test-ui image node-version

node-version:
	@node -e 'const [a,b]=process.versions.node.split(".").map(Number); if (a<22||(a===22&&b<17)) { console.error("make ui: Node "+process.versions.node+" is too old; the UI needs 22.17+ (cd ui && nvm use)"); process.exit(1) }'

ui: node-version
	cd ui && npm ci && npm run build
	rm -rf $(UI_STATIC)
	mkdir -p $(UI_STATIC)
	cp -R ui/build/. $(UI_STATIC)/
	find $(GO_EMBED) -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
	cp -R ui/build/. $(GO_EMBED)/

server:
	cd server && go build -trimpath -o ../bin/ledger-server ./cmd/ledger-server

demo:
	uv run python scripts/make_demo_db.py --out demo.db

dist: ui
	rm -rf dist
	uv build

test: test-py test-go test-ui

test-py:
	uv run pytest -q

test-go:
	cd server && go vet ./... && go test ./...

test-ui: node-version
	cd ui && npm run check && npm test

image:
	docker build -f deploy/Dockerfile.server -t ledger-trace .
