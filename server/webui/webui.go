// Package webui serves the built SvelteKit UI, embedded in the binary.
//
// `make ui` builds ui/ and copies the output into dist/ before `go build`.
// Without it the binary still serves the API, and the UI paths answer with a
// page saying how to build the UI.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// Built reports whether a UI build was embedded.
func Built() bool {
	_, err := fs.Stat(dist, "dist/index.html")
	return err == nil
}

// Handler serves the embedded UI. Paths that aren't files get index.html,
// so the SPA's client-side routes (/runs/<id>, /diff) load directly.
func Handler() http.Handler {
	root, _ := fs.Sub(dist, "dist")
	files := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Built() {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(notBuilt))
			return
		}
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if fi, err := fs.Stat(root, name); err != nil || fi.IsDir() {
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		files.ServeHTTP(w, r)
	})
}

const notBuilt = `<!doctype html><meta charset="utf-8"><title>Ledger</title>
<body style="font-family: system-ui, sans-serif; max-width: 40rem; margin: 4rem auto; padding: 0 1rem">
<h1>Ledger trace server</h1>
<p>The API is up at <code>/ledger.v1.TraceService/</code>, but this binary was built without the UI.
Run <code>make ui</code> in the repository, then rebuild the server.</p>`
