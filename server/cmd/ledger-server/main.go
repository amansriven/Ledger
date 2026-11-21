// Command ledger-server serves a Ledger event store to the trace UI:
// ledger.v1.TraceService over Connect, gRPC and gRPC-web, plus the UI itself.
//
//	ledger-server --db ledger.db                  # live: follows runs as they're written
//	ledger-server --db demo.db --immutable        # a read-only copy, e.g. the hosted demo
//
// It only reads the store; the Python runtime is the only writer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/amansriven/Ledger/server/gen/ledgerv1/ledgerv1connect"
	"github.com/amansriven/Ledger/server/internal/store"
	"github.com/amansriven/Ledger/server/internal/trace"
	"github.com/amansriven/Ledger/server/webui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ledger-server:", err)
		os.Exit(1)
	}
}

func run() error {
	db := flag.String("db", envOr("LEDGER_DB", "ledger.db"), "event store (default: $LEDGER_DB or ledger.db)")
	addr := flag.String("addr", "127.0.0.1:8700", "address to listen on")
	immutable := flag.Bool("immutable", false, "treat the store as a file that never changes (no locks, no -shm; for read-only copies)")
	flag.Parse()

	st, err := store.Open(*db, store.Options{Immutable: *immutable})
	if err != nil {
		return err
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           Handler(st),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// h2c, so gRPC clients work without TLS; browsers use HTTP/1.1.
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	ui := "UI not built (run `make ui`)"
	if webui.Built() {
		ui = "UI at http://" + ln.Addr().String() + "/"
	}
	log.Printf("serving %s on %s; %s", st.Path(), ln.Addr(), ui)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler routes the API and the UI.
func Handler(st *store.Store) http.Handler {
	mux := http.NewServeMux()
	path, api := ledgerv1connect.NewTraceServiceHandler(trace.New(st))
	mux.Handle(path, localCORS(api))
	mux.Handle("/", webui.Handler())
	return mux
}

// localCORS lets a UI dev server on localhost call the API from another port.
func localCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); isLocal(origin) {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			h.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			h.Set("Access-Control-Allow-Headers",
				"Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-User-Agent, X-Grpc-Web, Grpc-Timeout")
			h.Set("Access-Control-Expose-Headers", "Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isLocal(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || origin == "" {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".localhost")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
