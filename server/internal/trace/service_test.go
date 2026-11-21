package trace_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	ledgerv1 "github.com/amansriven/Ledger/server/gen/ledgerv1"
	"github.com/amansriven/Ledger/server/gen/ledgerv1/ledgerv1connect"
	"github.com/amansriven/Ledger/server/internal/store"
	"github.com/amansriven/Ledger/server/internal/trace"
)

// demoDB is a store built by scripts/make_demo_db.py, shared by the tests
// that only read it.
var demoDB string

func TestMain(m *testing.M) {
	os.Exit(func() int {
		repo, _ := filepath.Abs("../../..")
		if _, err := exec.LookPath("uv"); err != nil {
			fmt.Println("skipping: uv is needed to build the demo store")
			return 0
		}
		dir, err := os.MkdirTemp("", "ledger-trace-test")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		demoDB = filepath.Join(dir, "demo.db")
		cmd := exec.Command("uv", "run", "python", "scripts/make_demo_db.py",
			"--out", demoDB, "--latency-ms", "0", "5")
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Printf("make_demo_db.py failed: %v\n%s", err, out)
			return 1
		}
		return m.Run()
	}())
}

// serve starts the service over a store at path and returns a client
// speaking Connect with JSON, as the browser does.
func serve(t *testing.T, path string, opts store.Options, tweak func(*trace.Service)) ledgerv1connect.TraceServiceClient {
	t.Helper()
	st, err := store.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	svc := trace.New(st)
	svc.PollInterval = 20 * time.Millisecond
	if tweak != nil {
		tweak(svc)
	}
	mux := http.NewServeMux()
	mux.Handle(ledgerv1connect.NewTraceServiceHandler(svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close(); st.Close() })
	return ledgerv1connect.NewTraceServiceClient(srv.Client(), srv.URL, connect.WithProtoJSON())
}

func listRuns(t *testing.T, c ledgerv1connect.TraceServiceClient) []*ledgerv1.RunSummary {
	t.Helper()
	res, err := c.ListRuns(context.Background(), connect.NewRequest(&ledgerv1.ListRunsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	return res.Msg.GetRuns()
}

func find(t *testing.T, runs []*ledgerv1.RunSummary, ok func(*ledgerv1.RunSummary) bool) *ledgerv1.RunSummary {
	t.Helper()
	for _, r := range runs {
		if ok(r) {
			return r
		}
	}
	t.Fatal("no such run in the demo store")
	return nil
}

func code(err error) connect.Code {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Code()
	}
	return connect.CodeUnknown
}

// The summaries agree with `ledger runs`, line for line.
func TestListRunsMatchesTheCLI(t *testing.T) {
	runs := listRuns(t, serve(t, demoDB, store.Options{}, nil))
	cmd := exec.Command("uv", "run", "ledger", "--db", demoDB, "runs")
	cmd.Dir = "../../.."
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != len(runs) {
		t.Fatalf("ledger runs lists %d runs, ListRuns %d", len(lines), len(runs))
	}
	modes := map[ledgerv1.Mode]string{
		ledgerv1.Mode_MODE_RECORD: "record", ledgerv1.Mode_MODE_REPLAY: "replay", ledgerv1.Mode_MODE_FORK: "fork",
	}
	statuses := map[ledgerv1.RunStatus]string{
		ledgerv1.RunStatus_RUN_STATUS_OK: "ok", ledgerv1.RunStatus_RUN_STATUS_ERROR: "error",
		ledgerv1.RunStatus_RUN_STATUS_UNFINISHED: "unfinished",
	}
	for i, r := range runs {
		want := fmt.Sprintf("%s  %s", r.GetRunId(), modes[r.GetMode()])
		if r.GetSourceRunId() != "" {
			want += " of " + r.GetSourceRunId()
		}
		want += fmt.Sprintf("  %s  calls=%d live=%d", statuses[r.GetStatus()], r.GetCalls(), r.GetLive())
		if r.GetDiverged() > 0 {
			want += " diverged=" + strconv.Itoa(int(r.GetDiverged()))
		}
		if r.GetResumed() > 0 {
			want += " resumed=" + strconv.Itoa(int(r.GetResumed()))
		}
		if lines[i] != want {
			t.Errorf("run %d:\n ledger runs: %s\n ListRuns:    %s", i, lines[i], want)
		}
	}
}

func TestSummaries(t *testing.T) {
	runs := listRuns(t, serve(t, demoDB, store.Options{}, nil))
	replay := find(t, runs, func(r *ledgerv1.RunSummary) bool { return r.GetMode() == ledgerv1.Mode_MODE_REPLAY })
	if replay.GetLive() != 0 || replay.GetCalls() == 0 || replay.GetSourceRunId() == "" {
		t.Errorf("replay: %v", replay)
	}
	resumed := find(t, runs, func(r *ledgerv1.RunSummary) bool { return r.GetResumed() > 0 })
	if !strings.HasSuffix(resumed.GetGraph(), "refund_agent:graph_from_env") {
		t.Errorf("resumed run's graph is %q", resumed.GetGraph())
	}
	diagnosed := find(t, runs, func(r *ledgerv1.RunSummary) bool { return r.GetDiagnoses() > 0 })
	if diagnosed.GetDiagnoses() != 1 {
		t.Errorf("diagnosed run has %d diagnoses", diagnosed.GetDiagnoses())
	}
	for _, r := range runs {
		if r.GetStatus() != ledgerv1.RunStatus_RUN_STATUS_OK || r.GetStartedAt() == nil ||
			r.GetFinishedAt() == nil || r.GetFinishedAt().AsTime().Before(r.GetStartedAt().AsTime()) {
			t.Errorf("run %s: status %v, %v .. %v", r.GetRunId(), r.GetStatus(), r.GetStartedAt(), r.GetFinishedAt())
		}
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].GetStartedAt().AsTime().Before(runs[i-1].GetStartedAt().AsTime()) {
			t.Errorf("runs out of order at %d", i)
		}
	}
}

func TestGetRun(t *testing.T) {
	c := serve(t, demoDB, store.Options{}, nil)
	ctx := context.Background()
	runs := listRuns(t, c)
	for _, r := range runs {
		res, err := c.GetRun(ctx, connect.NewRequest(&ledgerv1.GetRunRequest{RunId: r.GetRunId()}))
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(res.Msg.GetSummary(), r) {
			t.Errorf("GetRun summary differs from ListRuns for %s", r.GetRunId())
		}
		events := res.Msg.GetEvents()
		if int32(len(events)) != r.GetEvents() {
			t.Errorf("%s: %d events, summary says %d", r.GetRunId(), len(events), r.GetEvents())
		}
		for i, ev := range events {
			if ev.GetIndex() != int64(i) || ev.GetRunId() != r.GetRunId() {
				t.Errorf("%s: event %d has index %d, run %q", r.GetRunId(), i, ev.GetIndex(), ev.GetRunId())
			}
			for _, p := range store.Payloads(ev) {
				if len(p.GetJson()) == 0 || !json.Valid(p.GetJson()) {
					t.Errorf("%s #%d: payload %s not filled in", r.GetRunId(), i, p.GetSha256())
				}
			}
		}
	}
	_, err := c.GetRun(ctx, connect.NewRequest(&ledgerv1.GetRunRequest{RunId: "nope"}))
	if code(err) != connect.CodeNotFound {
		t.Errorf("unknown run: %v", err)
	}
}

// Payloads over the inline limit come back empty, with their digest, and
// GetPayload serves them.
func TestInlineLimitAndGetPayload(t *testing.T) {
	c := serve(t, demoDB, store.Options{}, func(s *trace.Service) { s.InlineLimit = 1000 })
	ctx := context.Background()
	var left []*ledgerv1.Payload
	for _, r := range listRuns(t, c) {
		res, err := c.GetRun(ctx, connect.NewRequest(&ledgerv1.GetRunRequest{RunId: r.GetRunId()}))
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range res.Msg.GetEvents() {
			for _, p := range store.Payloads(ev) {
				if len(p.GetJson()) > 1000 {
					t.Errorf("payload of %d bytes over a 1000-byte limit", len(p.GetJson()))
				}
				if len(p.GetJson()) == 0 {
					left = append(left, p)
				}
			}
		}
	}
	if len(left) == 0 {
		t.Fatal("no payload was over the limit")
	}
	blobs := 0
	for _, p := range left {
		res, err := c.GetPayload(ctx, connect.NewRequest(&ledgerv1.GetPayloadRequest{Sha256: p.GetSha256()}))
		if code(err) == connect.CodeNotFound {
			continue // inline in the log, just over this test's tiny limit
		}
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(res.Msg.GetPayload().GetJson())
		if hex.EncodeToString(sum[:]) != p.GetSha256() {
			t.Errorf("GetPayload(%s) returned other bytes", p.GetSha256()[:12])
		}
		blobs++
	}
	if blobs == 0 {
		t.Error("no left-out payload was in the blob store")
	}
	for _, digest := range []string{"nope", strings.Repeat("0", 64)} {
		_, err := c.GetPayload(ctx, connect.NewRequest(&ledgerv1.GetPayloadRequest{Sha256: digest}))
		if code(err) != connect.CodeNotFound {
			t.Errorf("GetPayload(%q): %v", digest, err)
		}
	}
}

func TestACorruptBlobIsDataLoss(t *testing.T) {
	dir := t.TempDir()
	db := copyStore(t, dir)
	var digest string
	_ = filepath.WalkDir(db+".blobs", func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && digest == "" {
			digest = filepath.Base(filepath.Dir(p)) + d.Name()
			_ = os.WriteFile(p, []byte(`"tampered"`), 0o644)
		}
		return nil
	})
	c := serve(t, db, store.Options{}, nil)
	_, err := c.GetPayload(context.Background(), connect.NewRequest(&ledgerv1.GetPayloadRequest{Sha256: digest}))
	if code(err) != connect.CodeDataLoss {
		t.Errorf("corrupt blob: %v", err)
	}
}

func TestListDiagnoses(t *testing.T) {
	c := serve(t, demoDB, store.Options{}, nil)
	ctx := context.Background()
	runs := listRuns(t, c)
	diagnosed := find(t, runs, func(r *ledgerv1.RunSummary) bool { return r.GetDiagnoses() > 0 })
	res, err := c.ListDiagnoses(ctx, connect.NewRequest(&ledgerv1.ListDiagnosesRequest{RunId: diagnosed.GetRunId()}))
	if err != nil {
		t.Fatal(err)
	}
	ds := res.Msg.GetDiagnoses()
	if len(ds) != 1 || ds[0].GetRunId() != diagnosed.GetRunId() || ds[0].GetCreatedAt() == nil {
		t.Fatalf("diagnoses: %v", ds)
	}
	var d struct {
		Schema          string `json:"schema"`
		Status          string `json:"status"`
		Step            struct{ Node string } `json:"step"`
		Counterfactuals []struct {
			RunIDs []string `json:"run_ids"`
		} `json:"counterfactuals"`
	}
	if err := json.Unmarshal([]byte(ds[0].GetJson()), &d); err != nil {
		t.Fatal(err)
	}
	if d.Schema != "ledger.diagnosis/1" || d.Status != "found" || d.Step.Node != "research_web" {
		t.Errorf("diagnosis: %+v", d)
	}
	ids := map[string]bool{}
	for _, r := range runs {
		ids[r.GetRunId()] = true
	}
	for _, cf := range d.Counterfactuals {
		for _, id := range cf.RunIDs {
			if !ids[id] {
				t.Errorf("counterfactual run %s is not in the store", id)
			}
		}
	}
	other := find(t, runs, func(r *ledgerv1.RunSummary) bool { return r.GetDiagnoses() == 0 })
	res, err = c.ListDiagnoses(ctx, connect.NewRequest(&ledgerv1.ListDiagnosesRequest{RunId: other.GetRunId()}))
	if err != nil || len(res.Msg.GetDiagnoses()) != 0 {
		t.Errorf("undiagnosed run: %v, %v", res, err)
	}
	_, err = c.ListDiagnoses(ctx, connect.NewRequest(&ledgerv1.ListDiagnosesRequest{RunId: "nope"}))
	if code(err) != connect.CodeNotFound {
		t.Errorf("unknown run: %v", err)
	}
}

func collect(t *testing.T, stream *connect.ServerStreamForClient[ledgerv1.WatchRunResponse], n int) []*ledgerv1.Event {
	t.Helper()
	var got []*ledgerv1.Event
	for len(got) < n && stream.Receive() {
		got = append(got, stream.Msg().GetEvent())
	}
	if len(got) < n {
		t.Fatalf("stream ended after %d of %d events: %v", len(got), n, stream.Err())
	}
	return got
}

// A finished run's stream is its backlog from from_index, then the end.
func TestWatchAFinishedRun(t *testing.T) {
	c := serve(t, demoDB, store.Options{Immutable: true}, nil)
	ctx := context.Background()
	r := listRuns(t, c)[0]
	stream, err := c.WatchRun(ctx, connect.NewRequest(&ledgerv1.WatchRunRequest{RunId: r.GetRunId(), FromIndex: 3}))
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, stream, int(r.GetEvents())-3)
	if got[0].GetIndex() != 3 || got[len(got)-1].GetRunFinished() == nil {
		t.Errorf("stream ran #%d..#%d", got[0].GetIndex(), got[len(got)-1].GetIndex())
	}
	if stream.Receive() || stream.Err() != nil {
		t.Errorf("stream did not end cleanly: %v", stream.Err())
	}
	stream, _ = c.WatchRun(ctx, connect.NewRequest(&ledgerv1.WatchRunRequest{RunId: "nope"}))
	if stream.Receive() || code(stream.Err()) != connect.CodeNotFound {
		t.Errorf("unknown run: %v", stream.Err())
	}
}

// Events appended while a client watches reach it, and the stream ends with
// the run.
func TestWatchARunBeingWritten(t *testing.T) {
	db := copyStore(t, t.TempDir())
	writer, err := sql.Open("sqlite", "file:"+db+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	// Replay a finished run's rows as a new run, as the runtime would log it.
	rows, err := writer.Query("SELECT idx, step_key, kind, data FROM events WHERE run_id = (SELECT run_id FROM events WHERE idx = 0 LIMIT 1) ORDER BY idx")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		idx  int64
		key  sql.NullString
		kind string
		data []byte
	}
	var src []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.idx, &r.key, &r.kind, &r.data); err != nil {
			t.Fatal(err)
		}
		ev := &ledgerv1.Event{}
		if err := proto.Unmarshal(r.data, ev); err != nil {
			t.Fatal(err)
		}
		ev.RunId = "live"
		r.data, _ = proto.Marshal(ev)
		src = append(src, r)
	}
	rows.Close()
	appendRows := func(rs []row) {
		for _, r := range rs {
			if _, err := writer.Exec("INSERT INTO events (run_id, idx, step_key, kind, data) VALUES ('live', ?, ?, ?, ?)",
				r.idx, r.key, r.kind, r.data); err != nil {
				t.Fatal(err)
			}
		}
	}
	half := len(src) / 2
	appendRows(src[:half])

	c := serve(t, db, store.Options{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := c.WatchRun(ctx, connect.NewRequest(&ledgerv1.WatchRunRequest{RunId: "live"}))
	if err != nil {
		t.Fatal(err)
	}
	first := collect(t, stream, half)
	appendRows(src[half : len(src)-1]) // all but RunFinished: the stream stays open
	middle := collect(t, stream, len(src)-1-half)
	appendRows(src[len(src)-1:])
	last := collect(t, stream, 1)
	if last[0].GetRunFinished() == nil {
		t.Errorf("last event is %T", last[0].GetBody())
	}
	if stream.Receive() || stream.Err() != nil {
		t.Errorf("stream did not end with the run: %v", stream.Err())
	}
	for i, ev := range append(append(first, middle...), last...) {
		if ev.GetIndex() != int64(i) {
			t.Fatalf("event %d has index %d", i, ev.GetIndex())
		}
	}
}

// copyStore copies the demo store (database and blobs) into dir.
func copyStore(t *testing.T, dir string) string {
	t.Helper()
	dst := filepath.Join(dir, "demo.db")
	copyFile(t, demoDB, dst)
	err := filepath.WalkDir(demoDB+".blobs", func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(demoDB+".blobs", p)
		copyFile(t, p, filepath.Join(dst+".blobs", rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
}
