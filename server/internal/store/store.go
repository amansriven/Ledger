// Package store reads the event log the Python runtime writes: a SQLite
// database in WAL mode (ledger/core/store.py) and the content-addressed blob
// directory next to it (ledger/core/blobs.py). It never writes.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	_ "modernc.org/sqlite"

	ledgerv1 "github.com/amansriven/Ledger/server/gen/ledgerv1"
)

var (
	// ErrNotFound: no such run, diagnosis or blob.
	ErrNotFound = errors.New("not found")
	// ErrCorrupt: a blob whose bytes don't match its digest.
	ErrCorrupt = errors.New("blob does not match its digest")
)

var digestRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Options for Open.
type Options struct {
	// Immutable opens the database as a file that never changes (SQLite's
	// immutable=1): no locks and no -shm file, for a read-only copy served
	// from a read-only disk. Watching a run then never sees new events.
	Immutable bool
}

// Store is a read-only view of one event store.
type Store struct {
	db    *sql.DB
	blobs string
	path  string
}

// Open opens the store at path (the .db file) read-only. The database must
// exist.
func Open(path string, opt Options) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, fmt.Errorf("event store %s: %w", path, err)
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	if opt.Immutable {
		q.Set("immutable", "1")
		q.Set("mode", "ro")
	} else {
		// Not mode=ro: reading a WAL database read-only needs its -shm file
		// to exist already. query_only keeps this connection from writing.
		q.Set("mode", "rw")
		q.Add("_pragma", "query_only(1)")
	}
	dsn := "file:" + (&url.URL{Path: abs}).EscapedPath() + "?" + q.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("event store %s: %w", path, err)
	}
	return &Store{db: db, blobs: abs + ".blobs", path: abs}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Path is the database file's absolute path.
func (s *Store) Path() string { return s.path }

// Run is one run's events, in log order, as stored (big payloads not
// filled in; see Hydrate).
type Run struct {
	ID     string
	Events []*ledgerv1.Event
}

// Runs returns every run, ordered as `ledger runs` orders them: by the
// recorded_at of its first event, then by id.
func (s *Store) Runs(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT run_id, data FROM events ORDER BY run_id, idx")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		ev, err := decode(data)
		if err != nil {
			return nil, fmt.Errorf("run %s: %w", id, err)
		}
		if len(runs) == 0 || runs[len(runs)-1].ID != id {
			runs = append(runs, Run{ID: id})
		}
		r := &runs[len(runs)-1]
		r.Events = append(r.Events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortRuns(runs)
	return runs, nil
}

func sortRuns(runs []Run) {
	start := func(r Run) int64 {
		if len(r.Events) == 0 {
			return 0
		}
		return r.Events[0].GetRecordedAt().AsTime().UnixNano()
	}
	// The query returns runs by id; a stable sort keeps that for ties.
	sort.SliceStable(runs, func(i, j int) bool { return start(runs[i]) < start(runs[j]) })
}

// Events returns the run's events with index >= from, in log order. A run
// with no events at all is ErrNotFound.
func (s *Store) Events(ctx context.Context, runID string, from int64) ([]*ledgerv1.Event, error) {
	return eventsFrom(ctx, s.db, runID, from)
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func eventsFrom(ctx context.Context, q querier, runID string, from int64) ([]*ledgerv1.Event, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT data FROM events WHERE run_id = ? AND idx >= ? ORDER BY idx", runID, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ledgerv1.Event
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		ev, err := decode(data)
		if err != nil {
			return nil, fmt.Errorf("run %s: %w", runID, err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// HasRun reports whether the run has any events.
func (s *Store) HasRun(ctx context.Context, runID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events WHERE run_id = ? AND idx = 0", runID).Scan(&n)
	return n > 0, err
}

func decode(data []byte) (*ledgerv1.Event, error) {
	ev := &ledgerv1.Event{}
	if err := proto.Unmarshal(data, ev); err != nil {
		return nil, err
	}
	return ev, nil
}

// Diagnosis is one saved diagnosis (ledger diagnose).
type Diagnosis struct {
	ID        int64
	RunID     string
	CreatedAt time.Time
	JSON      string
}

// ToProto converts it for the API.
func (d Diagnosis) ToProto() *ledgerv1.Diagnosis {
	return &ledgerv1.Diagnosis{
		Id: d.ID, RunId: d.RunID, CreatedAt: timestamppb.New(d.CreatedAt), Json: d.JSON,
	}
}

// Diagnoses returns the diagnoses saved for a run, oldest first. Stores
// written before diagnoses were saved have no table; they have none.
func (s *Store) Diagnoses(ctx context.Context, runID string) ([]Diagnosis, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, created_at, data FROM diagnoses WHERE run_id = ? ORDER BY id", runID)
	if isNoTable(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Diagnosis
	for rows.Next() {
		d := Diagnosis{RunID: runID}
		var created string
		if err := rows.Scan(&d.ID, &created, &d.JSON); err != nil {
			return nil, err
		}
		if d.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			return nil, fmt.Errorf("diagnosis %d: created_at %q: %w", d.ID, created, err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DiagnosisCounts returns how many diagnoses each run has.
func (s *Store) DiagnosisCounts(ctx context.Context) (map[string]int32, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT run_id, COUNT(*) FROM diagnoses GROUP BY run_id")
	if isNoTable(err) {
		return map[string]int32{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

func isNoTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

// Blob returns a blob's bytes, checked against its digest.
func (s *Store) Blob(digest string) ([]byte, error) {
	if !digestRE.MatchString(digest) {
		return nil, fmt.Errorf("%w: not a SHA-256 hex digest: %q", ErrNotFound, digest)
	}
	data, err := os.ReadFile(s.blobPath(digest))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("blob %s: %w", digest[:12], ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, fmt.Errorf("blob %s: %w", digest[:12], ErrCorrupt)
	}
	return data, nil
}

// BlobSize returns a blob's size without reading it.
func (s *Store) BlobSize(digest string) (int64, error) {
	if !digestRE.MatchString(digest) {
		return 0, ErrNotFound
	}
	fi, err := os.Stat(s.blobPath(digest))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (s *Store) blobPath(digest string) string {
	return filepath.Join(s.blobs, digest[:2], digest[2:])
}

// Payloads returns every Payload set on the event, as
// ledger.core.store.payloads does.
func Payloads(ev *ledgerv1.Event) []*ledgerv1.Payload {
	var ps []*ledgerv1.Payload
	add := func(p ...*ledgerv1.Payload) {
		for _, x := range p {
			if x != nil {
				ps = append(ps, x)
			}
		}
	}
	switch b := ev.GetBody().(type) {
	case *ledgerv1.Event_RunStarted:
		add(b.RunStarted.GetInput())
	case *ledgerv1.Event_RunFinished:
		add(b.RunFinished.GetOutput())
	case *ledgerv1.Event_LlmCall:
		add(b.LlmCall.GetRequest(), b.LlmCall.GetResponse())
	case *ledgerv1.Event_ToolCall:
		add(b.ToolCall.GetArgs(), b.ToolCall.GetResult())
	case *ledgerv1.Event_Nondet:
		add(b.Nondet.GetValue())
	case *ledgerv1.Event_Effect:
		add(b.Effect.GetArgs(), b.Effect.GetResult())
	}
	return ps
}

// Hydrate fills in the event's payloads that live in the blob store, up to
// limit bytes each. Payloads bigger than limit are left (or made) empty,
// with their digest set, for the reader to fetch on demand.
func (s *Store) Hydrate(ev *ledgerv1.Event, limit int) error {
	for _, p := range Payloads(ev) {
		if len(p.Json) > limit {
			p.Json = nil
			continue
		}
		if len(p.Json) > 0 || p.Sha256 == "" {
			continue
		}
		size, err := s.BlobSize(p.Sha256)
		if err != nil {
			return fmt.Errorf("payload %s: %w", p.Sha256, err)
		}
		if size > int64(limit) {
			continue
		}
		if p.Json, err = s.Blob(p.Sha256); err != nil {
			return err
		}
	}
	return nil
}

// Watcher follows one database for changes on a connection of its own, as
// SQLite's data_version is per connection.
type Watcher struct {
	conn *sql.Conn
}

// Watch pins a connection for change detection. Close it when done.
func (s *Store) Watch(ctx context.Context) (*Watcher, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Watcher{conn: conn}, nil
}

// DataVersion changes whenever another connection commits to the database.
func (w *Watcher) DataVersion(ctx context.Context) (int64, error) {
	var v int64
	err := w.conn.QueryRowContext(ctx, "PRAGMA data_version").Scan(&v)
	return v, err
}

// Events is Store.Events on the watcher's connection.
func (w *Watcher) Events(ctx context.Context, runID string, from int64) ([]*ledgerv1.Event, error) {
	return eventsFrom(ctx, w.conn, runID, from)
}

func (w *Watcher) Close() error { return w.conn.Close() }
