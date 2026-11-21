// Package trace implements ledger.v1.TraceService over a read-only store.
// It must answer exactly as `ledger ui` (ledger/ui/server.py) does.
package trace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/amansriven/Ledger/server/gen/ledgerv1"
	"github.com/amansriven/Ledger/server/gen/ledgerv1/ledgerv1connect"
	"github.com/amansriven/Ledger/server/internal/store"
)

// DefaultInlineLimit is the largest payload, in bytes, returned inside an
// event (256 KiB, as the proto says); bigger ones are fetched with
// GetPayload.
const DefaultInlineLimit = 256 << 10

// Service answers TraceService requests from a store.
type Service struct {
	Store *store.Store
	// PollInterval is how often WatchRun checks for new events.
	PollInterval time.Duration
	// InlineLimit is DefaultInlineLimit outside tests.
	InlineLimit int
}

var _ ledgerv1connect.TraceServiceHandler = (*Service)(nil)

// New returns a service polling every 200 ms.
func New(s *store.Store) *Service {
	return &Service{Store: s, PollInterval: 200 * time.Millisecond, InlineLimit: DefaultInlineLimit}
}

func (s *Service) ListRuns(ctx context.Context, _ *connect.Request[ledgerv1.ListRunsRequest]) (*connect.Response[ledgerv1.ListRunsResponse], error) {
	runs, err := s.Store.Runs(ctx)
	if err != nil {
		return nil, internal(err)
	}
	counts, err := s.Store.DiagnosisCounts(ctx)
	if err != nil {
		return nil, internal(err)
	}
	out := &ledgerv1.ListRunsResponse{Runs: make([]*ledgerv1.RunSummary, 0, len(runs))}
	for _, r := range runs {
		out.Runs = append(out.Runs, Summarize(r.ID, r.Events, counts[r.ID]))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) GetRun(ctx context.Context, req *connect.Request[ledgerv1.GetRunRequest]) (*connect.Response[ledgerv1.GetRunResponse], error) {
	id := req.Msg.GetRunId()
	events, err := s.Store.Events(ctx, id, 0)
	if err != nil {
		return nil, internal(err)
	}
	if len(events) == 0 {
		return nil, noRun(id)
	}
	diags, err := s.Store.Diagnoses(ctx, id)
	if err != nil {
		return nil, internal(err)
	}
	sum := Summarize(id, events, int32(len(diags)))
	for _, ev := range events {
		if err := s.Store.Hydrate(ev, s.InlineLimit); err != nil {
			return nil, internal(err)
		}
	}
	return connect.NewResponse(&ledgerv1.GetRunResponse{Summary: sum, Events: events}), nil
}

func (s *Service) WatchRun(ctx context.Context, req *connect.Request[ledgerv1.WatchRunRequest], stream *connect.ServerStream[ledgerv1.WatchRunResponse]) error {
	id, from := req.Msg.GetRunId(), req.Msg.GetFromIndex()
	if from < 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("from_index must not be negative"))
	}
	w, err := s.Store.Watch(ctx)
	if err != nil {
		return internal(err)
	}
	defer w.Close()

	// The status needs the whole log, so read from 0 and send from `from`.
	st := newStatus()
	next := int64(0)
	send := func(events []*ledgerv1.Event) error {
		for _, ev := range events {
			st.add(ev)
			next = ev.GetIndex() + 1
			if ev.GetIndex() < from {
				continue
			}
			if err := s.Store.Hydrate(ev, s.InlineLimit); err != nil {
				return internal(err)
			}
			if err := stream.Send(&ledgerv1.WatchRunResponse{Event: ev}); err != nil {
				return err
			}
		}
		return nil
	}

	version, err := w.DataVersion(ctx)
	if err != nil {
		return internal(err)
	}
	backlog, err := w.Events(ctx, id, 0)
	if err != nil {
		return internal(err)
	}
	if len(backlog) == 0 {
		return noRun(id)
	}
	if err := send(backlog); err != nil {
		return err
	}
	tick := time.NewTicker(s.PollInterval)
	defer tick.Stop()
	for !st.done() {
		select {
		case <-ctx.Done():
			return nil // the client went away
		case <-tick.C:
		}
		v, err := w.DataVersion(ctx)
		if err != nil {
			return internal(err)
		}
		if v == version {
			continue
		}
		version = v
		events, err := w.Events(ctx, id, next)
		if err != nil {
			return internal(err)
		}
		if err := send(events); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) GetPayload(_ context.Context, req *connect.Request[ledgerv1.GetPayloadRequest]) (*connect.Response[ledgerv1.GetPayloadResponse], error) {
	digest := req.Msg.GetSha256()
	data, err := s.Store.Blob(digest)
	if errors.Is(err, store.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no payload %q", digest))
	}
	if errors.Is(err, store.ErrCorrupt) {
		return nil, connect.NewError(connect.CodeDataLoss, err)
	}
	if err != nil {
		return nil, internal(err)
	}
	return connect.NewResponse(&ledgerv1.GetPayloadResponse{
		Payload: &ledgerv1.Payload{Sha256: digest, Json: data},
	}), nil
}

func (s *Service) ListDiagnoses(ctx context.Context, req *connect.Request[ledgerv1.ListDiagnosesRequest]) (*connect.Response[ledgerv1.ListDiagnosesResponse], error) {
	id := req.Msg.GetRunId()
	ok, err := s.Store.HasRun(ctx, id)
	if err != nil {
		return nil, internal(err)
	}
	if !ok {
		return nil, noRun(id)
	}
	diags, err := s.Store.Diagnoses(ctx, id)
	if err != nil {
		return nil, internal(err)
	}
	out := &ledgerv1.ListDiagnosesResponse{Diagnoses: make([]*ledgerv1.Diagnosis, 0, len(diags))}
	for _, d := range diags {
		out.Diagnoses = append(out.Diagnoses, d.ToProto())
	}
	return connect.NewResponse(out), nil
}

func noRun(id string) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("no run %q", id))
}

func internal(err error) error {
	return connect.NewError(connect.CodeInternal, err)
}
