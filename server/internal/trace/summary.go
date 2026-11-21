package trace

import (
	ledgerv1 "github.com/amansriven/Ledger/server/gen/ledgerv1"
)

// isCall: an LLM call, tool call or effect outcome (not an effect's PENDING
// record). Mirrors _is_call in ledger/cli.py.
func isCall(ev *ledgerv1.Event) bool {
	switch b := ev.GetBody().(type) {
	case *ledgerv1.Event_LlmCall, *ledgerv1.Event_ToolCall:
		return true
	case *ledgerv1.Event_Effect:
		return b.Effect.GetPhase() != ledgerv1.EffectCall_PHASE_PENDING
	}
	return false
}

// status follows ledger.replay.resume.run_status: only the last RunFinished
// after the last RunResumed counts.
type status struct {
	code     ledgerv1.RunStatus
	err      *ledgerv1.Error
	finished *ledgerv1.Event
}

func (s *status) add(ev *ledgerv1.Event) {
	switch b := ev.GetBody().(type) {
	case *ledgerv1.Event_RunResumed:
		*s = status{code: ledgerv1.RunStatus_RUN_STATUS_UNFINISHED}
	case *ledgerv1.Event_RunFinished:
		s.finished = ev
		if b.RunFinished.GetError() != nil {
			s.code, s.err = ledgerv1.RunStatus_RUN_STATUS_ERROR, b.RunFinished.GetError()
		} else {
			s.code, s.err = ledgerv1.RunStatus_RUN_STATUS_OK, nil
		}
	}
}

func newStatus() status { return status{code: ledgerv1.RunStatus_RUN_STATUS_UNFINISHED} }

func (s status) done() bool { return s.code != ledgerv1.RunStatus_RUN_STATUS_UNFINISHED }

// Summarize builds a run's summary from its whole log, as `ledger runs`
// describes it.
func Summarize(runID string, events []*ledgerv1.Event, diagnoses int32) *ledgerv1.RunSummary {
	sum := &ledgerv1.RunSummary{
		RunId:     runID,
		Events:    int32(len(events)),
		Diagnoses: diagnoses,
	}
	if len(events) > 0 {
		sum.StartedAt = events[0].GetRecordedAt()
		if rs := events[0].GetRunStarted(); rs != nil {
			sum.Mode, sum.SourceRunId, sum.Graph = rs.GetMode(), rs.GetSourceRunId(), rs.GetGraph()
		}
	}
	st := newStatus()
	for _, ev := range events {
		st.add(ev)
		if isCall(ev) {
			sum.Calls++
			if !ev.GetFromLog() {
				sum.Live++
			}
		}
		switch ev.GetBody().(type) {
		case *ledgerv1.Event_Diverged:
			sum.Diverged++
		case *ledgerv1.Event_RunResumed:
			sum.Resumed++
		}
	}
	sum.Status, sum.Error = st.code, st.err
	if st.done() {
		sum.FinishedAt = st.finished.GetRecordedAt()
	}
	return sum
}
