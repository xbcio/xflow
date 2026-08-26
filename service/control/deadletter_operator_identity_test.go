package control

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// recordingReplayStore keeps the request it was handed. The package's other
// fake (fakeDeadLetterStore) writes `_ = req` and throws it away, which is why
// no existing test can see what the manager actually sends downstream.
type recordingReplayStore struct {
	mu  sync.Mutex
	got []engine.ReplayDeadLetterRequest
}

func (s *recordingReplayStore) ListDeadLetters(context.Context, types.ExecutionID, engine.DeadLetterPage) (engine.DeadLetterList, error) {
	return engine.DeadLetterList{}, nil
}

func (s *recordingReplayStore) ReplayDeadLetter(_ context.Context, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	s.mu.Lock()
	s.got = append(s.got, req)
	s.mu.Unlock()
	return engine.ReplayDeadLetterResult{
		Outcome:      engine.ReplayReplayed,
		AuditID:      "audit-forgery-1",
		ExecutionID:  req.ExecutionID,
		NodeID:       "review",
		ActivationID: "7",
	}, nil
}

// recordingReceiptAppender is the durable end of the chain: whatever lands here
// is what an auditor reads back out of xflow_audit_events.
type recordingReceiptAppender struct {
	mu   sync.Mutex
	rows []store.AuditRecord
}

func (a *recordingReceiptAppender) AppendAuditIfAbsent(_ context.Context, rec *store.AuditRecord) (bool, error) {
	a.mu.Lock()
	a.rows = append(a.rows, *rec)
	a.mu.Unlock()
	return true, nil
}

func (a *recordingReceiptAppender) AuditByReceiptAuditID(context.Context, string) (*store.AuditRecord, error) {
	return nil, store.ErrNotFound
}

func (a *recordingReceiptAppender) only(t *testing.T) store.AuditRecord {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.rows) != 1 {
		t.Fatalf("projected %d durable audit rows, want exactly 1", len(a.rows))
	}
	return a.rows[0]
}

// TestReplayRecordsTheAuthenticatedPrincipalNotTheRequestedOperator drives the
// whole identity chain a dead-letter replay writes: Replay injects the operator
// (deadletter_manager.go:88), finish hands the request to the audit sink,
// projectorAuditSink calls receiptFromReplay (deadletter_projector.go:108,
// Operator: req.Operator), and receiptToAuditRecord maps it to the durable row
// (deadletter_projector.go:75, Principal: r.Operator). That row is the answer
// to "who authorized this replay".
//
// Nothing along it is pinned today. Three separate mutations survive the whole
// service/control package:
//
//	deadletter_manager.go:88    delete `req.Operator = principal.Subject`
//	deadletter_projector.go:108 `Operator: req.Operator` -> `Operator: ""`
//	deadletter_projector.go:75  `Principal: r.Operator` -> `Principal: ""`
//
// The first is the serious one. Its own comment states the property — "req.
// Operator is ignored so callers cannot self-report identity" — and deleting
// the line lets an `"operator"` field in the HTTP request body flow through to
// the durable audit row unmodified. Replaying a dead letter re-runs a node that
// already failed; the identity of whoever ordered it is the entire point of
// recording the replay. A caller able to write that field can pin the action on
// anyone.
//
// The other two lose the identity instead of forging it. That is milder and
// noisier, but it is permanent: this table is the durable secondary projection
// the reconcile diff-scan converges on, and reconcile re-derives Principal from
// the same receipt, so the blank is what it backfills too.
//
// Existing coverage misses all three because the package's fakes discard the
// evidence. fakeDeadLetterStore.ReplayDeadLetter writes `_ = req`;
// fakeAuditSink.RecordReplay takes `_ engine.ReplayDeadLetterRequest`; and
// grepping the two dead-letter test files for `.Principal` returns nothing.
// service/apiserver/deadletter_unified_test.go does have a "forged operator"
// case, but it asserts the apiserver's own admission audit event, whose
// Principal has always come from the authenticated principal by a different
// route — and in that test the execution 404s, so Replay returns at the store
// error branch and never reaches finish at all.
func TestReplayRecordsTheAuthenticatedPrincipalNotTheRequestedOperator(t *testing.T) {
	const (
		authenticated = "cli:alice"
		forged        = "admin-bob-who-never-touched-this"
		ns            = "tenant-a"
	)

	st := &recordingReplayStore{}
	appender := &recordingReceiptAppender{}
	sink := NewProjectorAuditSink(NewReceiptProjector(appender), nil)
	mgr := NewDeadLetterManager(st, nil, sink)

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace(ns))
	principal := DeadLetterReplayPrincipal{
		Subject:   authenticated,
		Namespace: ns,
		Scopes:    []string{ScopeDeadLetterReplay},
	}

	res, err := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-1",
		EntryID:     "entry-1",
		RequestID:   "req-1",
		Reason:      "downstream recovered",
		// The request body claims to be someone else. This is the only field a
		// caller controls that names a person.
		Operator: forged,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want %q", res.Outcome, engine.ReplayReplayed)
	}

	st.mu.Lock()
	sent := append([]engine.ReplayDeadLetterRequest(nil), st.got...)
	st.mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("store saw %d replay requests, want 1", len(sent))
	}
	if sent[0].Operator != authenticated {
		t.Fatalf("the request reaching the store carries Operator %q, want %q: the "+
			"operator must be replaced with the authenticated subject before the "+
			"store writes the authoritative Redis receipt, not after",
			sent[0].Operator, authenticated)
	}

	row := appender.only(t)
	if row.Principal != authenticated {
		t.Fatalf("durable audit row Principal = %q, want %q: this row is the answer "+
			"to who authorized replaying a node that already failed, and it is what "+
			"the reconcile diff-scan converges on", row.Principal, authenticated)
	}

	// The forged value must not survive anywhere in the row, not just in the
	// field that was checked. A name absent from Principal but present in
	// Resource or Reason still reads as attribution to a human eye and to any
	// downstream text search.
	if field := findStringField(row, forged); field != "" {
		t.Fatalf("the operator string supplied by the caller survived into durable "+
			"audit field %s; a caller must not be able to write any part of the "+
			"identity record", field)
	}

	// Correlation must still be intact — an audit row that is anonymous *and*
	// unlinkable would satisfy the assertions above.
	if row.ReceiptAuditID != res.AuditID {
		t.Fatalf("row.ReceiptAuditID = %q, want the receipt's %q", row.ReceiptAuditID, res.AuditID)
	}
	if row.ExecutionID != "exec-1" || row.EntryID != "entry-1" || row.Namespace != ns {
		t.Fatalf("row correlation = {exec %q entry %q ns %q}, want {exec-1 entry-1 %s}",
			row.ExecutionID, row.EntryID, row.Namespace, ns)
	}
}

// findStringField returns the name of the first string field of rec whose value
// contains want, or "" if none does. Reflection rather than a hand-written list
// so a field added later is scanned without anyone remembering to add it here.
func findStringField(rec store.AuditRecord, want string) string {
	v := reflect.ValueOf(rec)
	for i := range v.NumField() {
		f := v.Field(i)
		if f.Kind() == reflect.String && strings.Contains(f.String(), want) {
			return v.Type().Field(i).Name
		}
	}
	return ""
}
