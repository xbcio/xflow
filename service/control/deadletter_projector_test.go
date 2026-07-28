package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// fakeReceiptAppender is an in-memory store.ReceiptAuditAppender for projector
// unit tests. It mirrors the SQL provider's check-then-append idempotency:
// the first Project with a given ReceiptAuditID appends; a second is skipped
// (appended=false). A configurable failure function simulates a transient SQL
// outage so the retry/backlog path is exercised.
type fakeReceiptAppender struct {
	mu       sync.Mutex
	rows     []*store.AuditRecord
	failNext func(rec *store.AuditRecord) error
}

var _ store.ReceiptAuditAppender = (*fakeReceiptAppender)(nil)

func (f *fakeReceiptAppender) AppendAudit(ctx context.Context, rec *store.AuditRecord) error {
	return f.AppendAuditIfAbsentCompat(ctx, rec)
}

// AppendAuditIfAbsentCompat mirrors the SQL provider's idempotent append for
// the fake. Used by AppendAudit too so a plain append still respects the
// idempotency key (the projector only calls AppendAuditIfAbsent).
func (f *fakeReceiptAppender) AppendAuditIfAbsentCompat(ctx context.Context, rec *store.AuditRecord) error {
	_, err := f.AppendAuditIfAbsent(ctx, rec)
	return err
}

func (f *fakeReceiptAppender) AppendAuditIfAbsent(_ context.Context, rec *store.AuditRecord) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec.ReceiptAuditID != "" {
		for _, r := range f.rows {
			if r.ReceiptAuditID == rec.ReceiptAuditID {
				return false, nil
			}
		}
	}
	if f.failNext != nil {
		if err := f.failNext(rec); err != nil {
			return false, err
		}
	}
	cp := *rec
	f.rows = append(f.rows, &cp)
	return true, nil
}

func (f *fakeReceiptAppender) AuditByReceiptAuditID(_ context.Context, receiptAuditID string) (*store.AuditRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rows {
		if r.ReceiptAuditID == receiptAuditID {
			return r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeReceiptAppender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func (f *fakeReceiptAppender) snapshot() []*store.AuditRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*store.AuditRecord, len(f.rows))
	copy(out, f.rows)
	return out
}

// captureOutboxObserver and its countErrors helper are defined in
// deadletter_manager_test.go (shared).

// receiptForTest builds a canonical receipt used across the projector tests.
func receiptForTest() engine.ReplayReceipt {
	return engine.ReplayReceipt{
		Namespace:    "namespace-a",
		ExecutionID:  "exec-1",
		RequestID:    "req-1",
		AuditID:      "req-1:1700000000000",
		NodeID:       "review",
		ActivationID: "1",
		Outcome:      engine.ReplayReplayed,
		Operator:     "alice",
		Reason:       "root cause fixed",
		EntryID:      "entry-1",
		TimestampMs:  1700000000000,
	}
}

// TestReceiptProjectorProjectsOnceAndSkipsDuplicate proves the core idempotency
// contract: the first Project appends, the second is a no-op. A retry after a
// lost SQL write, a process restart mid-reconcile, or a duplicate projection
// from the manager path never appends a second row.
func TestReceiptProjectorProjectsOnceAndSkipsDuplicate(t *testing.T) {
	appender := &fakeReceiptAppender{}
	p := NewReceiptProjector(appender)
	r := receiptForTest()

	appended, err := p.Project(context.Background(), r)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if !appended {
		t.Fatal("first Project: appended=false, want true")
	}
	if got := appender.count(); got != 1 {
		t.Fatalf("rows after first project = %d, want 1", got)
	}
	appended2, err := p.Project(context.Background(), r)
	if err != nil {
		t.Fatalf("second Project: %v", err)
	}
	if appended2 {
		t.Fatal("second Project: appended=true, want false (idempotent skip)")
	}
	if got := appender.count(); got != 1 {
		t.Fatalf("rows after duplicate project = %d, want 1 (idempotent)", got)
	}
}

// TestReceiptProjectorDoesNotPersistReason asserts the security property: the
// operator's free-text Reason is bounded but untrusted, so the durable SQL
// projection omits it. The Redis receipt retains the reason; the SQL row is
// metadata-only.
func TestReceiptProjectorDoesNotPersistReason(t *testing.T) {
	appender := &fakeReceiptAppender{}
	p := NewReceiptProjector(appender)
	r := receiptForTest()
	r.Reason = "sensitive-looking operator rationale with maybe a token: abc123"

	if _, err := p.Project(context.Background(), r); err != nil {
		t.Fatalf("Project: %v", err)
	}
	rows := appender.snapshot()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Reason == r.Reason {
		t.Fatalf("durable projection persisted the operator free-text reason %q (security: reason must stay in Redis only)", rows[0].Reason)
	}
	if rows[0].Reason != "replay_receipt" {
		t.Fatalf("reason code = %q, want replay_receipt", rows[0].Reason)
	}
	// The receipt audit_id (idempotency key) and node/activation/entry
	// correlation fields must be projected so the reconcile diff-scan can
	// join them.
	if rows[0].ReceiptAuditID != r.AuditID {
		t.Fatalf("receipt_audit_id = %q, want %q", rows[0].ReceiptAuditID, r.AuditID)
	}
	if rows[0].NodeID != r.NodeID || rows[0].ActivationID != r.ActivationID || rows[0].EntryID != r.EntryID {
		t.Fatalf("correlation fields not projected: %+v", rows[0])
	}
	if rows[0].Outcome != string(engine.ReplayReplayed) {
		t.Fatalf("outcome = %q, want replayed", rows[0].Outcome)
	}
}

// TestProjectorAuditSinkRetriesThenAlarmsOnTransientFailure exercises the
// retry/backlog + alarm path: a transient SQL failure is retried in-request,
// and when all retries exhaust the failure is emitted via OnOutboxError so it
// is observable as an alarm. The Redis receipt remains authoritative; a later
// reconcile re-projects it.
func TestProjectorAuditSinkRetriesThenAlarmsOnTransientFailure(t *testing.T) {
	appender := &fakeReceiptAppender{
		failNext: func(*store.AuditRecord) error { return errors.New("sql transient boom") },
	}
	obs := &captureOutboxObserver{}
	sink := NewProjectorAuditSink(NewReceiptProjector(appender), obs)
	// Speed up the retry backoff so the test does not sleep for seconds.
	s := sink.(*projectorAuditSink)
	s.sleep = func(time.Duration) {}
	s.maxRetry = 2

	res := engine.ReplayDeadLetterResult{
		Outcome:      engine.ReplayReplayed,
		AuditID:      "req-1:1700000000000",
		ExecutionID:  "exec-1",
		NodeID:       "review",
		ActivationID: "1",
	}
	req := engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-1", EntryID: "entry-1", RequestID: "req-1", Reason: "r", Operator: "alice",
	}
	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	err := sink.RecordReplay(ctx, res, req)
	if err == nil {
		t.Fatal("RecordReplay: err = nil, want transient failure propagated after retries")
	}
	if obs.countErrors("replay_project") == 0 {
		t.Fatal("OnOutboxError not emitted for projection failure (alarm missing)")
	}
	if appender.count() != 0 {
		t.Fatalf("rows = %d, want 0 (all retries failed, nothing projected)", appender.count())
	}
}

// TestProjectorAuditSinkSucceedsAfterTransientRetry proves the retry path
// recovers: the first attempt fails, the second succeeds, and no alarm is
// emitted.
func TestProjectorAuditSinkSucceedsAfterTransientRetry(t *testing.T) {
	var attempts int
	var mu sync.Mutex
	appender := &fakeReceiptAppender{
		failNext: func(*store.AuditRecord) error {
			mu.Lock()
			attempts++
			mu.Unlock()
			if attempts == 1 {
				return errors.New("sql transient boom")
			}
			return nil
		},
	}
	obs := &captureOutboxObserver{}
	sink := NewProjectorAuditSink(NewReceiptProjector(appender), obs)
	s := sink.(*projectorAuditSink)
	s.sleep = func(time.Duration) {}

	res := engine.ReplayDeadLetterResult{
		Outcome: engine.ReplayReplayed, AuditID: "req-1:1700000000000",
		ExecutionID: "exec-1", NodeID: "review", ActivationID: "1",
	}
	req := engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-1", EntryID: "entry-1", RequestID: "req-1", Reason: "r", Operator: "alice",
	}
	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	if err := sink.RecordReplay(ctx, res, req); err != nil {
		t.Fatalf("RecordReplay: %v (should succeed on retry)", err)
	}
	if obs.countErrors("replay_project") != 0 {
		t.Fatal("alarm emitted despite eventual success")
	}
	if appender.count() != 1 {
		t.Fatalf("rows = %d, want 1 (projected after retry)", appender.count())
	}
}

// TestProjectorAuditSinkSkipsWhenAlreadyProjected proves a duplicate
// projection (e.g. the manager path and the reconcile command both try) is a
// no-op: the second RecordReplay finds the row already present and skips.
func TestProjectorAuditSinkSkipsWhenAlreadyProjected(t *testing.T) {
	appender := &fakeReceiptAppender{}
	obs := &captureOutboxObserver{}
	sink := NewProjectorAuditSink(NewReceiptProjector(appender), obs)
	s := sink.(*projectorAuditSink)
	s.sleep = func(time.Duration) {}

	res := engine.ReplayDeadLetterResult{
		Outcome: engine.ReplayReplayed, AuditID: "req-1:1700000000000",
		ExecutionID: "exec-1", NodeID: "review", ActivationID: "1",
	}
	req := engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-1", EntryID: "entry-1", RequestID: "req-1", Reason: "r", Operator: "alice",
	}
	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	if err := sink.RecordReplay(ctx, res, req); err != nil {
		t.Fatalf("first RecordReplay: %v", err)
	}
	// Second call with the same receipt (same AuditID) must skip, not append.
	if err := sink.RecordReplay(ctx, res, req); err != nil {
		t.Fatalf("second RecordReplay: %v", err)
	}
	if appender.count() != 1 {
		t.Fatalf("rows = %d, want 1 (duplicate projection skipped)", appender.count())
	}
	if obs.countErrors("replay_project") != 0 {
		t.Fatal("alarm emitted for a benign duplicate projection")
	}
}

// TestReceiptFromReplayCarriesIdempotencyKey confirms the manager-path receipt
// carries the same AuditID the Redis receipt would, so a projection from the
// manager path and a projection from the reconcile diff-scan collapse to one
// row.
func TestReceiptFromReplayCarriesIdempotencyKey(t *testing.T) {
	res := engine.ReplayDeadLetterResult{AuditID: "req-1:1700000000000", ExecutionID: "exec-1", NodeID: "n", ActivationID: "1", Outcome: engine.ReplayReplayed}
	req := engine.ReplayDeadLetterRequest{ExecutionID: "exec-1", EntryID: "e1", RequestID: "req-1", Reason: "r", Operator: "alice"}
	r := receiptFromReplay(res, req, "namespace-a")
	if r.AuditID != "req-1:1700000000000" {
		t.Fatalf("audit_id = %q, want the Redis receipt audit_id", r.AuditID)
	}
	if r.Namespace != "namespace-a" {
		t.Fatalf("namespace = %q, want namespace-a", r.Namespace)
	}
	if r.EntryID != "e1" || r.NodeID != "n" || r.ActivationID != "1" {
		t.Fatalf("correlation fields not carried: %+v", r)
	}
}

// TestReceiptProjectorEmptyAuditIDIsAlwaysAppended confirms that a record
// without an idempotency key (an admission/outcome row, not a receipt) is
// always appended — the idempotency guard only applies to receipt projections.
func TestReceiptProjectorEmptyAuditIDIsAlwaysAppended(t *testing.T) {
	appender := &fakeReceiptAppender{}
	p := NewReceiptProjector(appender)
	r := receiptForTest()
	r.AuditID = ""

	if _, err := p.Project(context.Background(), r); err != nil {
		t.Fatalf("first Project: %v", err)
	}
	if _, err := p.Project(context.Background(), r); err != nil {
		t.Fatalf("second Project: %v", err)
	}
	if got := appender.count(); got != 2 {
		t.Fatalf("rows = %d, want 2 (no idempotency key → always append)", got)
	}
}

// TestReceiptProjectorNilAppenderReturnsError confirms a nil/ unavailable
// appender surfaces an error rather than silently dropping the projection.
func TestReceiptProjectorNilAppenderReturnsError(t *testing.T) {
	p := NewReceiptProjector(nil)
	_, err := p.Project(context.Background(), receiptForTest())
	if err == nil {
		t.Fatal("Project with nil appender: err = nil, want error")
	}
	if err.Error() == "" {
		t.Fatal("Project with nil appender returned an empty error")
	}
}
