package apiserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// failingAuditAppender always errors, so a SQLAuditSink over it must surface
// the failure (mutations fail-closed on this).
type failingAuditAppender struct{}

func (failingAuditAppender) AppendAudit(context.Context, *store.AuditRecord) error {
	return errors.New("db down")
}

func TestSQLAuditSinkPersistsEvent(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ev := AuditEvent{
		RequestID:  "req-1",
		Principal:  "alice",
		Namespace:  "namespace-a",
		Operation:  OpWorkflowCreate,
		WorkflowID: "wf-1",
		Decision:   DecisionAllow,
		Outcome:    "admitted",
		TraceID:    "trace-1",
		Timestamp:  time.Now(),
	}
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	r := records[0]
	if r.Principal != "alice" || r.Operation != OpWorkflowCreate || r.Outcome != "admitted" {
		t.Fatalf("audit record = %+v, want alice/workflow.create/admitted", r)
	}
	if r.ID == 0 {
		t.Fatal("audit record ID not assigned by appender")
	}
}

func TestSQLAuditSinkFailsClosedWhenAppenderErrors(t *testing.T) {
	sink := NewSQLAuditSink(failingAuditAppender{})
	err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowCreate})
	if err == nil {
		t.Fatal("Append succeeded with failing appender, want error (fail-closed)")
	}
}

func TestSQLAuditSinkNilAppenderIsUnavailable(t *testing.T) {
	sink := NewSQLAuditSink(nil)
	err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowCreate})
	if !errors.Is(err, ErrAuditUnavailable) {
		t.Fatalf("err = %v, want ErrAuditUnavailable", err)
	}
}

func TestSQLAuditSinkStampsTimestampWhenZero(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)
	if err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowRead}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	r := db.AuditRecords()[0]
	if r.Timestamp.IsZero() {
		t.Fatal("audit record timestamp not stamped when caller left it zero")
	}
}

func TestSQLAuditSinkPrefersNamespaceFromContext(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-ctx"))
	ev := AuditEvent{
		Principal: "alice",
		Namespace: "namespace-event",
		Operation: OpWorkflowCreate,
		Decision:  DecisionAllow,
		Outcome:   "admitted",
	}
	if err := sink.Append(ctx, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	if got := records[0].Namespace; got != "namespace-ctx" {
		t.Fatalf("audit Namespace = %q, want namespace-ctx (from context)", got)
	}
}

// TestSQLAuditSinkTransmitsAllEventFields is deliberately exhaustive rather
// than "assert Revision persists": SQLAuditSink.Append builds store.AuditRecord
// by hand-copying each AuditEvent field, and it once silently dropped Revision
// (grep for "Revision" inside Append found zero hits before the fix). A test
// that only checks the one field that was known to be missing would not catch
// the next field that gets forgotten the same way. This test gives every field
// a distinct non-zero value and checks the round trip field-by-field, naming
// the field on mismatch — do not use InMemoryAuditSink here, it copies the
// struct by value and would pass even with every field dropped.
func TestSQLAuditSinkTransmitsAllEventFields(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ev := AuditEvent{
		RequestID:      "req-full-1",
		Principal:      "principal-full-1",
		Namespace:      "namespace-full-1",
		Operation:      "operation-full-1",
		Resource:       "resource-full-1",
		WorkflowID:     "workflow-full-1",
		ExecutionID:    "execution-full-1",
		Decision:       DecisionAllow,
		Reason:         "reason-full-1",
		Outcome:        "outcome-full-1",
		Phase:          "phase-full-1",
		TraceID:        "trace-full-1",
		Timestamp:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		NodeID:         "node-full-1",
		ActivationID:   "activation-full-1",
		EntryID:        "entry-full-1",
		ReceiptAuditID: "receipt-full-1",
		Revision:       424242,
	}
	// context.Background() carries no namespace, so the existing
	// context-fallback semantics (covered by the two tests above) apply and
	// ev.Namespace is expected to persist unchanged — this test must not
	// fight that behavior, only prove every field round-trips.
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}
	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	got := records[0]

	checks := []struct {
		name string
		want any
		got  any
	}{
		{"RequestID", ev.RequestID, got.RequestID},
		{"Principal", ev.Principal, got.Principal},
		{"Namespace", ev.Namespace, got.Namespace},
		{"Operation", ev.Operation, got.Operation},
		{"Resource", ev.Resource, got.Resource},
		{"WorkflowID", ev.WorkflowID, got.WorkflowID},
		{"ExecutionID", ev.ExecutionID, got.ExecutionID},
		{"Decision", string(ev.Decision), got.Decision},
		{"Reason", ev.Reason, got.Reason},
		{"Outcome", ev.Outcome, got.Outcome},
		{"Phase", ev.Phase, got.Phase},
		{"TraceID", ev.TraceID, got.TraceID},
		{"Timestamp", ev.Timestamp, got.Timestamp},
		{"NodeID", ev.NodeID, got.NodeID},
		{"ActivationID", ev.ActivationID, got.ActivationID},
		{"EntryID", ev.EntryID, got.EntryID},
		{"ReceiptAuditID", ev.ReceiptAuditID, got.ReceiptAuditID},
		{"Revision", ev.Revision, got.Revision},
	}
	for _, c := range checks {
		if c.want != c.got {
			t.Errorf("field %s not transmitted by SQLAuditSink.Append: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestSQLAuditSinkFallsBackToEventNamespace(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ev := AuditEvent{
		Principal: "alice",
		Namespace: "namespace-event",
		Operation: OpWorkflowCreate,
		Decision:  DecisionAllow,
		Outcome:   "admitted",
	}
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	if got := records[0].Namespace; got != "namespace-event" {
		t.Fatalf("audit Namespace = %q, want namespace-event (fallback)", got)
	}
}
