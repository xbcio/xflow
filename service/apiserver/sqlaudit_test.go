package apiserver

import (
	"context"
	"errors"
	"testing"
	"time"

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
		RequestID:   "req-1",
		Principal:   "alice",
		TenantID:    "tenant-a",
		Operation:   OpWorkflowCreate,
		WorkflowID:  "wf-1",
		Decision:    DecisionAllow,
		Outcome:     "admitted",
		TraceID:     "trace-1",
		Timestamp:   time.Now(),
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
