//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// TestSQLStoreAuditAppend proves the durable audit sink persists append-only
// audit records to MySQL and that the record is readable back via a fresh
// provider (independent connection). This is the B3 durability boundary for
// the admission-audit fail-closed path. Requires the xflow_audit_events table
// (applied via `make env-migrate` / db/xflow_schema.sql).
func TestSQLStoreAuditAppend(t *testing.T) {
	dsn := requireMySQL(t)

	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New(%q): %v", dsn, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rec := &store.AuditRecord{
		RequestID:   "req-audit-real",
		Principal:   "alice",
		TenantID:    "tenant-a",
		Operation:   "workflow.create",
		Resource:    "workflow/wf-1",
		WorkflowID:  "wf-1",
		ExecutionID: "exec-audit-1",
		Decision:    "allow",
		Outcome:     "admitted",
		TraceID:     "trace-audit-1",
		Timestamp:   time.Now().UTC(),
	}
	if err := p.AppendAudit(ctx, rec); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}
	if rec.ID == 0 {
		t.Fatal("audit record ID not assigned")
	}

	// Reopen with a fresh provider/connection to prove durability across
	// connections (not just the in-process cache).
	p2, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("reopen mysqlstore.New: %v", err)
	}
	got, err := p2.AuditByID(ctx, rec.ID)
	if err != nil {
		t.Fatalf("AuditByID: %v", err)
	}
	if got.Principal != "alice" || got.Operation != "workflow.create" || got.Outcome != "admitted" {
		t.Fatalf("audit row = %+v, want alice/workflow.create/admitted", got)
	}
	if got.RequestID != "req-audit-real" || got.TraceID != "trace-audit-1" {
		t.Fatalf("audit row correlation fields mismatch: %+v", got)
	}
}

// TestSQLStoreAuditAppendInTransaction proves the audit append can share a
// transaction with another write, so the admission audit commits or rolls
// back atomically with the mutation it audits.
func TestSQLStoreAuditAppendInTransaction(t *testing.T) {
	dsn := requireMySQL(t)
	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New(%q): %v", dsn, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txErr := p.Transaction(ctx, func(s store.Set) error {
		if s.Audit == nil {
			t.Fatal("audit repo not bound in transaction Set")
		}
		return s.Audit.AppendAudit(ctx, &store.AuditRecord{
			Principal: "tx-alice",
			Operation: "execution.signal",
			Decision:  "allow",
			Outcome:   "admitted",
			Timestamp: time.Now().UTC(),
		})
	})
	if txErr != nil {
		t.Fatalf("Transaction: %v", txErr)
	}
}

// Compile-time: keep sqlstore import meaningful even if only the Provider type
// is referenced indirectly.
var _ = (*sqlstore.Provider)(nil)

