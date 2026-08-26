//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/xbcio/xflow/types"
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
		Namespace:   "namespace-a",
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
// back atomically with the mutation it audits. This is the commit half; the
// rollback half is TestSQLStoreAuditRollsBackWithTheMutationItAudits.
//
// This used to make one audit write inside a transaction and assert only that
// Transaction returned nil. It never read the row back and never made a second
// write, so it demonstrated neither durability nor atomicity — the two things
// its name claims. A Transaction that swallowed every write and committed an
// empty tx passed it.
func TestSQLStoreAuditAppendInTransaction(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "audit-tx-commit")
	rec := &store.AuditRecord{
		RequestID:   "req-" + string(execID),
		Principal:   "tx-alice",
		Namespace:   "namespace-tx",
		Operation:   "execution.signal",
		ExecutionID: string(execID),
		Decision:    "allow",
		Outcome:     "admitted",
		Timestamp:   time.Now().UTC(),
	}

	txErr := p.Transaction(ctx, func(s store.Set) error {
		if s.Audit == nil {
			t.Fatal("audit repo not bound in transaction Set")
		}
		// The mutation being audited, in the same transaction.
		if err := s.Execution.CreateExecution(ctx, &store.ExecutionRecord{
			ExecutionID:  execID,
			WorkflowName: "wf-audit-tx",
			WorkflowDef:  emptyJSON,
			Params:       emptyJSON,
			Runtime:      emptyJSON,
			Status:       types.ExecutionStatusRunning,
		}); err != nil {
			return err
		}
		return s.Audit.AppendAudit(ctx, rec)
	})
	if txErr != nil {
		t.Fatalf("Transaction: %v", txErr)
	}
	if rec.ID == 0 {
		t.Fatal("audit record ID not assigned inside the transaction")
	}

	// Read both back through a provider that never saw the transaction.
	p2 := newSQLStoreProvider(t)
	got, err := p2.AuditByID(ctx, rec.ID)
	if err != nil {
		t.Fatalf("AuditByID(%d) after commit: %v", rec.ID, err)
	}
	if got.Principal != "tx-alice" || got.Operation != "execution.signal" {
		t.Fatalf("audit row = %+v, want tx-alice/execution.signal", got)
	}
	if got.ExecutionID != string(execID) {
		t.Fatalf("audit row ExecutionID = %q, want %q", got.ExecutionID, execID)
	}
	if _, err := p2.GetExecution(ctx, execID); err != nil {
		t.Fatalf("GetExecution(%q) after commit: %v — the audited mutation did not "+
			"commit alongside its audit row", execID, err)
	}
}

// TestSQLStoreAuditRollsBackWithTheMutationItAudits is the half the commit
// test cannot cover: when the audited mutation fails, the audit row must not
// survive on its own. An admission audit that outlives an aborted mutation is
// a false record of something that never happened.
//
// The abort comes from a real constraint (a duplicate ExecutionID against
// uk_execution_id) rather than a sentinel error, so this also covers the case
// where MySQL — not the callback — is what ends the transaction. AppendAudit
// on an admission row is a plain INSERT with no conflict clause, so nothing in
// the audit path can quietly absorb the failure.
func TestSQLStoreAuditRollsBackWithTheMutationItAudits(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "audit-tx-rollback")
	newBaseExecution(ctx, t, p, execID) // committed outside the tx; the collision target

	rec := &store.AuditRecord{
		RequestID:   "req-" + string(execID),
		Principal:   "rollback-alice",
		Namespace:   "namespace-tx",
		Operation:   "workflow.create",
		ExecutionID: string(execID),
		Decision:    "allow",
		Outcome:     "admitted",
		Timestamp:   time.Now().UTC(),
	}

	txErr := p.Transaction(ctx, func(s store.Set) error {
		// Audit first, mutation second — the ordering that leaves an orphan
		// audit row behind if the transaction is not real.
		if err := s.Audit.AppendAudit(ctx, rec); err != nil {
			return err
		}
		return s.Execution.CreateExecution(ctx, &store.ExecutionRecord{
			ExecutionID:  execID, // already exists: violates uk_execution_id
			WorkflowName: "wf-audit-tx-dup",
			WorkflowDef:  emptyJSON,
			Params:       emptyJSON,
			Runtime:      emptyJSON,
			Status:       types.ExecutionStatusRunning,
		})
	})
	if txErr == nil {
		t.Fatal("Transaction returned nil, want a duplicate-key failure on uk_execution_id")
	}
	// Assert the failure is the one being staged. Without this, a NOT NULL
	// violation or a missing column would abort the tx for the wrong reason and
	// the rollback assertion below would still pass.
	if !errors.Is(txErr, gorm.ErrDuplicatedKey) {
		t.Fatalf("Transaction error = %v, want gorm.ErrDuplicatedKey (uk_execution_id)", txErr)
	}
	if rec.ID == 0 {
		t.Fatal("audit record ID not assigned; the INSERT never reached MySQL, so " +
			"there is nothing for the rollback to undo and this test proves nothing")
	}

	if _, err := p.AuditByID(ctx, rec.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("AuditByID(%d) after rollback: err=%v, want ErrNotFound — the audit "+
			"row outlived the mutation it was auditing", rec.ID, err)
	}
}

// Compile-time: keep sqlstore import meaningful even if only the Provider type
// is referenced indirectly.
var _ = (*sqlstore.Provider)(nil)
