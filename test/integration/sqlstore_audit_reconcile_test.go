//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// TestSQLStoreAuditReconcilePendingScanAndIdempotentOutcome proves the T9
// audit reconcile contract against real MySQL:
//   - ListUnreconciledAdmissions returns admitted (phase=admission,
//     outcome=admitted) rows older than the cutoff with no matching outcome
//     row, and excludes admissions that already have an outcome.
//   - AppendOutcomeIfAbsent appends a phase=outcome row, and a second call
//     for the same (tenant, request_id) returns appended=false (idempotent),
//     enforced by the unique uk_phase_key index on the generated phase_key
//     column.
//
// Requires the xflow_audit_events table with the T9 phase column + phase_key
// unique index (applied via `make env-migrate` / db/xflow_schema.sql).
func TestSQLStoreAuditReconcilePendingScanAndIdempotentOutcome(t *testing.T) {
	dsn := requireMySQL(t)
	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now().UTC()
	// An admitted mutation whose outcome was never appended (crash window).
	pending := &store.AuditRecord{
		RequestID:   uniqueAuditRequestID("pending"),
		Principal:   "alice",
		TenantID:    "tenant-reconcile",
		Operation:   "workflow.create",
		ExecutionID: "exec-pending",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		TraceID:     "trace-pending",
		Timestamp:   now.Add(-2 * time.Minute),
	}
	if err := p.AppendAudit(ctx, pending); err != nil {
		t.Fatalf("AppendAudit pending: %v", err)
	}

	// An admission that already has an outcome — must NOT appear in the
	// pending scan (NOT EXISTS on phase=outcome for the same tenant+request).
	settled := &store.AuditRecord{
		RequestID:   uniqueAuditRequestID("settled"),
		Principal:   "alice",
		TenantID:    "tenant-reconcile",
		Operation:   "workflow.create",
		ExecutionID: "exec-settled",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		Timestamp:   now.Add(-3 * time.Minute),
	}
	if err := p.AppendAudit(ctx, settled); err != nil {
		t.Fatalf("AppendAudit settled admission: %v", err)
	}
	if err := p.AppendAudit(ctx, &store.AuditRecord{
		RequestID:   settled.RequestID,
		Principal:   "alice",
		TenantID:    "tenant-reconcile",
		Operation:   "workflow.create",
		ExecutionID: "exec-settled",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeReconciled,
		Phase:       store.AuditPhaseOutcome,
		Timestamp:   now.Add(-3 * time.Minute),
	}); err != nil {
		t.Fatalf("AppendAudit settled outcome: %v", err)
	}

	// Scan with a future cutoff so the just-inserted rows qualify by
	// created_at (the backlog-age filter is exercised in the worker unit
	// tests via the event timestamp; this test focuses on the pending-scan
	// NOT EXISTS filter and the idempotent outcome append).
	cutoff := time.Now().UTC().Add(time.Second)
	candidates, err := p.ListUnreconciledAdmissions(ctx, cutoff, 64)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions: %v", err)
	}
	foundPending := false
	for _, c := range candidates {
		if c.RequestID == settled.RequestID {
			t.Fatalf("settled admission %q appeared in pending scan", settled.RequestID)
		}
		if c.RequestID == pending.RequestID {
			foundPending = true
		}
	}
	if !foundPending {
		t.Fatalf("pending admission %q not returned by scan, candidates=%d", pending.RequestID, len(candidates))
	}

	// Append the outcome for the pending admission — must succeed.
	appended, err := p.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID:   pending.RequestID,
		Principal:   "alice",
		TenantID:    "tenant-reconcile",
		Operation:   "workflow.create",
		ExecutionID: "exec-pending",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeReconciled,
		Phase:       store.AuditPhaseOutcome,
		Timestamp:   now,
	})
	if err != nil {
		t.Fatalf("AppendOutcomeIfAbsent first: %v", err)
	}
	if !appended {
		t.Fatal("first AppendOutcomeIfAbsent: appended=false, want true")
	}

	// A second append for the same (tenant, request_id) must be a no-op
	// (idempotent) — enforced by the unique phase_key index.
	appended2, err := p.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID:   pending.RequestID,
		Principal:   "alice",
		TenantID:    "tenant-reconcile",
		Operation:   "workflow.create",
		ExecutionID: "exec-pending",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeReconciled,
		Phase:       store.AuditPhaseOutcome,
		Timestamp:   now,
	})
	if err != nil {
		t.Fatalf("AppendOutcomeIfAbsent second: %v", err)
	}
	if appended2 {
		t.Fatal("second AppendOutcomeIfAbsent: appended=true, want false (idempotent)")
	}

	// After settling, the pending scan no longer returns it.
	candidates2, err := p.ListUnreconciledAdmissions(ctx, cutoff, 64)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions after settle: %v", err)
	}
	for _, c := range candidates2 {
		if c.RequestID == pending.RequestID {
			t.Fatalf("pending admission %q still returned after outcome appended", pending.RequestID)
		}
	}
}

// uniqueAuditRequestID returns a per-test-unique request id so concurrent
// integration runs do not collide on the uk_phase_key index.
func uniqueAuditRequestID(suffix string) string {
	return "req-reconcile-" + suffix + "-" + time.Now().Format("150405.000000000")
}
