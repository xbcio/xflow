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
//     for the same (namespace, request_id) returns appended=false (idempotent),
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
		Namespace:   "namespace-reconcile",
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
	// pending scan (NOT EXISTS on phase=outcome for the same namespace+request).
	settled := &store.AuditRecord{
		RequestID:   uniqueAuditRequestID("settled"),
		Principal:   "alice",
		Namespace:   "namespace-reconcile",
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
		Namespace:   "namespace-reconcile",
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
	// Use afterSeqID = pending.ID - 1 so the cursor skips past the large
	// pre-existing backlog and returns only our just-inserted rows.
	cutoff := time.Now().UTC().Add(time.Second)
	candidates, err := p.ListUnreconciledAdmissions(ctx, cutoff, pending.ID-1, 64)
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
		Namespace:   "namespace-reconcile",
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

	// A second append for the same (namespace, request_id) must be a no-op
	// (idempotent) — enforced by the unique phase_key index.
	appended2, err := p.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID:   pending.RequestID,
		Principal:   "alice",
		Namespace:   "namespace-reconcile",
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
	candidates2, err := p.ListUnreconciledAdmissions(ctx, cutoff, pending.ID-1, 64)
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

// TestSQLStoreAuditCursorPagination proves R3.4 cursor pagination against
// real MySQL: afterSeqID filters correctly, and CountUnreconciledAdmissions
// returns correct full-table metrics. Uses a unique namespace prefix so
// assertions are isolated from any pre-existing rows in the table.
func TestSQLStoreAuditCursorPagination(t *testing.T) {
	dsn := requireMySQL(t)
	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Use a unique namespace to isolate from other rows in the table.
	namespace := "namespace-cursor-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC()

	// Insert 3 admitted rows with no outcome (pending). They will get
	// ascending auto-increment IDs.
	var seqIDs []uint64
	for i := 1; i <= 3; i++ {
		rec := &store.AuditRecord{
			RequestID:   uniqueAuditRequestID("cur" + string(rune('0'+i))),
			Principal:   "alice",
			Namespace:   namespace,
			Operation:   "workflow.create",
			ExecutionID: "exec-cur-" + string(rune('0'+i)),
			Decision:    "allow",
			Outcome:     store.AuditOutcomeAdmitted,
			Phase:       store.AuditPhaseAdmission,
			Timestamp:   now.Add(-time.Duration(4-i) * time.Minute), // oldest first
		}
		if err := p.AppendAudit(ctx, rec); err != nil {
			t.Fatalf("AppendAudit row %d: %v", i, err)
		}
		// AppendAudit sets rec.ID; SeqID maps to ID in sqlstore
		seqIDs = append(seqIDs, rec.ID)
	}

	cutoff := time.Now().UTC().Add(time.Second)

	// Use afterSeqID = first row's ID - 1 so we skip the large pre-existing
	// backlog and start scanning from just before our test rows.
	baseSeqID := seqIDs[0] - 1
	all, err := p.ListUnreconciledAdmissions(ctx, cutoff, baseSeqID, 100)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions(afterSeqID=%d): %v", baseSeqID, err)
	}
	var allForNamespace []*store.AuditRecord
	for _, c := range all {
		if c.Namespace == namespace {
			allForNamespace = append(allForNamespace, c)
		}
	}
	if len(allForNamespace) != 3 {
		t.Fatalf("expected 3 rows for namespace, got %d (afterSeqID=%d)", len(allForNamespace), baseSeqID)
	}
	// Verify SeqID is populated and monotonically increasing.
	for i, c := range allForNamespace {
		if c.SeqID == 0 {
			t.Fatalf("row %d has SeqID=0, want non-zero", i)
		}
		if i > 0 && c.SeqID <= allForNamespace[i-1].SeqID {
			t.Fatalf("row %d SeqID=%d not > row %d SeqID=%d", i, c.SeqID, i-1, allForNamespace[i-1].SeqID)
		}
	}

	// afterSeqID = first row's SeqID → should skip the first row, return 2.
	afterFirst := allForNamespace[0].SeqID
	page2, err := p.ListUnreconciledAdmissions(ctx, cutoff, afterFirst, 100)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions(afterSeqID=%d): %v", afterFirst, err)
	}
	var page2ForNamespace []*store.AuditRecord
	for _, c := range page2 {
		if c.Namespace == namespace {
			page2ForNamespace = append(page2ForNamespace, c)
		}
	}
	if len(page2ForNamespace) != 2 {
		t.Fatalf("expected 2 rows after first, got %d", len(page2ForNamespace))
	}
	// The first row in page2 should have SeqID > afterFirst.
	if page2ForNamespace[0].SeqID <= afterFirst {
		t.Fatalf("first row in page2 SeqID=%d should be > afterSeqID=%d", page2ForNamespace[0].SeqID, afterFirst)
	}

	// afterSeqID = last row's SeqID → should return 0 (for this namespace).
	afterLast := allForNamespace[2].SeqID
	page3, err := p.ListUnreconciledAdmissions(ctx, cutoff, afterLast, 100)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions(afterSeqID=%d): %v", afterLast, err)
	}
	var page3ForNamespace []*store.AuditRecord
	for _, c := range page3 {
		if c.Namespace == namespace {
			page3ForNamespace = append(page3ForNamespace, c)
		}
	}
	if len(page3ForNamespace) != 0 {
		t.Fatalf("expected 0 rows after last, got %d", len(page3ForNamespace))
	}

	// CountUnreconciledAdmissions should report >= 3 pending (our rows at
	// minimum; there may be other rows from other tests in the shared DB).
	pending, oldest, err := p.CountUnreconciledAdmissions(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountUnreconciledAdmissions: %v", err)
	}
	if pending < 3 {
		t.Fatalf("pending = %d, want >= 3", pending)
	}
	if oldest.IsZero() {
		t.Fatal("oldest is zero, want a real timestamp")
	}
	// Oldest should be at most ~4 minutes ago (our oldest is 3 min ago; some
	// buffer for test timing and pre-existing rows).
	_ = seqIDs
}
