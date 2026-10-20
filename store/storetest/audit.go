package storetest

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// AuditStore is the surface the audit contract needs: append plus the read
// path the reconcile worker uses. Both in-tree backends implement it; a
// backend that can only append cannot satisfy the contract because "the row
// was durably appended" is not observable through store.AuditAppender alone.
type AuditStore interface {
	store.AuditAppender
	store.AuditReconciler
	store.ScopedAuditReconciler
}

// AuditContract exercises the append-only audit log against one backend.
//
// The properties pinned here are the ones the reconcile worker and the
// admission fail-closed path depend on:
//
//   - an admitted admission row is visible to the pending scan until an
//     outcome row with the same (namespace, request_id) settles it;
//   - the correlation is namespace-scoped, so a request id reused in another
//     namespace can never settle this one;
//   - admission rows are append-only (a retry appends another row rather than
//     overwriting the first);
//   - a nil record is refused rather than silently persisted as a zero row;
//   - AppendOutcomeIfAbsent is idempotent on (namespace, request_id, phase).
//
// nsPrefix scopes the fixture. It must be a legal namespace prefix (see
// namespace.Validate): no ':' or glob metacharacters. Unique namespaces keep
// the contract runnable against a shared MySQL database that still holds rows
// from earlier runs.
func AuditContract(t *testing.T, s AuditStore, nsPrefix string) {
	t.Helper()
	ctx := context.Background()
	// The pending scans compare created_at (SQL) or Timestamp (memstore)
	// against `before`. An hour in the future covers both clocks; a non-zero
	// Timestamp on every fixture row keeps the memstore's "zero means old
	// enough" shortcut out of the way.
	before := time.Now().UTC().Add(time.Hour)
	admittedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	admission := func(ns namespace.Namespace, requestID string) *store.AuditRecord {
		return &store.AuditRecord{
			RequestID:   requestID,
			Principal:   "service:contract",
			Namespace:   string(ns),
			Operation:   "workflow.create",
			Resource:    "workflow/demo",
			WorkflowID:  "wf-demo",
			ExecutionID: "exec-contract",
			Decision:    "allow",
			Outcome:     store.AuditOutcomeAdmitted,
			TraceID:     "trace-contract",
			Timestamp:   admittedAt,
			Phase:       store.AuditPhaseAdmission,
		}
	}
	outcome := func(ns namespace.Namespace, requestID string) *store.AuditRecord {
		return &store.AuditRecord{
			RequestID: requestID,
			Namespace: string(ns),
			Operation: "workflow.create",
			Decision:  "allow",
			Outcome:   store.AuditOutcomeReconciled,
			Timestamp: admittedAt,
			Phase:     store.AuditPhaseOutcome,
		}
	}
	list := func(t *testing.T, ns namespace.Namespace) []*store.AuditRecord {
		t.Helper()
		recs, err := s.ListUnreconciledAdmissionsIn(ctx, []namespace.Namespace{ns}, before, 0, 256)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissionsIn(%q) error = %v", ns, err)
		}
		return recs
	}
	count := func(t *testing.T, ns namespace.Namespace) int {
		t.Helper()
		pending, oldest, err := s.CountUnreconciledAdmissionsIn(ctx, []namespace.Namespace{ns}, before)
		if err != nil {
			t.Fatalf("CountUnreconciledAdmissionsIn(%q) error = %v", ns, err)
		}
		if pending == 0 && !oldest.IsZero() {
			t.Fatalf("CountUnreconciledAdmissionsIn(%q) = 0 pending with oldest=%v, want zero time", ns, oldest)
		}
		if pending > 0 && oldest.IsZero() {
			t.Fatalf("CountUnreconciledAdmissionsIn(%q) = %d pending with a zero oldest timestamp", ns, pending)
		}
		return pending
	}

	t.Run("AdmissionVisibleUntilOutcomeSettlesIt", func(t *testing.T) {
		ns := namespace.Namespace(nsPrefix + "-settle")
		rec := admission(ns, "req-settle")
		if err := s.AppendAudit(ctx, rec); err != nil {
			t.Fatalf("AppendAudit(admission) error = %v", err)
		}
		recs := list(t, ns)
		if len(recs) != 1 {
			t.Fatalf("pending scan = %d rows, want the one admission", len(recs))
		}
		got := recs[0]
		if got.RequestID != "req-settle" || got.Namespace != string(ns) {
			t.Fatalf("pending row identity = (%q, %q), want (req-settle, %q)", got.RequestID, got.Namespace, ns)
		}
		if got.Operation != "workflow.create" || got.Principal != "service:contract" || got.TraceID != "trace-contract" {
			t.Fatalf("pending row lost audit fields: %+v", got)
		}
		if got.Phase != store.AuditPhaseAdmission || got.Outcome != store.AuditOutcomeAdmitted {
			t.Fatalf("pending row phase/outcome = (%q, %q), want (%q, %q)",
				got.Phase, got.Outcome, store.AuditPhaseAdmission, store.AuditOutcomeAdmitted)
		}
		if !got.Timestamp.Equal(admittedAt) {
			t.Fatalf("pending row timestamp = %v, want %v", got.Timestamp, admittedAt)
		}
		if pending := count(t, ns); pending != 1 {
			t.Fatalf("pending count = %d, want 1", pending)
		}

		if err := s.AppendAudit(ctx, outcome(ns, "req-settle")); err != nil {
			t.Fatalf("AppendAudit(outcome) error = %v", err)
		}
		if recs := list(t, ns); len(recs) != 0 {
			t.Fatalf("pending scan after the outcome = %d rows, want none: %+v", len(recs), recs)
		}
		if pending := count(t, ns); pending != 0 {
			t.Fatalf("pending count after the outcome = %d, want 0", pending)
		}
	})

	t.Run("OutcomeIsCorrelatedByNamespace", func(t *testing.T) {
		nsA := namespace.Namespace(nsPrefix + "-scope-a")
		nsB := namespace.Namespace(nsPrefix + "-scope-b")
		if err := s.AppendAudit(ctx, admission(nsA, "req-shared")); err != nil {
			t.Fatalf("AppendAudit(nsA admission) error = %v", err)
		}
		if err := s.AppendAudit(ctx, outcome(nsB, "req-shared")); err != nil {
			t.Fatalf("AppendAudit(nsB outcome) error = %v", err)
		}
		if recs := list(t, nsA); len(recs) != 1 {
			t.Fatalf("an outcome in another namespace settled nsA's admission: %d rows, want 1", len(recs))
		}
		if recs := list(t, nsB); len(recs) != 0 {
			t.Fatalf("an outcome row was reported as a pending admission in nsB: %+v", recs)
		}
		// A fresh request in nsB is its own admission, and settling it changes
		// nothing in nsA. (A second admission under a request id whose outcome
		// already exists is considered settled by both backends; that duplicate
		// case is not part of the scan's contract.)
		if err := s.AppendAudit(ctx, admission(nsB, "req-own")); err != nil {
			t.Fatalf("AppendAudit(nsB admission) error = %v", err)
		}
		if recs := list(t, nsB); len(recs) != 1 {
			t.Fatalf("nsB admission = %d rows, want 1", len(recs))
		}
		if recs := list(t, nsA); len(recs) != 1 {
			t.Fatalf("nsA admission after nsB activity = %d rows, want 1", len(recs))
		}
	})

	t.Run("AdmissionRowsAreAppendOnly", func(t *testing.T) {
		ns := namespace.Namespace(nsPrefix + "-append")
		for i := 0; i < 2; i++ {
			if err := s.AppendAudit(ctx, admission(ns, "req-append")); err != nil {
				t.Fatalf("AppendAudit #%d error = %v", i+1, err)
			}
		}
		recs := list(t, ns)
		if len(recs) != 2 {
			t.Fatalf("two appended admission rows produced %d pending rows, want 2: "+
				"the audit log is append-only and admission rows carry no idempotency key", len(recs))
		}
		if ids := auditRequestIDs(recs); !reflect.DeepEqual(ids, []string{"req-append", "req-append"}) {
			t.Fatalf("pending request ids = %v, want both appended rows", ids)
		}
	})

	t.Run("NilRecordIsRejected", func(t *testing.T) {
		ns := namespace.Namespace(nsPrefix + "-nil")
		if err := s.AppendAudit(ctx, nil); err == nil {
			t.Fatal("AppendAudit(nil) returned nil; a nil audit record must be refused")
		}
		if recs := list(t, ns); len(recs) != 0 {
			t.Fatalf("a refused nil append left %d pending rows", len(recs))
		}
	})

	t.Run("AppendOutcomeIfAbsentIsIdempotent", func(t *testing.T) {
		ns := namespace.Namespace(nsPrefix + "-outcome")
		if err := s.AppendAudit(ctx, admission(ns, "req-outcome")); err != nil {
			t.Fatalf("AppendAudit(admission) error = %v", err)
		}
		first, err := s.AppendOutcomeIfAbsent(ctx, outcome(ns, "req-outcome"))
		if err != nil || !first {
			t.Fatalf("first AppendOutcomeIfAbsent = (%v, %v), want (true, nil)", first, err)
		}
		second, err := s.AppendOutcomeIfAbsent(ctx, outcome(ns, "req-outcome"))
		if err != nil || second {
			t.Fatalf("second AppendOutcomeIfAbsent = (%v, %v), want (false, nil)", second, err)
		}
		if recs := list(t, ns); len(recs) != 0 {
			t.Fatalf("pending scan after the outcome = %d rows, want none", len(recs))
		}
		if _, err := s.AppendOutcomeIfAbsent(ctx, nil); err == nil {
			t.Fatal("AppendOutcomeIfAbsent(nil) returned nil; a nil record must be refused")
		}
	})
}

func auditRequestIDs(recs []*store.AuditRecord) []string {
	ids := make([]string, len(recs))
	for i, rec := range recs {
		ids[i] = rec.RequestID
	}
	sort.Strings(ids)
	return ids
}
