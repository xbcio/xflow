package memstore

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
)

// The reconcile dedup key is (namespace, request_id) — five places in
// memstore.go build or compare `r.Namespace+"|"+r.RequestID` (:377, :394, :420,
// :421, :443, :455) and store.AuditRecord's own doc comment calls the
// (Namespace, RequestID, Phase) triple "the reconcile worker's idempotency
// key". Nothing executed the namespace half of it: dropping `r.Namespace+"|"`
// from all five sites compiles and leaves the suite green, because every test
// that reaches these methods uses a single namespace ("default") and
// pairwise-unique RequestIDs.
//
// The RequestID is not ours to assume unique. service/apiserver/authz.go:424
// takes it verbatim from the caller's X-Request-ID header, truncated to 128
// bytes and never namespaced; only a request that omits the header gets a
// generated `req-<unixnano>`. Two tenants behind two different gateways
// picking the same trace id — or one tenant deliberately replaying another's —
// is a header away.
//
// Two distinct failures follow from a namespace-blind key, so both are pinned
// below:
//
//   - the pending scan (ListUnreconciledAdmissions / CountUnreconciledAdmissions)
//     accepts tenant A's outcome row as proof that tenant B's admission
//     settled, so B's genuinely-crashed mutation is dropped from the pending
//     set forever and the T9 worker never reconciles it;
//   - AppendOutcomeIfAbsent refuses to write B's outcome at all (appended=false,
//     no error), leaving a permanent hole in B's audit trail while telling the
//     caller the row was already there.
func TestAuditReconcileKeyIsScopedByNamespace(t *testing.T) {
	ctx := context.Background()
	s := New()

	// The same client-supplied request id, in two namespaces.
	const shared = "req-collides-across-tenants"
	now := time.Now().UTC()
	admittedAt := now.Add(-time.Hour)

	for _, ns := range []string{"tenant-a", "tenant-b"} {
		if err := s.AppendAudit(ctx, &store.AuditRecord{
			RequestID: shared,
			Namespace: ns,
			Operation: "PutSupply",
			Phase:     store.AuditPhaseAdmission,
			Outcome:   store.AuditOutcomeAdmitted,
			Timestamp: admittedAt,
		}); err != nil {
			t.Fatalf("AppendAudit(%s admission): %v", ns, err)
		}
	}

	// tenant-a's mutation lands and its outcome is recorded. tenant-b's process
	// crashed after admission, so it has no outcome row.
	appended, err := s.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID: shared,
		Namespace: "tenant-a",
		Operation: "PutSupply",
		Outcome:   store.AuditOutcomeReconciled,
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("AppendOutcomeIfAbsent(tenant-a): %v", err)
	}
	if !appended {
		t.Fatal("AppendOutcomeIfAbsent(tenant-a) = false on an empty outcome set")
	}

	// tenant-b must still be pending: nobody reconciled it.
	pending, err := s.ListUnreconciledAdmissions(ctx, now, 0, 0)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions: %v", err)
	}
	if len(pending) != 1 {
		// Exactly one, not ">= 1": zero means tenant-a's outcome swallowed
		// tenant-b's admission, two means tenant-a's own outcome was ignored.
		var got []string
		for _, r := range pending {
			got = append(got, r.Namespace)
		}
		t.Fatalf("pending admissions = %d %v, want exactly 1 (tenant-b): "+
			"a namespace-blind dedup key lets one tenant's outcome row mark "+
			"another tenant's crashed admission as settled, and the T9 worker "+
			"then never reconciles it", len(pending), got)
	}
	if pending[0].Namespace != "tenant-b" {
		t.Fatalf("pending admission namespace = %q, want tenant-b", pending[0].Namespace)
	}

	count, oldest, err := s.CountUnreconciledAdmissions(ctx, now)
	if err != nil {
		t.Fatalf("CountUnreconciledAdmissions: %v", err)
	}
	if count != 1 {
		// The backlog gauge is built from this call, independently of the
		// cursor scan above, and has its own copy of the key.
		t.Fatalf("CountUnreconciledAdmissions = %d, want 1: the reconcile "+
			"backlog metric under-reports whenever two namespaces share a "+
			"request id", count)
	}
	if !oldest.Equal(admittedAt) {
		t.Fatalf("oldest pending = %v, want %v", oldest, admittedAt)
	}

	// And tenant-b's own outcome must still be writable. Returning false here
	// is the quieter half of the defect: no error, no row, a permanent gap in
	// tenant-b's audit trail.
	appended, err = s.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID: shared,
		Namespace: "tenant-b",
		Operation: "PutSupply",
		Outcome:   store.AuditOutcomeReconciled,
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("AppendOutcomeIfAbsent(tenant-b): %v", err)
	}
	if !appended {
		t.Fatal("AppendOutcomeIfAbsent(tenant-b) = false: tenant-a's outcome " +
			"row was accepted as tenant-b's, so tenant-b's outcome is never " +
			"written and the caller is told it already exists")
	}
}

// TestAuditReconcileDedupesWithinANamespace is the positive control for the
// test above: without it, a key that never matches anything — or an
// AppendOutcomeIfAbsent that unconditionally returns true — satisfies every
// assertion there while destroying the idempotency the method exists to
// provide. A leader switch mid-reconcile would then write a second outcome row
// for the same request.
//
// It also pins the pending filter itself: a scan that never filters would
// report tenant-b pending above for the wrong reason.
func TestAuditReconcileDedupesWithinANamespace(t *testing.T) {
	ctx := context.Background()
	s := New()

	const reqID = "req-settled-twice"
	now := time.Now().UTC()

	if err := s.AppendAudit(ctx, &store.AuditRecord{
		RequestID: reqID,
		Namespace: "tenant-a",
		Operation: "PutSupply",
		Phase:     store.AuditPhaseAdmission,
		Outcome:   store.AuditOutcomeAdmitted,
		Timestamp: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	outcome := func() *store.AuditRecord {
		return &store.AuditRecord{
			RequestID: reqID,
			Namespace: "tenant-a",
			Operation: "PutSupply",
			Outcome:   store.AuditOutcomeReconciled,
			Timestamp: now,
		}
	}

	if appended, err := s.AppendOutcomeIfAbsent(ctx, outcome()); err != nil || !appended {
		t.Fatalf("first AppendOutcomeIfAbsent = %v, %v; want true, nil", appended, err)
	}
	if appended, err := s.AppendOutcomeIfAbsent(ctx, outcome()); err != nil || appended {
		t.Fatalf("second AppendOutcomeIfAbsent = %v, %v; want false, nil: the "+
			"same (namespace, request_id) must not get two outcome rows",
			appended, err)
	}

	pending, err := s.ListUnreconciledAdmissions(ctx, now, 0, 0)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0 once the admission has an outcome row", len(pending))
	}
	count, _, err := s.CountUnreconciledAdmissions(ctx, now)
	if err != nil {
		t.Fatalf("CountUnreconciledAdmissions: %v", err)
	}
	if count != 0 {
		t.Fatalf("CountUnreconciledAdmissions = %d, want 0", count)
	}
}
