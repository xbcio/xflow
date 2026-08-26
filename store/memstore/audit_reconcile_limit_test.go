package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
)

// defaultUnreconciledLimit is the page size ListUnreconciledAdmissions falls
// back to when the caller passes limit <= 0. The value is arbitrary; what is
// not arbitrary is that store/sqlstore/audit_repo.go:208 hardcodes the same
// number for the same interface. Nothing in store/interfaces.go says what
// limit <= 0 means, so the two implementations agree only by coincidence of
// having been written together.
const defaultUnreconciledLimit = 256

// seedPendingAdmissions appends n admitted rows with no outcome, all older than
// `before`, each with its own request id so the (namespace, request_id) dedup
// key never collapses them.
func seedPendingAdmissions(t *testing.T, s *Store, n int, at time.Time) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		if err := s.AppendAudit(ctx, &store.AuditRecord{
			RequestID: fmt.Sprintf("req-pending-%03d", i),
			Namespace: "tenant-a",
			Operation: "PutSupply",
			Phase:     store.AuditPhaseAdmission,
			Outcome:   store.AuditOutcomeAdmitted,
			Timestamp: at,
		}); err != nil {
			t.Fatalf("AppendAudit(%d): %v", i, err)
		}
	}
}

// TestListUnreconciledAdmissionsLimitZeroMeansDefaultPage pins what a
// non-positive limit means.
//
// Two callers already pass 0 — audit_reconcile_namespace_test.go:76 and :177 —
// and both read it as "give me every pending admission". Neither can tell:
// deleting `if limit <= 0 { limit = 256 }` leaves all five packages that import
// memstore green. The reason is that the bound is checked after the append, so
// limit=0 yields exactly one row, and those two call sites happen to expect
// exactly one and exactly zero. They have been reading the first pending row
// and calling it the whole set.
//
// That is the shape from [[list-options-zero-limit-returns-nothing]] with the
// off-by-one flipped: not "asserting no rows passes unconditionally" but
// "asserting one row passes no matter how many there are". Both make a test
// that names a set assert something about its head.
//
// The three arms are not redundant. Zero-means-everything alone is satisfied by
// ignoring limit entirely, so the explicit-limit arm holds the other side. The
// 256 arm pins the boundary itself, which is what keeps memstore and sqlstore
// from drifting apart silently — the reconcile worker's cursor arithmetic
// (audit_reconcile_worker.go:260, `len(candidates) < w.batch`) reads a short
// page as "reached the tail", so a store that quietly returned fewer rows than
// asked would make the worker wrap its cursor early and re-scan the head
// forever while the tail of the backlog never settles.
func TestListUnreconciledAdmissionsLimitZeroMeansDefaultPage(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	admittedAt := now.Add(-time.Hour)

	t.Run("limit 0 returns every pending row, not just the first", func(t *testing.T) {
		s := New()
		const seeded = 3
		seedPendingAdmissions(t, s, seeded, admittedAt)

		pending, err := s.ListUnreconciledAdmissions(ctx, now, 0, 0)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissions: %v", err)
		}
		if len(pending) != seeded {
			t.Fatalf("pending = %d, want %d: a caller passing limit=0 asks for the "+
				"default page, and every existing limit=0 call site reads the result "+
				"as the complete pending set; returning the head of it makes those "+
				"assertions true regardless of how much backlog exists", len(pending), seeded)
		}
		// Oldest-first, and distinct: a slice of three copies of the same row
		// would satisfy the length check.
		for i, r := range pending {
			want := fmt.Sprintf("req-pending-%03d", i)
			if r.RequestID != want {
				t.Fatalf("pending[%d].RequestID = %q, want %q (oldest-first)", i, r.RequestID, want)
			}
		}
	})

	t.Run("an explicit limit is honored", func(t *testing.T) {
		s := New()
		seedPendingAdmissions(t, s, 5, admittedAt)

		pending, err := s.ListUnreconciledAdmissions(ctx, now, 0, 2)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissions: %v", err)
		}
		if len(pending) != 2 {
			t.Fatalf("pending = %d, want 2: limit=0 meaning 'everything' must not be "+
				"implemented by ignoring limit", len(pending))
		}
		if pending[0].RequestID != "req-pending-000" || pending[1].RequestID != "req-pending-001" {
			t.Fatalf("page = [%s %s], want the two oldest",
				pending[0].RequestID, pending[1].RequestID)
		}
	})

	t.Run("the default page is bounded at 256", func(t *testing.T) {
		s := New()
		seedPendingAdmissions(t, s, defaultUnreconciledLimit+1, admittedAt)

		pending, err := s.ListUnreconciledAdmissions(ctx, now, 0, 0)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissions: %v", err)
		}
		if len(pending) != defaultUnreconciledLimit {
			t.Fatalf("pending = %d, want %d: limit=0 is a default page size, not an "+
				"unbounded scan, and the number must stay equal to sqlstore's copy of "+
				"it (store/sqlstore/audit_repo.go:208)", len(pending), defaultUnreconciledLimit)
		}
		// The page is the head of the backlog, so the cursor advances past the
		// oldest rows rather than skipping them.
		if pending[0].RequestID != "req-pending-000" {
			t.Fatalf("first row = %q, want req-pending-000", pending[0].RequestID)
		}
	})
}
