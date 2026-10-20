package control

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// scopedFakeAuditReconciler adds the scoped capability to the in-memory fake,
// built on the same AuditScopeValues rule the real stores share, and counts
// which flavor of scan the worker used.
type scopedFakeAuditReconciler struct {
	*fakeAuditReconciler
	mu            sync.Mutex
	scopedCalls   int
	unscopedCalls int
}

var _ store.ScopedAuditReconciler = (*scopedFakeAuditReconciler)(nil)

func (f *scopedFakeAuditReconciler) bump(scoped bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if scoped {
		f.scopedCalls++
	} else {
		f.unscopedCalls++
	}
}

func (f *scopedFakeAuditReconciler) calls() (scoped, unscoped int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scopedCalls, f.unscopedCalls
}

func (f *scopedFakeAuditReconciler) ListUnreconciledAdmissions(ctx context.Context, before time.Time, after uint64, limit int) ([]*store.AuditRecord, error) {
	f.bump(false)
	return f.fakeAuditReconciler.ListUnreconciledAdmissions(ctx, before, after, limit)
}

func (f *scopedFakeAuditReconciler) CountUnreconciledAdmissions(ctx context.Context, before time.Time) (int, time.Time, error) {
	f.bump(false)
	return f.fakeAuditReconciler.CountUnreconciledAdmissions(ctx, before)
}

func (f *scopedFakeAuditReconciler) inScope(owned []namespace.Namespace, rec *store.AuditRecord) bool {
	for _, v := range store.AuditScopeValues(owned) {
		if rec.Namespace == v {
			return true
		}
	}
	return false
}

func (f *scopedFakeAuditReconciler) ListUnreconciledAdmissionsIn(ctx context.Context, owned []namespace.Namespace, before time.Time, after uint64, limit int) ([]*store.AuditRecord, error) {
	f.bump(true)
	all, err := f.fakeAuditReconciler.ListUnreconciledAdmissions(ctx, before, after, 1<<20)
	if err != nil {
		return nil, err
	}
	var out []*store.AuditRecord
	for _, rec := range all {
		if f.inScope(owned, rec) && len(out) < limit {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (f *scopedFakeAuditReconciler) CountUnreconciledAdmissionsIn(ctx context.Context, owned []namespace.Namespace, before time.Time) (int, time.Time, error) {
	f.bump(true)
	all, err := f.fakeAuditReconciler.ListUnreconciledAdmissions(ctx, before, 0, 1<<20)
	if err != nil {
		return 0, time.Time{}, err
	}
	var count int
	var oldest time.Time
	for _, rec := range all {
		if !f.inScope(owned, rec) {
			continue
		}
		count++
		if oldest.IsZero() || rec.Timestamp.Before(oldest) {
			oldest = rec.Timestamp
		}
	}
	return count, oldest, nil
}

func TestAuditReconcileWorkerScope(t *testing.T) {
	tests := []struct {
		name       string
		owned      []namespace.Namespace
		wantSettle []string
		wantScoped bool
	}{
		// Mutation: scoping the nil case would stop legacy deployments from
		// settling anything that is not in a configured namespace.
		{"nil owned settles every namespace", nil, []string{"req-default", "req-empty", "req-local"}, false},
		{"empty entries collapse to unscoped", nsList(""), []string{"req-default", "req-empty", "req-local"}, false},
		// Mutation: ignoring the scope in list/count settles the other
		// deployment's admission, i.e. probes state that is not ours.
		{"dedicated namespace settles only its own rows", nsList("local-jack"), []string{"req-local"}, true},
		// Mutation: dropping the empty-namespace rule strands legacy rows that
		// carry no namespace, which belong to default.
		{"default also owns empty-namespace rows", nsList("default"), []string{"req-default", "req-empty"}, true},
		{"unowned namespace settles nothing", nsList("other"), nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			audit := &scopedFakeAuditReconciler{fakeAuditReconciler: &fakeAuditReconciler{}}
			past := time.Now().Add(-time.Minute)
			audit.addAdmission(admissionForRow("req-default", "default", opWorkflowCreate, "e1", past))
			audit.addAdmission(admissionForRow("req-empty", "", opWorkflowCreate, "e2", past))
			audit.addAdmission(admissionForRow("req-local", "local-jack", opWorkflowCreate, "e3", past))
			authority := &fakeAuthority{effects: map[string]MutationEffect{
				"req-default": EffectConfirmed, "req-empty": EffectConfirmed, "req-local": EffectConfirmed,
			}}
			w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
				Period: 10 * time.Millisecond, BacklogAge: time.Millisecond, Batch: 64,
				Elector: backend.AlwaysLeader{}, Namespaces: tc.owned,
			})
			if n := w.ReconcileOnce(context.Background()); n != len(tc.wantSettle) {
				t.Fatalf("settled = %d, want %d", n, len(tc.wantSettle))
			}
			var got []string
			for _, o := range audit.outcomes() {
				got = append(got, o.RequestID)
			}
			sort.Strings(got)
			if len(got) != len(tc.wantSettle) {
				t.Fatalf("outcomes for %v, want %v", got, tc.wantSettle)
			}
			for i := range got {
				if got[i] != tc.wantSettle[i] {
					t.Fatalf("outcomes for %v, want %v", got, tc.wantSettle)
				}
			}
			scoped, unscoped := audit.calls()
			if tc.wantScoped && (scoped != 2 || unscoped != 0) {
				t.Fatalf("calls scoped=%d unscoped=%d, want 2/0", scoped, unscoped)
			}
			if !tc.wantScoped && (scoped != 0 || unscoped != 2) {
				t.Fatalf("calls scoped=%d unscoped=%d, want 0/2", scoped, unscoped)
			}
		})
	}
}

// Mutation: removing the fail-closed guard lets a store that cannot scope be
// swept unscoped, settling every other deployment's admissions.
func TestAuditReconcileWorkerScopeWithoutScopedStoreFailsClosed(t *testing.T) {
	audit := &fakeAuditReconciler{}
	audit.addAdmission(admissionForRow("req-1", "local-jack", opWorkflowCreate, "e1", time.Now().Add(-time.Minute)))
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-1": EffectConfirmed}}
	logger := &recordingLogger{}
	w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period: 10 * time.Millisecond, BacklogAge: time.Millisecond, Batch: 64,
		Elector: backend.AlwaysLeader{}, Namespaces: nsList("local-jack"), Logger: logger,
	})
	for i := 0; i < 3; i++ {
		if n := w.ReconcileOnce(context.Background()); n != 0 {
			t.Fatalf("sweep %d settled = %d, want 0", i, n)
		}
	}
	if authority.probeCount() != 0 || audit.outcomeCount() != 0 {
		t.Fatalf("probes=%d outcomes=%d, want nothing touched", authority.probeCount(), audit.outcomeCount())
	}
	var errs int
	for _, e := range logger.entries {
		if e.level == "error" {
			errs++
		}
	}
	if errs != 1 {
		t.Fatalf("error log entries = %d, want exactly 1 across repeated sweeps", errs)
	}
}
