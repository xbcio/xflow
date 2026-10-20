package storecontract

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// ScopedAuditStore is what RunScopedAuditReconcilerContract needs from an
// implementation: it appends rows, and it answers the unscoped and scoped
// pending scans over them.
type ScopedAuditStore interface {
	store.AuditAppender
	store.AuditReconciler
	store.ScopedAuditReconciler
}

// RunScopedAuditReconcilerContract holds every ScopedAuditReconciler to the same
// scoping behavior: a hard filter on the row's own namespace, a
// (namespace, request_id)-correlated outcome check, empty-namespace rows owned
// by the default namespace alone, and fail-closed on an empty set. factory must
// return an empty store on every call.
func RunScopedAuditReconcilerContract(t *testing.T, factory func(t *testing.T) ScopedAuditStore) {
	t.Helper()
	ctx := context.Background()
	// SQL stamps created_at itself (now), memstore reads Timestamp; a cutoff an
	// hour ahead makes every seeded row old enough in both.
	before := time.Now().UTC().Add(time.Hour)
	admittedAt := time.Now().UTC().Add(-time.Hour)

	// The fixture. r-shared is the same client-supplied request id in two
	// namespaces: settled in default, crashed in local-jack. An outcome check
	// that ignored the namespace would hide local-jack's admission.
	type row struct {
		ns, req string
		settled bool
	}
	rows := []row{
		{"default", "r-default-1", false},
		{"default", "r-shared", true},
		{"", "r-empty-1", false},
		{"local-jack", "r-local-1", false},
		{"local-jack", "r-local-2", false},
		{"local-jack", "r-local-settled", true},
		{"local-jack", "r-shared", false},
		{"tenant-x", "r-other-1", false},
	}
	seed := func(t *testing.T) ScopedAuditStore {
		t.Helper()
		st := factory(t)
		for _, r := range rows {
			if err := st.AppendAudit(ctx, &store.AuditRecord{
				RequestID: r.req, Namespace: r.ns, Operation: "PutSupply",
				Phase: store.AuditPhaseAdmission, Outcome: store.AuditOutcomeAdmitted,
				Timestamp: admittedAt,
			}); err != nil {
				t.Fatalf("seed admission %s/%s: %v", r.ns, r.req, err)
			}
			if !r.settled {
				continue
			}
			if err := st.AppendAudit(ctx, &store.AuditRecord{
				RequestID: r.req, Namespace: r.ns, Operation: "PutSupply",
				Phase: store.AuditPhaseOutcome, Outcome: store.AuditOutcomeReconciled,
				Timestamp: admittedAt,
			}); err != nil {
				t.Fatalf("seed outcome %s/%s: %v", r.ns, r.req, err)
			}
		}
		return st
	}
	reqIDs := func(recs []*store.AuditRecord) []string {
		out := make([]string, 0, len(recs))
		for _, r := range recs {
			out = append(out, r.RequestID)
		}
		sort.Strings(out)
		return out
	}

	cases := []struct {
		name  string
		owned []namespace.Namespace
		want  []string
	}{
		// The default namespace owns the empty-namespace rows.
		{"default", []namespace.Namespace{namespace.Default}, []string{"r-default-1", "r-empty-1"}},
		// A dedicated deployment sees its own rows only — including the request id
		// that another namespace already settled — and none of the empty ones.
		{"dedicated", []namespace.Namespace{"local-jack"}, []string{"r-local-1", "r-local-2", "r-shared"}},
		{"other", []namespace.Namespace{"tenant-x"}, []string{"r-other-1"}},
		{"union", []namespace.Namespace{"local-jack", "tenant-x"}, []string{"r-local-1", "r-local-2", "r-other-1", "r-shared"}},
		{"duplicated-members", []namespace.Namespace{namespace.Default, namespace.Default}, []string{"r-default-1", "r-empty-1"}},
		{"unknown", []namespace.Namespace{"nobody"}, []string{}},
	}
	for _, tc := range cases {
		t.Run("scope/"+tc.name, func(t *testing.T) {
			st := seed(t)
			got, err := st.ListUnreconciledAdmissionsIn(ctx, tc.owned, before, 0, 100)
			if err != nil {
				t.Fatalf("ListUnreconciledAdmissionsIn: %v", err)
			}
			if ids := reqIDs(got); !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("list = %v, want %v", ids, tc.want)
			}
			pending, oldest, err := st.CountUnreconciledAdmissionsIn(ctx, tc.owned, before)
			if err != nil {
				t.Fatalf("CountUnreconciledAdmissionsIn: %v", err)
			}
			if pending != len(tc.want) {
				t.Fatalf("count = %d, want %d (count and list must describe the same rows)", pending, len(tc.want))
			}
			if (pending > 0) == oldest.IsZero() {
				t.Fatalf("pending=%d oldest=%v: oldest must be set exactly when something is pending", pending, oldest)
			}
		})
	}

	// Mutation note: expanding an empty set to "every namespace" must turn this
	// red — a caller whose configured set came out empty would then settle the
	// whole shared table, the exact cross-deployment write the scope prevents.
	t.Run("empty set fails closed", func(t *testing.T) {
		st := seed(t)
		for name, owned := range map[string][]namespace.Namespace{"nil": nil, "empty": {}} {
			got, err := st.ListUnreconciledAdmissionsIn(ctx, owned, before, 0, 100)
			if err != nil || len(got) != 0 {
				t.Fatalf("%s list = %d rows, err %v; want none", name, len(got), err)
			}
			pending, oldest, err := st.CountUnreconciledAdmissionsIn(ctx, owned, before)
			if err != nil || pending != 0 || !oldest.IsZero() {
				t.Fatalf("%s count = (%d, %v, %v); want (0, zero, nil)", name, pending, oldest, err)
			}
		}
	})

	// The scopes partition the table: listing every namespace's scope and
	// concatenating must reproduce the unscoped scan, row for row.
	t.Run("scopes partition the unscoped scan", func(t *testing.T) {
		st := seed(t)
		all, err := st.ListUnreconciledAdmissions(ctx, before, 0, 100)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissions: %v", err)
		}
		var union []*store.AuditRecord
		for _, ns := range []namespace.Namespace{namespace.Default, "local-jack", "tenant-x"} {
			part, err := st.ListUnreconciledAdmissionsIn(ctx, []namespace.Namespace{ns}, before, 0, 100)
			if err != nil {
				t.Fatalf("scope %s: %v", ns, err)
			}
			union = append(union, part...)
		}
		if got, want := reqIDs(union), reqIDs(all); !reflect.DeepEqual(got, want) {
			t.Fatalf("union of scopes = %v, unscoped = %v", got, want)
		}
		if len(all) != 6 {
			t.Fatalf("unscoped scan = %d rows, want the fixture's 6", len(all))
		}
	})

	// The cursor contract (afterSeqID, oldest-first, limit) must hold inside a
	// scope, and the pages must not leak rows from other scopes.
	t.Run("cursor pagination inside a scope", func(t *testing.T) {
		st := seed(t)
		owned := []namespace.Namespace{"local-jack"}
		page1, err := st.ListUnreconciledAdmissionsIn(ctx, owned, before, 0, 2)
		if err != nil || len(page1) != 2 {
			t.Fatalf("page 1 = %d rows, err %v; want 2", len(page1), err)
		}
		page2, err := st.ListUnreconciledAdmissionsIn(ctx, owned, before, page1[1].SeqID, 2)
		if err != nil || len(page2) != 1 {
			t.Fatalf("page 2 = %d rows, err %v; want the remaining 1", len(page2), err)
		}
		page3, err := st.ListUnreconciledAdmissionsIn(ctx, owned, before, page2[0].SeqID, 2)
		if err != nil || len(page3) != 0 {
			t.Fatalf("page 3 = %d rows, err %v; want none", len(page3), err)
		}
		var seen []*store.AuditRecord
		seen = append(append(seen, page1...), page2...)
		if got, want := reqIDs(seen), []string{"r-local-1", "r-local-2", "r-shared"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("pages = %v, want %v", got, want)
		}
	})
}
