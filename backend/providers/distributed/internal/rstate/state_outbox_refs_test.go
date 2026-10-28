package rstate

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// seedRefsStore seeds one ready outbox entry per (namespace, id) pair into a
// fresh miniredis-backed store, each created under its own namespace the way a
// namespaced deployment creates them.
func seedRefsStore(t *testing.T, indexOn bool, seeds ...engine.OutboxExecutionRef) *Store {
	t.Helper()
	state, _, _ := newOutboxIndexTestStore(t)
	state.ConfigureOutboxReadyIndex(indexOn)
	for _, seed := range seeds {
		ctx := namespace.WithNamespace(context.Background(), seed.Namespace)
		seedReadyOutbox(t, state, ctx, seed.ID, time.Now().Add(-time.Second))
	}
	return state
}

// forEachDiscoveryPath runs fn once with the readiness index answering and once
// with only the keyspace sweep, because the namespace has to survive both: the
// index reads it from the key it iterates, the sweep from the key it parses.
func forEachDiscoveryPath(t *testing.T, fn func(t *testing.T, indexOn bool)) {
	t.Helper()
	for _, tc := range []struct {
		name    string
		indexOn bool
	}{{"index", true}, {"sweep", false}} {
		t.Run(tc.name, func(t *testing.T) { fn(t, tc.indexOn) })
	}
}

// TestListOutboxExecutionRefsReportsTheExecutionsNamespace pins the half of the
// fix that lives in the store: an execution created in a dedicated namespace is
// discovered together with that namespace.
//
// The call is made on a context with NO namespace, which is the dispatcher's own
// (Backend.StartBinding starts it on context.Background()). Discovery must not
// depend on the caller's namespace to find the work, and it must hand back the
// namespace the caller needs to flush it.
func TestListOutboxExecutionRefsReportsTheExecutionsNamespace(t *testing.T) {
	forEachDiscoveryPath(t, func(t *testing.T, indexOn bool) {
		want := engine.OutboxExecutionRef{Namespace: "acme", ID: "exec-refs-acme-0"}
		state := seedRefsStore(t, indexOn, want)

		got, err := state.ListOutboxExecutionRefs(context.Background(), 16)
		if err != nil {
			t.Fatalf("ListOutboxExecutionRefs() error = %v", err)
		}
		if !reflect.DeepEqual(got, []engine.OutboxExecutionRef{want}) {
			t.Fatalf("refs = %+v, want [%+v] -- the namespace is dropped, so the dispatcher "+
				"flushes this execution under \"default\" and never finds its outbox", got, want)
		}
	})
}

// TestListOutboxExecutionRefsKeepsSameNamedExecutionsApart is the dedupe key
// test. Two namespaces may each hold an execution called "exec-shared", with an
// outbox of its own; keyed by ID alone, the second to be collected was taken for
// a repeat of the first and silently never delivered.
func TestListOutboxExecutionRefsKeepsSameNamedExecutionsApart(t *testing.T) {
	forEachDiscoveryPath(t, func(t *testing.T, indexOn bool) {
		const shared = types.ExecutionID("exec-shared")
		// Listed in (namespace, ID) order, which is what discovery returns.
		want := []engine.OutboxExecutionRef{
			{Namespace: "acme", ID: shared},
			{Namespace: "globex", ID: "exec-only-globex"},
			{Namespace: "globex", ID: shared},
		}
		state := seedRefsStore(t, indexOn, want...)

		got, err := state.ListOutboxExecutionRefs(context.Background(), 64)
		if err != nil {
			t.Fatalf("ListOutboxExecutionRefs() error = %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("refs = %+v, want %+v (ordered by namespace, then ID) -- a same-named "+
				"execution in another namespace was swallowed by ID-only de-duplication", got, want)
		}
	})
}

// TestListOutboxExecutionsIsTheRefsProjection pins the compatibility contract:
// the ID-only method answers what the ref method does with the namespaces
// dropped, and a same-named execution in two namespaces is one ID there.
func TestListOutboxExecutionsIsTheRefsProjection(t *testing.T) {
	forEachDiscoveryPath(t, func(t *testing.T, indexOn bool) {
		state := seedRefsStore(t, indexOn,
			engine.OutboxExecutionRef{Namespace: "acme", ID: "exec-shared"},
			engine.OutboxExecutionRef{Namespace: "globex", ID: "exec-shared"},
			engine.OutboxExecutionRef{Namespace: "globex", ID: "exec-a"},
		)

		got, err := state.ListOutboxExecutions(context.Background(), 64)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() error = %v", err)
		}
		want := []types.ExecutionID{"exec-a", "exec-shared"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ids = %v, want %v", got, want)
		}
	})
}

// TestDiscoveredRefNamespaceIsWhatLeaseOutboxNeeds ties the discovery result to
// the failure it exists to prevent. LeaseOutbox builds its keys from the
// namespace on the context it is given: on the dispatcher's bare context it
// finds nothing, and on a context carrying the discovered namespace it claims
// the entry. If a ref's namespace ever stopped being enough to flush its
// execution, this is the test that says so.
func TestDiscoveredRefNamespaceIsWhatLeaseOutboxNeeds(t *testing.T) {
	want := engine.OutboxExecutionRef{Namespace: "acme", ID: "exec-lease-acme"}
	state := seedRefsStore(t, true, want)

	refs, err := state.ListOutboxExecutionRefs(context.Background(), 16)
	if err != nil || len(refs) != 1 {
		t.Fatalf("ListOutboxExecutionRefs() = %+v, %v, want exactly %+v", refs, err, want)
	}
	ref := refs[0]
	now := time.Now().UTC()

	bare, err := state.LeaseOutbox(context.Background(), ref.ID, now, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(bare ctx) error = %v", err)
	}
	if len(bare) != 0 {
		t.Fatalf("LeaseOutbox on a context with no namespace claimed %d entries, want 0 -- "+
			"the premise of the defect (the outbox is not visible without its namespace) no longer holds", len(bare))
	}

	scoped, err := state.LeaseOutbox(namespace.WithNamespace(context.Background(), ref.Namespace), ref.ID, now, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(namespaced ctx) error = %v", err)
	}
	if len(scoped) != 1 {
		t.Fatalf("LeaseOutbox on the discovered namespace claimed %d entries, want 1", len(scoped))
	}
}
