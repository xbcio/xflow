package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// nsOutboxDiscoveryLimit is far above the handful of executions a case seeds, so
// a page boundary never decides whether discovery found something. The contract
// is about WHAT is reported, not how many calls it takes to report it.
const nsOutboxDiscoveryLimit = 1024

// runNamespacedOutboxLister exercises engine.NamespacedOutboxLister against a
// backend whose manifest declares CapabilityNamespacedOutboxLister.
//
// The capability exists because discovery runs on a context that carries no
// namespace, while every other outbox method resolves its keys from the
// namespace on the context it is given. An execution found by bare ID in a
// dedicated namespace is therefore looked up under the default one, finds an
// empty outbox, and is never delivered -- with no error, because an empty claim
// is a normal result. Four things have to hold for a flush to work:
//
//   - each ref carries the namespace its execution lives in;
//   - two namespaces holding the same execution ID are two refs, not one;
//   - ListOutboxExecutions reports the same executions, ids only, so the two
//     views of "what is pending" cannot disagree;
//   - the ref's namespace, put on the context, is enough to lease the entry.
//
// A backend whose outbox is not partitioned by namespace has nothing to report
// and leaves the capability undeclared: the case skips explicitly rather than
// asking that backend for a partition it does not have.
//
// The store is shared with the rest of RunStateStoreContract, so every case
// seeds execution IDs of its own and asserts on those, never on the total.
func runNamespacedOutboxLister(t *testing.T, manifest *Manifest, state engine.StateStore) {
	t.Helper()
	lister, ok := OptionalCapability[engine.NamespacedOutboxLister](t, manifest, CapabilityNamespacedOutboxLister, state)
	if !ok {
		return
	}
	atomic := RequiredCapability[engine.AtomicStateStore](t, manifest, CapabilityAtomicStateStore, state)
	leaser := RequiredCapability[engine.OutboxLeaser](t, manifest, CapabilityOutboxLeaser, state)

	t.Run("ReportsEachExecutionsNamespace", func(t *testing.T) {
		seeds := []engine.OutboxExecutionRef{
			{Namespace: "nsol-acme", ID: "exec-nsol-only-acme"},
			{Namespace: "nsol-globex", ID: "exec-nsol-only-globex"},
		}
		for _, seed := range seeds {
			nsOutboxSeed(t, atomic, seed)
		}
		refs := nsOutboxRefs(t, lister)
		for _, seed := range seeds {
			if got := refsWithID(refs, seed.ID); len(got) != 1 || got[0] != seed {
				t.Fatalf("refs for %q = %+v, want exactly [%+v] -- a ref without its namespace "+
					"is flushed under \"default\" and the entry is never found", seed.ID, got, seed)
			}
		}
	})

	t.Run("KeepsSameNamedExecutionsApart", func(t *testing.T) {
		const shared = types.ExecutionID("exec-nsol-shared")
		seeds := []engine.OutboxExecutionRef{
			{Namespace: "nsol-acme", ID: shared},
			{Namespace: "nsol-globex", ID: shared},
		}
		for _, seed := range seeds {
			nsOutboxSeed(t, atomic, seed)
		}
		got := refsWithID(nsOutboxRefs(t, lister), shared)
		if len(got) != len(seeds) || got[0] == got[1] {
			t.Fatalf("refs for %q = %+v, want one per namespace %+v -- the second namespace's "+
				"execution was taken for a repeat of the first and is never delivered", shared, got, seeds)
		}
		for _, seed := range seeds {
			if !containsRef(got, seed) {
				t.Fatalf("refs for %q = %+v, missing %+v", shared, got, seed)
			}
		}
	})
	t.Run("ListOutboxExecutionsIsTheRefsProjection", func(t *testing.T) {
		for _, seed := range []engine.OutboxExecutionRef{
			{Namespace: "nsol-acme", ID: "exec-nsol-proj-shared"},
			{Namespace: "nsol-globex", ID: "exec-nsol-proj-shared"},
			{Namespace: "nsol-globex", ID: "exec-nsol-proj-only"},
		} {
			nsOutboxSeed(t, atomic, seed)
		}
		// Back to back, with nothing writing in between, so a difference between
		// the two answers is a disagreement and not a race.
		refs := nsOutboxRefs(t, lister)
		ids, err := atomic.ListOutboxExecutions(context.Background(), nsOutboxDiscoveryLimit)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() error = %v", err)
		}
		want := map[types.ExecutionID]struct{}{}
		for _, ref := range refs {
			want[ref.ID] = struct{}{}
		}
		got := map[types.ExecutionID]struct{}{}
		for _, id := range ids {
			got[id] = struct{}{}
		}
		for id := range want {
			if _, ok := got[id]; !ok {
				t.Fatalf("ListOutboxExecutions() omits %q although ListOutboxExecutionRefs reports it: "+
					"the two views disagree about what is pending", id)
			}
		}
		for id := range got {
			if _, ok := want[id]; !ok {
				t.Fatalf("ListOutboxExecutions() reports %q which ListOutboxExecutionRefs does not: "+
					"the two views disagree about what is pending", id)
			}
		}
	})

	t.Run("RefNamespaceIsEnoughToLeaseTheEntry", func(t *testing.T) {
		seeds := []engine.OutboxExecutionRef{
			{Namespace: "nsol-acme", ID: "exec-nsol-lease-acme"},
			{Namespace: "nsol-globex", ID: "exec-nsol-lease-globex"},
		}
		for _, seed := range seeds {
			nsOutboxSeed(t, atomic, seed)
		}
		refs := nsOutboxRefs(t, lister)
		for _, seed := range seeds {
			got := refsWithID(refs, seed.ID)
			if len(got) != 1 {
				t.Fatalf("refs for %q = %+v, want exactly one", seed.ID, got)
			}
			// The dispatcher's only handle on the execution is the ref, so the
			// namespace it hands back must be sufficient to claim the entry.
			ctx := namespace.WithNamespace(context.Background(), got[0].Namespace)
			entries, err := leaser.LeaseOutbox(ctx, got[0].ID, time.Now().UTC(), 16)
			if err != nil {
				t.Fatalf("LeaseOutbox(%+v) error = %v", got[0], err)
			}
			if len(entries) != 1 || entries[0].ID != nsOutboxEntryID(seed.ID) {
				t.Fatalf("LeaseOutbox on the discovered namespace of %+v = %+v, want the seeded entry %q",
					got[0], entries, nsOutboxEntryID(seed.ID))
			}
		}
	})
}

// nsOutboxEntryID is the outbox entry nsOutboxSeed writes for id.
func nsOutboxEntryID(id types.ExecutionID) string {
	return "root/" + string(id) + "/start/1"
}

// nsOutboxSeed creates one running execution with one ready outbox entry under
// the ref's namespace, the way a namespaced deployment creates it: the
// namespace rides on the context of the create.
func nsOutboxSeed(t *testing.T, atomic engine.AtomicStateStore, ref engine.OutboxExecutionRef) {
	t.Helper()
	g := ContractGraph()
	idx, _ := g.NodeIndex("start")
	ctx := namespace.WithNamespace(context.Background(), ref.Namespace)
	if err := atomic.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: ref.ID, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID: nsOutboxEntryID(ref.ID),
		Task: engine.Task{
			ExecutionID: ref.ID, NodeName: "start", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox(%+v) error = %v", ref, err)
	}
}

// nsOutboxRefs runs discovery on a context with NO namespace, which is the
// dispatcher's own: discovery must not depend on the caller's namespace to find
// work.
func nsOutboxRefs(t *testing.T, lister engine.NamespacedOutboxLister) []engine.OutboxExecutionRef {
	t.Helper()
	refs, err := lister.ListOutboxExecutionRefs(context.Background(), nsOutboxDiscoveryLimit)
	if err != nil {
		t.Fatalf("ListOutboxExecutionRefs() error = %v", err)
	}
	return refs
}

// refsWithID selects the refs that name id, in whatever namespaces they appear.
func refsWithID(refs []engine.OutboxExecutionRef, id types.ExecutionID) []engine.OutboxExecutionRef {
	var out []engine.OutboxExecutionRef
	for _, ref := range refs {
		if ref.ID == id {
			out = append(out, ref)
		}
	}
	return out
}

func containsRef(refs []engine.OutboxExecutionRef, want engine.OutboxExecutionRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}
