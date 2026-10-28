package engine

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// markerKey tags the context a drain was started on, so a test can tell the
// dispatcher's own context apart from one derived from it.
type markerKey struct{}

// nsLeaseCall is what one flush showed the store: the namespace its context
// carried, the execution it asked about, and whether the context was derived
// from the drain's.
type nsLeaseCall struct {
	ns      namespace.Namespace
	id      types.ExecutionID
	derived bool
}

// nsLeaseState records, for every LeaseOutbox, the namespace on the context the
// flush ran under. That is the one thing the distributed store reads to build its
// keys, so it is the observable the defect is about. It implements OutboxLeaser
// (RenewOutbox only so the capability probe takes the lease path) and leases
// nothing, which ends each flush after one call.
type nsLeaseState struct {
	*fakeState
	mu    sync.Mutex
	calls []nsLeaseCall
}

func (s *nsLeaseState) LeaseOutbox(ctx context.Context, id types.ExecutionID, _ time.Time, _ int) ([]OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, nsLeaseCall{
		ns:      namespace.FromContext(ctx),
		id:      id,
		derived: ctx.Value(markerKey{}) != nil,
	})
	return nil, nil
}

func (s *nsLeaseState) RenewOutbox(_ context.Context, _ types.ExecutionID, _ []OutboxEntry, _ time.Time) ([]OutboxEntry, error) {
	return nil, nil
}

func (s *nsLeaseState) leaseCalls() []nsLeaseCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]nsLeaseCall(nil), s.calls...)
}

// nsRefListerState adds NamespacedOutboxLister. Its ID-only listing answers
// nothing, so a dispatcher that fell back to it would flush nothing and fail.
type nsRefListerState struct {
	*nsLeaseState
	refs []OutboxExecutionRef
}

func (s *nsRefListerState) ListOutboxExecutionRefs(_ context.Context, _ int) ([]OutboxExecutionRef, error) {
	return append([]OutboxExecutionRef(nil), s.refs...), nil
}

func (s *nsRefListerState) ListOutboxExecutions(context.Context, int) ([]types.ExecutionID, error) {
	return nil, nil
}

// idListerState is a store that predates the capability: bare IDs only.
type idListerState struct {
	*nsLeaseState
	ids []types.ExecutionID
}

func (s *idListerState) ListOutboxExecutions(context.Context, int) ([]types.ExecutionID, error) {
	return append([]types.ExecutionID(nil), s.ids...), nil
}

// drainContext carries a marker, plus a namespace when ns is set, standing in
// for the context Run was started with.
func drainContext(ns namespace.Namespace) context.Context {
	ctx := context.WithValue(context.Background(), markerKey{}, true)
	if ns != "" {
		ctx = namespace.WithNamespace(ctx, ns)
	}
	return ctx
}

// TestOutboxDispatcherFlushesEachRefUnderItsNamespace is the regression for the
// defect at the engine layer. The dispatcher runs on a context with no
// namespace, and a store that keys its outbox by namespace reads it from the
// context; so a ref found in "acme" has to be flushed on a context that says
// "acme", or the flush looks in "default" and finds nothing.
//
// The refs go in unsorted, with an exact repeat and an ID shared by two
// namespaces. They must come out ordered by (namespace, ID), the repeat dropped,
// and BOTH shared-ID executions flushed: de-duplicating or ordering by ID alone
// would flush one of them twice or never.
func TestOutboxDispatcherFlushesEachRefUnderItsNamespace(t *testing.T) {
	const shared = types.ExecutionID("exec-shared")
	state := &nsRefListerState{
		nsLeaseState: &nsLeaseState{fakeState: newFakeState()},
		refs: []OutboxExecutionRef{
			{Namespace: "globex", ID: shared},
			{Namespace: "acme", ID: shared},
			{Namespace: "acme", ID: "exec-a"},
			{Namespace: "acme", ID: "exec-a"},
		},
	}
	observer := &drainObserver{}
	eng := New(state, &toggleOutboxQueue{}, WithOutboxObserver(observer))

	// Serial, so the order is the dispatcher's and not the scheduler's.
	NewOutboxDispatcher(eng, time.Hour, WithOutboxFlushConcurrency(1)).drain(drainContext(""))

	want := []nsLeaseCall{
		{ns: "acme", id: "exec-a", derived: true},
		{ns: "acme", id: shared, derived: true},
		{ns: "globex", id: shared, derived: true},
	}
	if got := state.leaseCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("flushes = %+v, want %+v: each ref must be flushed once, under its own "+
			"namespace, on a context derived from the drain's, in (namespace, ID) order", got, want)
	}
	drains := observer.drainObservations()
	if len(drains) != 1 || drains[0].discovered != len(want) {
		t.Fatalf("drain observations = %+v, want one drain that discovered %d", drains, len(want))
	}
}

// TestOutboxDispatcherFlushesRefsConcurrentlyUnderTheirNamespaces is the same
// contract on the concurrent path, which is the default: the namespace must ride
// on the context each worker gets, not on state shared between workers.
func TestOutboxDispatcherFlushesRefsConcurrentlyUnderTheirNamespaces(t *testing.T) {
	want := make(map[nsLeaseCall]bool)
	var refs []OutboxExecutionRef
	for _, ns := range []namespace.Namespace{"acme", "globex", "initech"} {
		for _, id := range []types.ExecutionID{"exec-1", "exec-2", "exec-3"} {
			refs = append(refs, OutboxExecutionRef{Namespace: ns, ID: id})
			want[nsLeaseCall{ns: ns, id: id, derived: true}] = true
		}
	}
	state := &nsRefListerState{nsLeaseState: &nsLeaseState{fakeState: newFakeState()}, refs: refs}
	eng := New(state, &toggleOutboxQueue{})

	NewOutboxDispatcher(eng, time.Hour).drain(drainContext(""))

	got := make(map[nsLeaseCall]bool)
	for _, call := range state.leaseCalls() {
		if got[call] {
			t.Fatalf("flush %+v happened twice", call)
		}
		got[call] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flushes = %v, want %v", got, want)
	}
}

// TestOutboxDispatcherWithoutNamespacedListerIsUnchanged pins the other half of
// the contract: a store that does not implement NamespacedOutboxLister is
// discovered through ListOutboxExecutions, flushed in ID order, and flushed on
// the drain's own context exactly as it was before the capability existed.
//
// The drain's context here carries a namespace of its own, so "unchanged" is
// observable: the flush must still see it rather than have it replaced.
func TestOutboxDispatcherWithoutNamespacedListerIsUnchanged(t *testing.T) {
	state := &idListerState{
		nsLeaseState: &nsLeaseState{fakeState: newFakeState()},
		ids:          []types.ExecutionID{"exec-b", "exec-c", "exec-a"},
	}
	eng := New(state, &toggleOutboxQueue{})

	NewOutboxDispatcher(eng, time.Hour, WithOutboxFlushConcurrency(1)).drain(drainContext("tenant"))

	want := []nsLeaseCall{
		{ns: "tenant", id: "exec-a", derived: true},
		{ns: "tenant", id: "exec-b", derived: true},
		{ns: "tenant", id: "exec-c", derived: true},
	}
	if got := state.leaseCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("flushes = %+v, want %+v: a store with no namespaced listing must be "+
			"flushed in ID order on the dispatcher's own context", got, want)
	}
}

// TestOutboxDispatcherRefWithoutNamespaceKeepsTheDrainContext covers a lister
// that reports a ref with no namespace: there is nothing to apply, so the flush
// runs on the drain's context rather than being reset to "default".
func TestOutboxDispatcherRefWithoutNamespaceKeepsTheDrainContext(t *testing.T) {
	state := &nsRefListerState{
		nsLeaseState: &nsLeaseState{fakeState: newFakeState()},
		refs:         []OutboxExecutionRef{{ID: "exec-bare"}},
	}
	eng := New(state, &toggleOutboxQueue{})

	NewOutboxDispatcher(eng, time.Hour, WithOutboxFlushConcurrency(1)).drain(drainContext("tenant"))

	want := []nsLeaseCall{{ns: "tenant", id: "exec-bare", derived: true}}
	if got := state.leaseCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("flushes = %+v, want %+v", got, want)
	}
}
