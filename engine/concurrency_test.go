package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// plainStateStore hides the concurrency capability a *fakeState has, so a test
// can model a deployment whose backend cannot enforce a limit.
//
// AtomicStateStore and StateStore are embedded as INTERFACES rather than
// *fakeState on purpose: embedding the concrete type would promote its
// CreateExecutionWithConcurrency and the capability assertion would succeed.
// The engine therefore sees a store that is fully atomic and simply lacks the
// concurrency extension — the exact shape of a real backend that has not
// implemented it yet.
type plainStateStore struct {
	StateStore
	AtomicStateStore
}

var _ StateStore = (*plainStateStore)(nil)
var _ AtomicStateStore = (*plainStateStore)(nil)

func compileConcurrencyGraph(t *testing.T, name, version string, settings *types.ConcurrencySettings) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     name,
		Version:  version,
		Settings: &types.WorkflowSettings{Concurrency: settings},
		Nodes:    []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(%s@%s) error = %v", name, version, err)
	}
	return g
}

func compileLimitedGraph(t *testing.T, limit int) *graph.Graph {
	t.Helper()
	return compileConcurrencyGraph(t, "conc", "1", &types.ConcurrencySettings{
		Limit: limit,
		Mode:  types.ConcurrencyModeReject,
	})
}

// persistedFor counts the executions whose persisted snapshot is this exact
// compiled graph, which is what the slot tests need: they submit more than one
// version of the same workflow name.
func persistedFor(state *fakeState, g *graph.Graph) []*ExecutionSnapshot {
	state.mu.Lock()
	defer state.mu.Unlock()
	var found []*ExecutionSnapshot
	for _, e := range state.executions {
		if e.Graph == g {
			found = append(found, e)
		}
	}
	return found
}

// TestSubmitRejectsOverTheLimit pins the whole point of reject mode: the
// submission that crosses the limit is refused with the sentinel the HTTP layer
// maps to 429, and it leaves nothing behind. A rejection that persisted an
// execution first would show up as a zero-node "running" workflow in the list
// API — the exact confusion the status code is supposed to prevent.
func TestSubmitRejectsOverTheLimit(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileLimitedGraph(t, 1)

	first, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("first Submit() error = %v, want success", err)
	}

	_, err = eng.Submit(ctx, g, nil)
	if !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("second Submit() error = %v, want ErrConcurrencyLimit", err)
	}
	execs := persistedFor(state, g)
	if len(execs) != 1 || execs[0].ID != first {
		t.Fatalf("persisted executions = %d (%+v), want only the admitted %q", len(execs), execs, first)
	}
}

// TestSubmitReleasesSlotOnTerminalStatus is the release half of the contract:
// the slot is owned by whoever writes the terminal status, so finishing an
// execution must free the limit for the next submission.
func TestSubmitReleasesSlotOnTerminalStatus(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileLimitedGraph(t, 1)

	first, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if err := eng.Cancel(ctx, first); err != nil {
		t.Fatalf("Cancel(%q) error = %v", first, err)
	}

	second, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() after the slot was released error = %v, want success", err)
	}
	if second == first {
		t.Fatal("Submit() reused the terminal execution's ID")
	}
}

// TestSubmitFailsClosedWithoutTheCapability covers the deployment that matters
// most: a workflow declares a limit that its backend cannot enforce. Running it
// unlimited would defeat a limit the author set to protect a downstream, so the
// submission is refused instead of quietly downgraded.
func TestSubmitFailsClosedWithoutTheCapability(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(&plainStateStore{AtomicStateStore: state}, &fakeQueue{})
	g := compileLimitedGraph(t, 5)

	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, ErrConcurrencyUnsupported) {
		t.Fatalf("Submit() error = %v, want ErrConcurrencyUnsupported", err)
	}
	if execs := persistedFor(state, g); len(execs) != 0 {
		t.Fatalf("persisted executions = %d, want none", len(execs))
	}
}

// TestSubmitRefusesQueueModeUntilItLands guards the phase boundary. A bare int
// `concurrency: N` is QUEUE mode, so "admit anyway" would be the wrong
// fallback for most definitions that carry the shorthand: it would silently
// break the limit rather than fail the submission.
func TestSubmitRefusesQueueModeUntilItLands(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileConcurrencyGraph(t, "conc", "1", &types.ConcurrencySettings{Limit: 2})

	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, ErrConcurrencyQueueUnsupported) {
		t.Fatalf("Submit(queue mode) error = %v, want ErrConcurrencyQueueUnsupported", err)
	}
	if execs := persistedFor(state, g); len(execs) != 0 {
		t.Fatalf("persisted executions = %d, want none", len(execs))
	}
}

// TestSlotsAreKeyedByWorkflowNameNotVersion pins the key scope. Keying by
// version would let a re-published definition legally run limit x versions
// concurrently, and would orphan the slots the previous version's executions
// still hold — the limit exists to bound the workflow's load on whatever it
// calls, and that does not change when the definition's version does.
func TestSlotsAreKeyedByWorkflowNameNotVersion(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})

	if _, err := eng.Submit(ctx, compileLimitedGraph(t, 1), nil); err != nil {
		t.Fatalf("Submit(v1) error = %v", err)
	}
	_, err := eng.Submit(ctx, compileConcurrencyGraph(t, "conc", "2", &types.ConcurrencySettings{
		Limit: 1,
		Mode:  types.ConcurrencyModeReject,
	}), nil)
	if !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("Submit(v2 while v1 holds the only slot) error = %v, want ErrConcurrencyLimit", err)
	}
}

// TestSlotsAreKeyedByNamespace pins isolation: two tenants running the same
// workflow name must each get their own limit, or one tenant's backlog would
// throttle another's.
func TestSlotsAreKeyedByNamespace(t *testing.T) {
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileLimitedGraph(t, 1)

	other := namespace.WithNamespace(context.Background(), namespace.Namespace("other-tenant"))
	if _, err := eng.Submit(other, g, nil); err != nil {
		t.Fatalf("Submit() in the other namespace error = %v", err)
	}
	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("Submit() in the default namespace error = %v, want its own slot", err)
	}
}

// TestConcurrentSubmitsRespectTheLimit exercises admission as the concurrent
// transition it is: N submitters race for a limit of one, exactly one wins, and
// the losers report the sentinel instead of over-admitting.
func TestConcurrentSubmitsRespectTheLimit(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileLimitedGraph(t, 1)

	const submitters = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var admitted, rejected int
	var other []error
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := eng.Submit(ctx, g, nil)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				admitted++
			case errors.Is(err, ErrConcurrencyLimit):
				rejected++
			default:
				other = append(other, err)
			}
		}()
	}
	wg.Wait()

	if len(other) != 0 {
		t.Fatalf("unexpected submission errors: %v", other)
	}
	if admitted != 1 || rejected != submitters-1 {
		t.Fatalf("admitted/rejected = %d/%d, want 1/%d", admitted, rejected, submitters-1)
	}
	if execs := persistedFor(state, g); len(execs) != 1 {
		t.Fatalf("persisted executions = %d, want exactly the admitted one", len(execs))
	}
}

// TestUnlimitedSubmissionPathUnchanged guards the blast radius of the feature:
// a graph that declares no limit must keep using the ordinary atomic creation
// path, with its initial tasks flushed to the queue.
func TestUnlimitedSubmissionPathUnchanged(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "plain",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].ExecutionID != id || tasks[0].NodeName != "start" {
		t.Fatalf("queued tasks = %v, want the start node of %q", taskNames(tasks), id)
	}
	if entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Hour)); len(entries) != 0 {
		t.Fatalf("initial outbox after a successful flush = %+v, want empty", entries)
	}
}
