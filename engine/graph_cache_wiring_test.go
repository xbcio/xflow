package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// countingState wraps the standard fake so a test can count how often the
// engine actually reaches the state store. Every cache assertion below is
// really "did the decode happen again".
type countingState struct {
	*fakeState
	mu    sync.Mutex
	loads int
}

func (c *countingState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	return c.fakeState.LoadGraph(ctx, id)
}

func (c *countingState) loadCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}

func compileOneNodeGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  name,
		Nodes: []types.NodeDef{{Name: "A", Type: "test.a"}},
	})
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return g
}

// seedGraph registers an execution whose snapshot carries a compiled graph, the
// way a real execution snapshot does, and returns it.
func seedGraph(t *testing.T, st *fakeState, id types.ExecutionID) *graph.Graph {
	t.Helper()
	g := compileOneNodeGraph(t, string(id))
	st.executions[id] = &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	return g
}

// TestLoadGraphCachesDecodedGraph pins the reason the cache exists: a second
// dispatch of the same execution must not decode the graph again, and must see
// the same pointer (the graph is immutable and shared).
func TestLoadGraphCachesDecodedGraph(t *testing.T) {
	st := newFakeState()
	cs := &countingState{fakeState: st}
	seedGraph(t, st, "exec-a")

	e := New(cs, nil)
	first, err := e.loadGraph(context.Background(), "exec-a")
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	second, err := e.loadGraph(context.Background(), "exec-a")
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if first != second {
		t.Error("✓ a cache hit must return the same decoded graph")
	}
	if got := cs.loadCount(); got != 1 {
		t.Errorf("state loads = %d, want 1", got)
	}
}

// TestLoadGraphCacheCapacityEvicts proves the wiring to the bound: with a byte
// budget that fits one graph but not two, a second execution pushes the first
// out and its next load goes back to the state store.
func TestLoadGraphCacheCapacityEvicts(t *testing.T) {
	st := newFakeState()
	cs := &countingState{fakeState: st}
	ga := seedGraph(t, st, "exec-a")
	gb := seedGraph(t, st, "exec-b")

	e := New(cs, nil, WithGraphCache(ga.EstimatedHeapBytes()+gb.EstimatedHeapBytes()-1, 0))
	ctx := context.Background()
	mustLoad(t, e, ctx, "exec-a")
	mustLoad(t, e, ctx, "exec-b") // evicts exec-a by capacity
	mustLoad(t, e, ctx, "exec-a") // must reload

	if got := cs.loadCount(); got != 3 {
		t.Errorf("state loads = %d, want 3 (a, b, a-again)", got)
	}
}

// TestEvictExecutionForcesReload keeps the terminal-transition path intact:
// after EvictExecution the next task re-reads the graph instead of using a
// stale copy.
func TestEvictExecutionForcesReload(t *testing.T) {
	st := newFakeState()
	cs := &countingState{fakeState: st}
	seedGraph(t, st, "exec-a")

	e := New(cs, nil)
	ctx := context.Background()
	mustLoad(t, e, ctx, "exec-a")
	e.EvictExecution("exec-a")
	mustLoad(t, e, ctx, "exec-a")

	if got := cs.loadCount(); got != 2 {
		t.Errorf("state loads = %d, want 2", got)
	}
}

// TestLoadGraphDoesNotCacheMiss keeps a missing graph a miss every time: an
// execution that has no stored graph must not occupy a cache slot (and, more
// importantly, must not make a later appearance look cached).
func TestLoadGraphDoesNotCacheMiss(t *testing.T) {
	cs := &countingState{fakeState: newFakeState()}
	e := New(cs, nil)

	ctx := context.Background()
	if g, err := e.loadGraph(ctx, "exec-missing"); err != nil || g != nil {
		t.Fatalf("loadGraph(missing) = %v, %v; want nil, nil", g, err)
	}
	if g, err := e.loadGraph(ctx, "exec-missing"); err != nil || g != nil {
		t.Fatalf("loadGraph(missing) second = %v, %v; want nil, nil", g, err)
	}
	if got := cs.loadCount(); got != 2 {
		t.Errorf("state loads = %d, want 2 (a nil graph is never cached)", got)
	}
}

// TestGraphCacheEvictionDropsTraceCarrier ties the two per-execution maps
// together: when the cache drops a graph for idleness or capacity, the
// trace-carrier memo keyed by the same id must go with it, or the memo becomes
// the new leak.
func TestGraphCacheEvictionDropsTraceCarrier(t *testing.T) {
	st := newFakeState()
	cs := &countingState{fakeState: st}
	ga := seedGraph(t, st, "exec-a")
	gb := seedGraph(t, st, "exec-b")

	e := New(cs, nil, WithGraphCache(ga.EstimatedHeapBytes()+gb.EstimatedHeapBytes()-1, 0))
	ctx := context.Background()
	mustLoad(t, e, ctx, "exec-a")

	e.mu.Lock()
	e.traceCarriers["exec-a"] = &traceCarrierCacheEntry{carrier: map[string]string{"traceparent": "x"}}
	e.mu.Unlock()

	mustLoad(t, e, ctx, "exec-b") // capacity-evicts exec-a

	e.mu.Lock()
	_, kept := e.traceCarriers["exec-a"]
	e.mu.Unlock()
	if kept {
		t.Error("✓ evicting a graph must also drop its trace-carrier memo")
	}
}

// seedBoundedGraph is seedGraph for a workflow that declares settings.timeout,
// with the execution deadline stored on its snapshot so the engine has one to
// memoize.
func seedBoundedGraph(t *testing.T, st *fakeState, id types.ExecutionID) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     string(id),
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour},
		Nodes:    []types.NodeDef{{Name: "A", Type: "test.a"}},
	})
	if err != nil {
		t.Fatalf("compile %s: %v", id, err)
	}
	st.executions[id] = &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		Deadline: time.Now().UTC().Add(time.Hour),
	}
	return g
}

// TestGraphCacheEvictionDropsDeadlineMemo is the deadline twin of the
// trace-carrier test above: a graph the cache drops for capacity must take its
// execution-deadline memo with it, or the memo becomes the per-execution leak
// the bound exists to prevent.
//
// Which of the two entries a capacity trim picks depends on their last-use
// timestamps, so the test does not name the victim. It requires exactly one
// eviction, and that every execution's memo is present if and only if its graph
// is still cached: a memo left behind by the victim and a memo wiped from the
// survivor both fail it.
func TestGraphCacheEvictionDropsDeadlineMemo(t *testing.T) {
	st := newFakeState()
	ga := seedBoundedGraph(t, st, "exec-a")
	gb := seedBoundedGraph(t, st, "exec-b")

	e := New(st, nil, WithGraphCache(ga.EstimatedHeapBytes()+gb.EstimatedHeapBytes()-1, 0))
	ctx := context.Background()
	// Memoize both before any eviction, so a memo found afterwards can only be
	// one the eviction failed to clear.
	for id, g := range map[types.ExecutionID]*graph.Graph{"exec-a": ga, "exec-b": gb} {
		if _, err := e.executionDeadline(ctx, id, g); err != nil {
			t.Fatalf("executionDeadline(%q): %v", id, err)
		}
	}
	if got := deadlineMemoLen(e); got != 2 {
		t.Fatalf("memo entries before eviction = %d, want 2", got)
	}

	mustLoad(t, e, ctx, "exec-a")
	mustLoad(t, e, ctx, "exec-b") // over the byte budget: one of the two is evicted

	if got := e.graphs.Stats().EvictedCapacity; got != 1 {
		t.Fatalf("capacity evictions = %d, want exactly 1", got)
	}
	for _, id := range []types.ExecutionID{"exec-a", "exec-b"} {
		_, cached := e.graphs.Get(id)
		e.mu.RLock()
		_, memoized := e.executionDeadlines[id]
		e.mu.RUnlock()
		if memoized != cached {
			t.Errorf("%s: graph cached = %v, deadline memoized = %v, want them to agree", id, cached, memoized)
		}
	}
}

// TestGraphCacheIdleTTLIsWired proves the option reaches the cache. The sweep
// is opportunistic — it runs on Put, so the third execution's first load is
// what expires the idle entry; exec-a's next load must then go back to the
// state store.
func TestGraphCacheIdleTTLIsWired(t *testing.T) {
	st := newFakeState()
	cs := &countingState{fakeState: st}
	seedGraph(t, st, "exec-a")
	seedGraph(t, st, "exec-b")

	e := New(cs, nil, WithGraphCache(0, time.Nanosecond))
	ctx := context.Background()
	mustLoad(t, e, ctx, "exec-a")
	time.Sleep(time.Millisecond)  // let exec-a age past the TTL
	mustLoad(t, e, ctx, "exec-b") // Put runs the sweep and drops exec-a
	mustLoad(t, e, ctx, "exec-a") // must be re-decoded

	if got := cs.loadCount(); got != 3 {
		t.Errorf("state loads = %d, want 3 (idle entry re-decoded)", got)
	}
}

func mustLoad(t *testing.T, e *Engine, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	g, err := e.loadGraph(ctx, id)
	if err != nil {
		t.Fatalf("loadGraph(%q): %v", id, err)
	}
	if g == nil {
		t.Fatalf("loadGraph(%q) returned no graph", id)
	}
}
