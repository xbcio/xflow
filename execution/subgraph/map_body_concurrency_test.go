package subgraph

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// overlapHandler is the body member for the concurrency tests. It records the
// PEAK number of its own concurrent invocations, which is the only thing that
// separates "the items ran in parallel" from "the items ran fast".
//
// A wall-clock assertion cannot make that distinction: 8 items at 40ms each
// finishing in 60ms could be 8-way parallelism or a machine that happened to
// schedule the serial loop well. peak() answers directly.
type overlapHandler struct {
	// hold is how long each invocation stays inside, creating the window in
	// which a second invocation can be observed. It must exceed the per-item
	// setup cost (~30us measured: backend construction, engine wiring, submit)
	// by a wide margin, or a serial loop's items could appear to overlap purely
	// through the next item's setup racing the previous one's teardown.
	hold time.Duration

	mu      sync.Mutex
	inFlan  int
	peak    int
	invoked int
}

func (h *overlapHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.overlap"}
}

func (h *overlapHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.inFlan++
	h.invoked++
	if h.inFlan > h.peak {
		h.peak = h.inFlan
	}
	h.mu.Unlock()

	time.Sleep(h.hold)

	h.mu.Lock()
	h.inFlan--
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"item": in.Data["$item"]}}, nil
}

func (h *overlapHandler) stats() (peak, invoked int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peak, h.invoked
}

// buildOverlapBodyPackage is a single-member body whose member is
// overlapHandler. One member, not a chain: this test is about how many ITEMS
// run at once, and a chain would add per-item hops that only blur the signal.
func buildOverlapBodyPackage(t *testing.T) (*graph.SubgraphPackage, string) {
	t.Helper()
	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "overlap-body",
		EntryNode: "work",
		Def: &types.WorkflowDef{
			Name: "overlap-body",
			Nodes: []types.NodeDef{
				{Name: "work", Type: "test.overlap", Version: 1},
				{Name: "__collector_work_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"work": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_work_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_work_main", SrcNode: "work", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.overlap", NodeVersion: 1},
		},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	return pkg, hash
}

// newOverlapExecutor wires a real Executor over a real local backend, the same
// way service/runner/group_runtime.go does. Nothing here is stubbed: a fake
// executor would prove nothing about whether the production loop parallelises
// (see the probe-that-stuffs-the-field lesson).
func newOverlapExecutor(t *testing.T, hold time.Duration) (*Executor, *overlapHandler) {
	t.Helper()
	h := &overlapHandler{hold: hold}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", h)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
	return ex, h
}

func itemsN(n int) []any {
	items := make([]any, 0, n)
	for i := range n {
		items = append(items, i)
	}
	return items
}

// TestExecuteBatchBody_DefaultStaysSerial pins the default. Every workflow
// written before body_concurrency existed must keep running exactly one item at
// a time -- the setting is opt-in because parallelism multiplies the peak
// resource use of a deliberately-sized batch and weakens fail-fast (see
// TestExecuteBatchBody_ParallelStillStopsEarlyOnFailure).
//
// Concurrency 0 and 1 are both spelled out: 0 is what an absent parameter
// yields, and treating it as "unbounded" instead of "serial" is the exact
// mistake that would silently parallelise every existing map node.
func TestExecuteBatchBody_DefaultStaysSerial(t *testing.T) {
	for _, concurrency := range []int{0, 1} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			ex, h := newOverlapExecutor(t, 20*time.Millisecond)
			pkg, hash := buildOverlapBodyPackage(t)
			x := NewMapBodyExecutor(ex, false, time.Time{})

			results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
				Body:            pkg,
				BodyHash:        hash,
				Items:           itemsN(8),
				BatchSize:       8,
				BodyConcurrency: concurrency,
			})
			if err != nil {
				t.Fatalf("execute batch body: %v", err)
			}
			if len(results) != 8 {
				t.Fatalf("results = %d, want 8", len(results))
			}
			peak, invoked := h.stats()
			if peak != 1 {
				t.Fatalf("peak concurrent items = %d, want 1: body_concurrency=%d must stay serial",
					peak, concurrency)
			}
			if invoked != 8 {
				t.Fatalf("body invoked %d time(s), want 8", invoked)
			}
		})
	}
}

// TestExecuteBatchBody_ParallelRunsItemsConcurrently is the positive control
// for the feature: with body_concurrency=8 and 8 items, all 8 must be in the
// body at once.
//
// It asserts the EXACT peak, not ">1". A partial parallelisation -- two items
// at a time because a lock or a shared backend serialises the rest -- is the
// most likely wrong implementation, and ">1" would accept it.
func TestExecuteBatchBody_ParallelRunsItemsConcurrently(t *testing.T) {
	ex, h := newOverlapExecutor(t, 50*time.Millisecond)
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:            pkg,
		BodyHash:        hash,
		Items:           itemsN(8),
		BatchSize:       8,
		BodyConcurrency: 8,
	})
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}
	if len(results) != 8 {
		t.Fatalf("results = %d, want 8", len(results))
	}
	peak, invoked := h.stats()
	if peak != 8 {
		t.Fatalf("peak concurrent items = %d, want 8: body_concurrency=8 did not parallelise "+
			"(a peak of 1 means the loop is still serial; 2-7 means something downstream "+
			"serialises part of it)", peak)
	}
	if invoked != 8 {
		t.Fatalf("body invoked %d time(s), want 8", invoked)
	}
}

// TestExecuteBatchBody_ConcurrencyIsCapped is the reason body_concurrency is a
// NUMBER rather than a bool: the cap is the whole point. A batch of 40 items
// running 40-wide would multiply the peak memory of a deliberately-sized batch
// by 40 -- for the wasm bodies this feature exists for, that is 40 live guest
// instances instead of 4.
//
// 20 items at a cap of 4 also proves the semaphore is released and reused: a
// cap that only admitted the first 4 and then deadlocked would never reach 20
// invocations.
func TestExecuteBatchBody_ConcurrencyIsCapped(t *testing.T) {
	ex, h := newOverlapExecutor(t, 20*time.Millisecond)
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:            pkg,
		BodyHash:        hash,
		Items:           itemsN(20),
		BatchSize:       20,
		BodyConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}
	if len(results) != 20 {
		t.Fatalf("results = %d, want 20", len(results))
	}
	peak, invoked := h.stats()
	if peak > 4 {
		t.Fatalf("peak concurrent items = %d, want <= 4: the cap was exceeded", peak)
	}
	if peak < 2 {
		t.Fatalf("peak concurrent items = %d: nothing ran in parallel, so this test "+
			"cannot tell a working cap from a broken one", peak)
	}
	if invoked != 20 {
		t.Fatalf("body invoked %d time(s), want 20: a cap that is never released would "+
			"stop after the first window", invoked)
	}
}

// TestExecuteBatchBody_ParallelPreservesIndexOrder pins that results stay in
// item order regardless of the order items FINISH.
//
// Under parallelism completion order is nondeterministic, so an implementation
// that appends results as they arrive would return them shuffled -- and every
// downstream reader (BatchResultForCommit, completeLoopSplit's flatten,
// results[i] in a user's expression) reads them positionally. The handler holds
// for a duration that DECREASES with the index, so the natural completion order
// is the reverse of the item order: an append-on-completion implementation
// fails this loudly instead of passing by luck.
func TestExecuteBatchBody_ParallelPreservesIndexOrder(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", &reverseFinishHandler{})
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	const n = 8
	results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:            pkg,
		BodyHash:        hash,
		Items:           itemsN(n),
		BatchSize:       n,
		BatchIndex:      2, // non-zero, so a global-index bug cannot hide behind 0
		BodyConcurrency: n,
	})
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}
	if len(results) != n {
		t.Fatalf("results = %d, want %d", len(results), n)
	}
	for pos, r := range results {
		wantIndex := 2*n + pos
		if r.Index != wantIndex {
			t.Fatalf("results[%d].Index = %d, want %d: parallel items must be reported in "+
				"item order, not completion order", pos, r.Index, wantIndex)
		}
		if r.Err != nil {
			t.Fatalf("results[%d]: unexpected error %v", pos, r.Err)
		}
		got, _ := r.Data["item"].(int)
		if got != pos {
			t.Fatalf("results[%d].Data[item] = %v, want %d: the slot holds another item's "+
				"result", pos, r.Data["item"], pos)
		}
	}
}

// reverseFinishHandler finishes later for earlier items, so completion order is
// the reverse of item order. See TestExecuteBatchBody_ParallelPreservesIndexOrder.
type reverseFinishHandler struct{}

func (reverseFinishHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.overlap"}
}

func (reverseFinishHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	idx, _ := in.Data["$item"].(int)
	time.Sleep(time.Duration(40-idx*4) * time.Millisecond)
	return &types.Output{Data: map[string]any{"item": idx}}, nil
}

// TestExecuteBatchBody_ParallelStillStopsEarlyOnFailure pins the honest limit
// of parallel fail-fast.
//
// Serially, ContinueOnError=false means "no item after the failure runs at
// all". Under parallelism that promise cannot be kept for items already in
// flight -- there is no rollback -- so the guarantee weakens to: no item is
// STARTED after the failure is observed. This test pins the weakened form
// rather than pretending the strong one survives, and it pins that the batch
// still reports a failure.
//
// With 20 items at a cap of 4 and item 0 failing immediately, a correct
// implementation starts at most a few more items; a broken one runs all 20.
func TestExecuteBatchBody_ParallelStillStopsEarlyOnFailure(t *testing.T) {
	h := &failFirstHandler{}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", h)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:            pkg,
		BodyHash:        hash,
		Items:           itemsN(20),
		BatchSize:       20,
		BodyConcurrency: 4,
		ContinueOnError: false,
	})
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}

	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		}
	}
	if failed == 0 {
		t.Fatal("no item reported a failure, but item 0 always fails")
	}
	if started := int(h.started.Load()); started >= 20 {
		t.Fatalf("%d of 20 items started after item 0 failed: ContinueOnError=false must "+
			"stop admitting new items once a failure is observed", started)
	}
}

// failFirstHandler fails item 0 immediately and holds every other item, so the
// failure is observable long before the rest of the batch could finish.
type failFirstHandler struct {
	started atomic.Int64
}

func (*failFirstHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.overlap"}
}

func (h *failFirstHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.started.Add(1)
	idx, _ := in.Data["$item"].(int)
	if idx == 0 {
		return nil, fmt.Errorf("item 0 always fails")
	}
	time.Sleep(30 * time.Millisecond)
	return &types.Output{Data: map[string]any{"item": idx}}, nil
}
