package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// overlapBodyHandler records the PEAK number of its own concurrent invocations.
// That is the only measurement that separates "the items ran in parallel" from
// "the items ran fast" -- a wall-clock assertion cannot tell a well-scheduled
// serial loop from real overlap.
type overlapBodyHandler struct {
	mu       sync.Mutex
	inFlight int
	peak     int
}

func (h *overlapBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *overlapBodyHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.inFlight++
	if h.inFlight > h.peak {
		h.peak = h.inFlight
	}
	h.mu.Unlock()

	// Long enough to dwarf the ~30us per-item setup cost (backend construction,
	// engine wiring, submit), so a serial loop cannot appear to overlap through
	// one item's setup racing the previous one's teardown.
	time.Sleep(40 * time.Millisecond)

	h.mu.Lock()
	h.inFlight--
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"seen": in.Data["$item"]}}, nil
}

func (h *overlapBodyHandler) peakConcurrency() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peak
}

// concurrentBatchLease is batchBodyLease's shape with all 8 items in one batch
// and an explicit BodyConcurrency. One batch, because this test is about how many
// ITEMS of a single lease run at once -- the runner already gets batch-level
// concurrency from batches being separate leases.
func concurrentBatchLease(t *testing.T, pkg *graph.SubgraphPackage, concurrency int) *engine.TaskLease {
	t.Helper()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	items := []any{"a", "b", "c", "d", "e", "f", "g", "h"}
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-parent"),
		LeaseToken: engine.LeaseToken("token-parent"),
		Attempt:    1,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-conc"),
			NodeName:    "m/_batch/0",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeBatch,
		},
		NodeType: "xflow.map",
		SubgraphPayload: &engine.SubgraphLeasePayload{
			ProtocolVersion: 1,
			ParentNode:      "m",
			ParentNodeIdx:   0,
			BatchIndex:      0,
			ChildExecID:     types.ExecutionID("exec-conc/sub/m/lease-parent/0"),
			Items:           items,
			AllItems:        items,
			BatchSize:       len(items),
			Package:         pkg,
			PackageHash:     hash,
			BodyConcurrency: concurrency,
			Deadline:        time.Now().Add(30 * time.Second),
		},
	}
}

// runConcurrentBatch runs one lease through a real SubgraphRuntime and returns
// the body's peak concurrency plus the batch's item count. Nothing is stubbed:
// the point is that the runner's own assembly forwards the payload field, and a
// hand-built BatchBodyRequest would prove only that the executor works.
func runConcurrentBatch(t *testing.T, concurrency int) (peak, count int) {
	t.Helper()
	reg := execution.NewRegistry()
	body := &overlapBodyHandler{}
	reg.RegisterGlobal("test.body_item", body)
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}))

	res, err := rt.Execute(context.Background(), concurrentBatchLease(t, bodyPackageForTest(), concurrency))
	if err != nil {
		t.Fatalf("subgraph runtime execute: %v", err)
	}
	if res.Output == nil {
		t.Fatal("batch produced no output")
	}
	items, _ := res.Output.Data["items"].([]any)
	return body.peakConcurrency(), len(items)
}

// TestSubgraphRuntimeHonoursBodyConcurrency closes the wire route's second half.
// engine.BuildSubgraphLease putting BodyConcurrency on the payload buys nothing
// if this runtime does not copy it into the BatchBodyRequest -- and copying a
// struct field by field is exactly where this codebase has silently dropped new
// fields before.
func TestSubgraphRuntimeHonoursBodyConcurrency(t *testing.T) {
	peak, count := runConcurrentBatch(t, 8)
	if count != 8 {
		t.Fatalf("batch reported %d items, want 8", count)
	}
	if peak != 8 {
		t.Fatalf("peak concurrent body items = %d, want 8: the lease's BodyConcurrency did not "+
			"reach the body executor (a peak of 1 means the runtime dropped the field)", peak)
	}
}

// The default must stay serial on the runner too. Every batch lease built before
// BodyConcurrency existed carries the zero value, and treating zero as unbounded
// would parallelise every deployed map node the moment this code ships.
func TestSubgraphRuntimeStaysSerialWithoutBodyConcurrency(t *testing.T) {
	peak, count := runConcurrentBatch(t, 0)
	if count != 8 {
		t.Fatalf("batch reported %d items, want 8", count)
	}
	if peak != 1 {
		t.Fatalf("peak concurrent body items = %d, want 1: a lease with no BodyConcurrency "+
			"must run its items one at a time", peak)
	}
}
