//go:build stress

package stress

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// buildTriggerGroupGraph returns a compiled graph with a single trigger group
// containing one source and one sink node, plus a downstream "out" node.
func buildTriggerGroupGraph(tb testing.TB) *graph.Graph {
	tb.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "stress-trigger-group",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": types.PortConnections{Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	})
	if err != nil {
		tb.Fatalf("buildTriggerGroupGraph: %v", err)
	}
	return g
}

// groupUnitIdx returns the index of the first group-kind unit in the graph.
func groupUnitIdx(g *graph.Graph) int {
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			return i
		}
	}
	return -1
}

// BenchmarkGroupAdmission measures the throughput of SeedExecutionFromEntry
// on the local backend with concurrent callers. Each parallel iteration uses a
// unique admission key so no contention on duplicates.
func BenchmarkGroupAdmission(b *testing.B) {
	be := local.New()
	state := be.State().(engine.EntryAdmissionStore)
	g := buildTriggerGroupGraph(b)
	unitIdx := groupUnitIdx(g)

	ctx := context.Background()
	hash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, []engine.BoundaryExit{
		{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}},
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := engine.AdmissionKey(fmt.Sprintf("bench/wf/v1/g/topic/0/%d-%d", b.N+i, b.N+i))
			_, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
				AdmissionKey: key,
				EntryUnitID:  "g",
				EntryUnitIdx: unitIdx,
				Graph:        g,
				Outcome:      engine.GroupOutcomeSuccess,
				Exits: []engine.BoundaryExit{
					{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}},
				},
				ResultHash: hash,
			})
			if err != nil {
				b.Fatalf("SeedExecutionFromEntry: %v", err)
			}
			i++
		}
	})
}

// TestGroupStress_ConcurrentAdmissions verifies no data races or lost
// admissions under concurrent load. Each (worker, op) pair uses a unique
// admission key; all should be accepted without duplicates or conflicts.
func TestGroupStress_ConcurrentAdmissions(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}

	be := local.New()
	state := be.State().(engine.EntryAdmissionStore)
	g := buildTriggerGroupGraph(t)
	unitIdx := groupUnitIdx(g)
	ctx := context.Background()

	const numWorkers = 100
	const opsPerWorker = 100

	var accepted, duplicates, conflicts atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := engine.AdmissionKey(fmt.Sprintf("stress/wf/v1/g/topic/%d/%d-%d", workerID, i, i))
				hash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, []engine.BoundaryExit{
					{NodeName: "g.sink", Port: "main", Data: map[string]any{"w": workerID, "i": i}},
				})
				resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
					AdmissionKey: key,
					EntryUnitID:  "g",
					EntryUnitIdx: unitIdx,
					Graph:        g,
					Outcome:      engine.GroupOutcomeSuccess,
					Exits: []engine.BoundaryExit{
						{NodeName: "g.sink", Port: "main", Data: map[string]any{"w": workerID, "i": i}},
					},
					ResultHash: hash,
				})
				if err != nil {
					t.Errorf("worker %d op %d: %v", workerID, i, err)
					return
				}
				switch {
				case resp.Duplicate:
					duplicates.Add(1)
				case resp.State == engine.AdmissionStateConflict:
					conflicts.Add(1)
				default:
					accepted.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	total := accepted.Load() + duplicates.Load() + conflicts.Load()
	if total != numWorkers*opsPerWorker {
		t.Errorf("total ops = %d, want %d", total, numWorkers*opsPerWorker)
	}
	if accepted.Load() != numWorkers*opsPerWorker {
		t.Errorf("accepted = %d, want %d (all unique keys)", accepted.Load(), numWorkers*opsPerWorker)
	}
	if duplicates.Load() != 0 {
		t.Errorf("unexpected duplicates = %d", duplicates.Load())
	}
	if conflicts.Load() != 0 {
		t.Errorf("unexpected conflicts = %d", conflicts.Load())
	}

	t.Logf("stress: %d accepted, %d duplicates, %d conflicts", accepted.Load(), duplicates.Load(), conflicts.Load())
}

// TestGroupStress_DuplicateAdmissions verifies idempotency: submitting the same
// admission key + result hash concurrently always returns duplicate=true after
// the first acceptance, with no data loss.
func TestGroupStress_DuplicateAdmissions(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}

	be := local.New()
	state := be.State().(engine.EntryAdmissionStore)
	g := buildTriggerGroupGraph(t)
	unitIdx := groupUnitIdx(g)
	ctx := context.Background()

	const numWorkers = 50
	// All workers submit the same admission key with same hash.
	key := engine.AdmissionKey("dup/wf/v1/g/topic/0/0-0")
	hash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, []engine.BoundaryExit{
		{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}},
	})

	var accepted, duplicates atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
				AdmissionKey: key,
				EntryUnitID:  "g",
				EntryUnitIdx: unitIdx,
				Graph:        g,
				Outcome:      engine.GroupOutcomeSuccess,
				Exits: []engine.BoundaryExit{
					{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}},
				},
				ResultHash: hash,
			})
			if err != nil {
				t.Errorf("SeedExecutionFromEntry: %v", err)
				return
			}
			if resp.Duplicate {
				duplicates.Add(1)
			} else {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()

	// Exactly one should win first-writer, the rest are duplicates.
	if accepted.Load() != 1 {
		t.Errorf("expected exactly 1 accepted, got %d", accepted.Load())
	}
	if duplicates.Load() != numWorkers-1 {
		t.Errorf("expected %d duplicates, got %d", numWorkers-1, duplicates.Load())
	}
}

// TestGroupStress_BackpressureSaturation verifies that a semaphore-style
// backpressure mechanism correctly limits in-flight group operations and no
// data is lost under saturation.
func TestGroupStress_BackpressureSaturation(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}

	const maxInflight = 10
	const numWorkers = 50
	const opsPerWorker = 20

	sem := make(chan struct{}, maxInflight)
	var peakInflight atomic.Int64
	var currentInflight atomic.Int64
	var totalOps atomic.Int64
	var paused atomic.Int64
	var wg sync.WaitGroup

	be := local.New()
	state := be.State().(engine.EntryAdmissionStore)
	g := buildTriggerGroupGraph(t)
	unitIdx := groupUnitIdx(g)
	ctx := context.Background()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				// Attempt acquire — may block (backpressure).
				select {
				case sem <- struct{}{}:
				default:
					paused.Add(1)
					sem <- struct{}{} // block
				}

				cur := currentInflight.Add(1)
				// Track peak.
				for {
					peak := peakInflight.Load()
					if cur <= peak || peakInflight.CompareAndSwap(peak, cur) {
						break
					}
				}

				// Perform admission.
				key := engine.AdmissionKey(fmt.Sprintf("bp/wf/v1/g/topic/%d/%d-%d", workerID, i, i))
				hash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, []engine.BoundaryExit{
					{NodeName: "g.sink", Port: "main", Data: map[string]any{"w": workerID, "i": i}},
				})
				_, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
					AdmissionKey: key,
					EntryUnitID:  "g",
					EntryUnitIdx: unitIdx,
					Graph:        g,
					Outcome:      engine.GroupOutcomeSuccess,
					Exits: []engine.BoundaryExit{
						{NodeName: "g.sink", Port: "main", Data: map[string]any{"w": workerID, "i": i}},
					},
					ResultHash: hash,
				})
				if err != nil {
					t.Errorf("worker %d op %d: %v", workerID, i, err)
				}
				totalOps.Add(1)

				currentInflight.Add(-1)
				<-sem // release
			}
		}(w)
	}
	wg.Wait()

	if peakInflight.Load() > maxInflight {
		t.Errorf("peak inflight %d exceeded maxInflight %d", peakInflight.Load(), maxInflight)
	}
	if totalOps.Load() != numWorkers*opsPerWorker {
		t.Errorf("total ops = %d, want %d", totalOps.Load(), numWorkers*opsPerWorker)
	}
	if paused.Load() == 0 {
		t.Log("warning: no goroutine was paused by backpressure — increase numWorkers or decrease maxInflight for saturation")
	}

	t.Logf("backpressure: peak inflight=%d, total=%d, paused=%d",
		peakInflight.Load(), totalOps.Load(), paused.Load())
}
