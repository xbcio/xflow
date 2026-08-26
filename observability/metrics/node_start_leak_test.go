package metrics

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// MetricsHooks measures node duration by storing a start time on OnNodeStart and
// consuming it on OnNodeComplete. Every stored entry is keyed by execution ID,
// which is unbounded — one per message on a Kafka trigger — so any path that
// starts a node without completing it grows the map for the life of the process.
//
// Two such paths are proven in the engine: a commit that arrives after its
// execution went terminal returns CommitOutcomeExecutionInactive without firing
// OnNodeComplete (engine/commit.go), and Cancel moves a suspended node to
// Canceled through UpsertNode, also without firing it (engine/cancel.go). Both
// leave a node that was started and will never complete.
//
// These tests pin the eviction that bounds the map. They assert on live entry
// count rather than on memory, because the count is the thing with a correct
// value: one per node currently in flight, zero when nothing is running.

func TestNodeDurationEntryIsReleasedWhenExecutionEndsWithoutTheNode(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()

	hooks.OnNodeStart(ctx, types.ExecutionID("exec-cancelled"), "transform")
	if got := hooks.liveNodeStarts(); got != 1 {
		t.Fatalf("in-flight entries after one start = %d, want 1", got)
	}

	// The execution ends while that node is still Running. This is the signal
	// the engine does emit on both leak paths (engine/cancel.go:88,
	// engine/atomic.go:431), so it is what has to release the entry.
	hooks.OnExecutionComplete(ctx, types.ExecutionID("exec-cancelled"), types.ExecutionStatusCanceled)

	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after the execution ended = %d, want 0; the "+
			"node never completed, so nothing else will ever release it", got)
	}
}

// A late commit can still arrive after the execution was declared terminal. It
// must not resurrect the entry, or the eviction above would only postpone the
// leak by one call.
func TestLateNodeCompleteAfterExecutionEndDoesNotResurrectTheEntry(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()
	id := types.ExecutionID("exec-late")

	hooks.OnNodeStart(ctx, id, "transform")
	hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusFailed)
	hooks.OnNodeComplete(ctx, id, "transform", types.NodeStatusFailed)

	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after a late complete = %d, want 0", got)
	}
}

// Ending one execution must not evict another's in-flight nodes. A single shared
// map keyed only by node name would do exactly that, and the symptom would be a
// silently missing duration observation rather than an error.
func TestExecutionEndDoesNotEvictAnotherExecutionsNodes(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()

	hooks.OnNodeStart(ctx, types.ExecutionID("exec-a"), "transform")
	hooks.OnNodeStart(ctx, types.ExecutionID("exec-b"), "transform")
	hooks.OnNodeStart(ctx, types.ExecutionID("exec-b"), "sink")

	hooks.OnExecutionComplete(ctx, types.ExecutionID("exec-b"), types.ExecutionStatusSuccess)

	if got := hooks.liveNodeStarts(); got != 1 {
		t.Errorf("in-flight entries after ending exec-b = %d, want 1 (exec-a's "+
			"transform is still running)", got)
	}
}

// The normal path must be unchanged: complete observes a duration and releases
// the entry, and the execution ending afterwards is a no-op.
func TestNodeDurationIsStillObservedOnTheNormalPath(t *testing.T) {
	m := New()
	hooks := NewMetricsHooks(m)
	ctx := context.Background()
	id := types.ExecutionID("exec-ok")

	hooks.OnNodeStart(ctx, id, "transform")
	hooks.OnNodeComplete(ctx, id, "transform", types.NodeStatusSuccess)
	hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusSuccess)

	family := gatherMetricFamily(t, m, "xflow_node_duration_seconds")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("duration series = %d, want 1; the eviction removed the "+
			"observation it was supposed to preserve", len(family.GetMetric()))
	}
	if got := family.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("duration samples = %d, want 1", got)
	}
	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after a clean run = %d, want 0", got)
	}
}

// Not every terminal execution reaches OnExecutionComplete. engine.go's
// loadActiveGraph evicts a graph whose execution snapshot came back nil — state
// that expired or was deleted — and no completion hook fires for it. The age
// sweep is the backstop for that and for any path added later, so the map has a
// bound that does not depend on the engine's notification coverage being total.
func TestStaleNodeStartsAreSweptByAge(t *testing.T) {
	hooks := NewMetricsHooks(New())

	old := time.Now().Add(-2 * nodeStartMaxAge)
	hooks.storeNodeStartAt(types.ExecutionID("exec-vanished"), "transform", old)
	hooks.storeNodeStartAt(types.ExecutionID("exec-fresh"), "transform", time.Now())
	if got := hooks.liveNodeStarts(); got != 2 {
		t.Fatalf("seeded entries = %d, want 2", got)
	}

	hooks.sweepStaleNodeStarts(time.Now())

	if got := hooks.liveNodeStarts(); got != 1 {
		t.Errorf("entries after the sweep = %d, want 1 (only the stale one goes)", got)
	}

	// The sweep is itself a diagnostic: reaching it means a node started and its
	// execution never reported an end, which is a defect somewhere upstream. A
	// silent sweep would turn that into invisible missing data.
	family := gatherMetricFamily(t, hooks.Metrics, "xflow_node_starts_swept_total")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("swept series = %d, want 1", len(family.GetMetric()))
	}
	if got := family.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("swept count = %v, want 1", got)
	}
}

// A node start can legitimately arrive after its execution ended: the start
// fires when a runner acquires the lease (engine/lease.go), the end fires from
// the commit path, and nothing orders the two. Recording that start would create
// an entry with no remaining path to release it — the original leak, one narrow
// window smaller.
func TestNodeStartAfterExecutionEndIsNotRecorded(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()
	id := types.ExecutionID("exec-raced")

	hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusFailed)
	hooks.OnNodeStart(ctx, id, "transform")

	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after a start that lost the race = %d, want 0", got)
	}
}

// The tombstones that make the test above work are themselves entries, so they
// need a bound of their own. It is a count, not an age: at trigger rates every
// message is an execution, and a time-based tombstone would hold hundreds of
// thousands of them.
func TestTombstonesAreBoundedByCount(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()

	for i := 0; i < endedTombstones*3; i++ {
		id := types.ExecutionID(fmt.Sprintf("exec-%d", i))
		hooks.OnNodeStart(ctx, id, "transform")
		hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusSuccess)
	}

	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after %d completed executions = %d, want 0",
			endedTombstones*3, got)
	}
	// Exactly the ring's capacity, not "at most it". Measured: after 3x
	// endedTombstones completed executions the ring settles on 4096. The upper
	// bound was equally green for a ring that keeps one tombstone or none --
	// residency would read perfect while the raced-start rejection covered only
	// the newest execution and the original leak returned for every other one.
	if got := hooks.trackedExecutions(); got != endedTombstones {
		t.Errorf("tracked executions = %d, want exactly %d; more means the ring "+
			"is not evicting and residency grows with traffic, fewer means it is "+
			"evicting tombstones a racing start still needs", got, endedTombstones)
	}
	// And the residency has to be of the RIGHT ends: the newest tombstone is the
	// one a racing start is most likely to hit, so a start for it must still be
	// rejected.
	newest := types.ExecutionID(fmt.Sprintf("exec-%d", endedTombstones*3-1))
	hooks.OnNodeStart(ctx, newest, "transform")
	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("a start racing the end of the most recently ended execution was "+
			"recorded (%d in flight); the ring evicted the tombstone that rejects it", got)
	}
}

// The release races the starts it releases: OnExecutionComplete runs on one
// goroutine while sibling branches of the same execution are still dispatching
// nodes. The interesting case is a start that loads an entry the release then
// removes — writing into it would leak silently, since nothing will ever read
// that map again. Run under -race; the residency assertion is what catches the
// leak the race detector cannot see.
func TestConcurrentStartsAndExecutionEndsLeaveNothingBehind(t *testing.T) {
	hooks := NewMetricsHooks(New())
	ctx := context.Background()

	const executions = 64
	var wg sync.WaitGroup
	for i := 0; i < executions; i++ {
		id := types.ExecutionID(fmt.Sprintf("exec-%d", i))
		wg.Add(3)
		go func() {
			defer wg.Done()
			for n := 0; n < 8; n++ {
				hooks.OnNodeStart(ctx, id, fmt.Sprintf("node-%d", n))
			}
		}()
		go func() {
			defer wg.Done()
			for n := 0; n < 8; n++ {
				hooks.OnNodeComplete(ctx, id, fmt.Sprintf("node-%d", n), types.NodeStatusSuccess)
			}
		}()
		go func() {
			defer wg.Done()
			hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusSuccess)
		}()
	}
	wg.Wait()

	// Every execution ended, so nothing is in flight regardless of how the
	// starts, completes and ends interleaved.
	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight entries after every execution ended = %d, want 0", got)
	}
}
