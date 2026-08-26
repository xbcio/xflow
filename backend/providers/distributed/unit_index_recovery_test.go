package distributed

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// bindHandler wraps the caller's task handler in a graph-aware unit-index
// resolver (backend.go:661-679). It is the single owner of that recovery: the
// queue codec cannot reach a StateStore, so it decodes a pre-group durable
// payload with UnitIdx == engine.UnitIdxUnknown and leaves the real resolution
// to this wrapper.
//
// Nothing exercised it. Every UnitIdxUnknown assertion in the repo lives in the
// codec layer — internal/queue/codec_test.go and internal/rstate's outbox
// round trip — which prove the sentinel survives serialisation and stop there.
// No test ever drove a task carrying the sentinel through a bound handler, so
// the whole branch, including the TaskTypeGroupExec fail-closed check and the
// node-index range check, was dead from the suite's point of view.
//
// The comment on that branch names the exact bug it is avoiding: it must never
// assume UnitIdx == NodeIdx, because that only holds for graphs with no groups.
// buildUnits places ungrouped nodes before group units, so in a mixed graph the
// two numbers diverge — and a wrong unit index does not fail, it leases and
// commits the wrong unit.

// captureTransport records the handler the backend binds so a test can deliver
// tasks through the same wrapper a broker delivery would go through. The stub
// in transport_pluggable_test.go drops the handler on the floor, which is why
// these tests need their own.
type captureTransport struct {
	mu      sync.Mutex
	handler queue.TaskHandler
}

func (c *captureTransport) Enqueue(context.Context, *engine.Task) error { return nil }
func (c *captureTransport) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return nil
}

func (c *captureTransport) StartConsumer(_ queue.ConsumerConfig, h queue.TaskHandler) (func(), error) {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
	return func() {}, nil
}

func (c *captureTransport) Close() error { return nil }

func (c *captureTransport) deliver(t *testing.T, task *engine.Task) error {
	t.Helper()
	c.mu.Lock()
	h := c.handler
	c.mu.Unlock()
	if h == nil {
		t.Fatal("transport never received a handler: bind did not start the consumer")
	}
	return h(context.Background(), task)
}

// mixedUnitGraph has one group and one ungrouped node, which is the only shape
// where the resolver's answer differs from the shortcut it must not take.
func mixedUnitGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "mixed-units",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// bindCapture builds a backend over miniredis whose bound handler records every
// task it is handed, and returns the transport plus that record.
func bindCapture(t *testing.T) (*captureTransport, *[]engine.Task, *Backend) {
	t.Helper()
	tr := &captureTransport{}
	b := newTestBackend(t, tr)
	var seen []engine.Task
	stop, err := b.BindTaskHandler(engine.New(b.State(), b.Queue()),
		func(_ context.Context, task *engine.Task) error {
			seen = append(seen, *task)
			return nil
		})
	if err != nil {
		t.Fatalf("BindTaskHandler() error = %v", err)
	}
	t.Cleanup(stop)
	return tr, &seen, b
}

func seedGraph(t *testing.T, b *Backend, id types.ExecutionID, g *graph.Graph) {
	t.Helper()
	// The resolver calls LoadGraph with context.Background(), so the graph has
	// to be reachable from the default namespace for the lookup to find it.
	if err := b.State().CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
}

func TestBindResolvesUnknownUnitIdxThroughTheGraph(t *testing.T) {
	g := mixedUnitGraph(t)
	storeIdx, ok := g.NodeIndex("store")
	if !ok {
		t.Fatal("store node missing from the fixture")
	}
	wantUnit := g.UnitIndexForNode(storeIdx)

	// The fixture only has teeth if the two numbers actually disagree. A graph
	// where every node is its own unit makes "resolved through the graph" and
	// "copied the node index" produce the same answer, and this test would pass
	// for a resolver that does the thing its own comment forbids.
	if wantUnit == storeIdx {
		t.Fatalf("unit index for %q equals its node index (%d): the fixture "+
			"cannot distinguish a graph lookup from UnitIdx = NodeIdx", "store", storeIdx)
	}
	if wantUnit < 0 {
		t.Fatalf("unit index for %q is %d: the fixture node is not schedulable", "store", wantUnit)
	}

	tr, seen, b := bindCapture(t)
	id := types.ExecutionID("exec-mixed-units")
	seedGraph(t, b, id, g)

	if err := tr.deliver(t, &engine.Task{
		ExecutionID: id,
		NodeName:    "store",
		NodeIdx:     storeIdx,
		Type:        engine.TaskTypeNodeExec,
		UnitIdx:     engine.UnitIdxUnknown,
	}); err != nil {
		t.Fatalf("deliver() error = %v", err)
	}

	if len(*seen) != 1 {
		t.Fatalf("handler saw %d tasks, want 1", len(*seen))
	}
	if got := (*seen)[0].UnitIdx; got != wantUnit {
		t.Fatalf("handler received UnitIdx = %d, want %d (node index is %d): a "+
			"pre-group durable payload resolved to the wrong durable unit, so the "+
			"task leases and commits against a unit that belongs to another node",
			got, wantUnit, storeIdx)
	}
}

func TestBindLeavesAKnownUnitIdxAloneWithoutLoadingTheGraph(t *testing.T) {
	// The negative control for the test above, and the only thing that pins
	// "resolve *only* when the sentinel is present". No graph is seeded, so a
	// resolver that loads unconditionally fails with "no graph available"
	// rather than quietly doing redundant work — which is also the second
	// assertion: an extra LoadGraph per task is one Redis round trip on the
	// hot delivery path.
	g := mixedUnitGraph(t)
	storeIdx, _ := g.NodeIndex("store")
	wantUnit := g.UnitIndexForNode(storeIdx)

	tr, seen, _ := bindCapture(t)
	if err := tr.deliver(t, &engine.Task{
		ExecutionID: "exec-never-created",
		NodeName:    "store",
		NodeIdx:     storeIdx,
		Type:        engine.TaskTypeNodeExec,
		UnitIdx:     wantUnit,
	}); err != nil {
		t.Fatalf("deliver() error = %v: a task that already carries its durable "+
			"unit identity must not be sent through graph recovery", err)
	}
	if len(*seen) != 1 {
		t.Fatalf("handler saw %d tasks, want 1", len(*seen))
	}
	if got := (*seen)[0].UnitIdx; got != wantUnit {
		t.Fatalf("handler received UnitIdx = %d, want the delivered %d unchanged", got, wantUnit)
	}
}

func TestBindFailsClosedForAGroupTaskMissingItsUnitIdentity(t *testing.T) {
	g := mixedUnitGraph(t)
	tr, seen, b := bindCapture(t)
	id := types.ExecutionID("exec-group-stale")
	// The graph IS seeded here: the point is that a group task fails closed
	// even when recovery would have succeeded. Without the graph, "refused" and
	// "tried and could not load" are the same observation.
	seedGraph(t, b, id, g)

	entryIdx, ok := g.NodeIndex("ingest")
	if !ok {
		t.Fatal("ingest node missing from the fixture")
	}
	err := tr.deliver(t, &engine.Task{
		ExecutionID: id,
		NodeName:    "ingest",
		NodeIdx:     entryIdx,
		Type:        engine.TaskTypeGroupExec,
		UnitIdx:     engine.UnitIdxUnknown,
	})
	if err == nil {
		t.Fatal("deliver() error = nil for a group task with no durable unit " +
			"identity: a stale group payload was accepted, and the unit it runs " +
			"against was inferred rather than carried")
	}
	// Refusing must mean not running. An error returned *after* the handler ran
	// still executed the group against a guessed unit.
	if len(*seen) != 0 {
		t.Fatalf("handler ran %d times for a task that was supposed to fail "+
			"closed: the refusal happened after the work, not instead of it", len(*seen))
	}
}

func TestBindRejectsAnUnresolvableUnknownUnitIdx(t *testing.T) {
	g := mixedUnitGraph(t)
	storeIdx, _ := g.NodeIndex("store")

	for _, tc := range []struct {
		name    string
		seed    bool
		nodeIdx int
	}{
		// No execution created, so LoadGraph finds nothing. Accepting this
		// would let the task through with UnitIdx still -1, which is not a unit.
		{"no graph", false, storeIdx},
		// A node index past the end of the graph. UnitIndexForNode is indexed
		// by node, so without the range check this reads out of bounds or
		// returns a unit belonging to nothing.
		{"node index past the end", true, g.NodeCount()},
		{"negative node index", true, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr, seen, b := bindCapture(t)
			id := types.ExecutionID("exec-" + tc.name)
			if tc.seed {
				seedGraph(t, b, id, g)
			}
			err := tr.deliver(t, &engine.Task{
				ExecutionID: id,
				NodeName:    "store",
				NodeIdx:     tc.nodeIdx,
				Type:        engine.TaskTypeNodeExec,
				UnitIdx:     engine.UnitIdxUnknown,
			})
			if err == nil {
				t.Fatal("deliver() error = nil: the unit index could not be " +
					"resolved and the task was handed to the handler anyway")
			}
			if len(*seen) != 0 {
				t.Fatalf("handler ran %d times despite an unresolvable unit index", len(*seen))
			}
		})
	}
}
