package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileSkipDiamondGraph builds the shape a skip actually takes in production:
// a node with two output ports, each leading to a different downstream node,
// with those two branches reconverging.
//
// The two-port source is deliberately NOT the entry node. That distinction is
// load-bearing, and the first version of this fixture got it wrong: with the
// fan-out node at the entry, the very first commit already strands a branch, so
// a test meaning to exercise "the execute path" was silently exercising the
// skip path instead. Putting an ordinary single-port node in front makes the
// first transition genuinely all-execute.
//
// Reconvergence is what makes a skip possible at all. Two independent
// destinations of a fan-out are each scheduled by the port the source commits
// on; nothing is left unresolved. A skip requires an in-edge that will never
// carry data, and that only exists where branches join.
func compileSkipDiamondGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "advance-skip-diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "split", Type: "test.echo"},
			{Name: "on_main", Type: "test.echo"},
			{Name: "on_other", Type: "test.echo"},
			{Name: "join", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "split", Input: "main"}}}},
			"split": {
				"main":  {Targets: []types.Connection{{Node: "on_main", Input: "main"}}},
				"other": {Targets: []types.Connection{{Node: "on_other", Input: "main"}}},
			},
			"on_main":  {"main": {Targets: []types.Connection{{Node: "join", Input: "left"}}}},
			"on_other": {"main": {Targets: []types.Connection{{Node: "join", Input: "right"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// startSkipDiamond submits the fixture and leases its root task.
func startSkipDiamond(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue) (types.ExecutionID, *TaskLease) {
	t.Helper()
	id, err := eng.Submit(ctx, compileSkipDiamondGraph(t), map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("root delivery = %+v, want one task for start", tasks)
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	return id, lease
}

// commitOn flushes what the last commit scheduled, asserts the delivery is
// exactly the named node, and commits it on port. Failing on an unexpected
// delivery is what keeps a fixture that stops routing as expected from
// degrading silently into a test of something else.
func commitOn(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, id types.ExecutionID, node, port string) {
	t.Helper()
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != node {
		t.Fatalf("delivery = %+v, want exactly one task for %q", tasks, node)
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease(%s) = %v, %v", node, lease, err)
	}
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: port, Data: map[string]any{"out": node}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome(%s) error = %v", node, err)
	}
}

// TestNotifySkip_AdvanceReportsTheSkippedNode drives real commits and advances
// through the skip diamond and asserts the observer sees the skipped
// destination, with the flow label that says an ordinary advance decided it.
//
// The skip lands on on_other, and that is the correct answer rather than a
// compromise. split commits on "main", so the advance of split is the
// transition that resolves its "other" destination with no active port. join
// still has the on_other in-edge outstanding, so nothing has been decided about
// it yet and it is not scheduled at all.
//
// The node assertion is what the count cannot make: a count of one would also
// hold for an advance that reported the wrong node, or one that reported a skip
// per completed node regardless of what it scheduled.
func TestNotifySkip_AdvanceReportsTheSkippedNode(t *testing.T) {
	ctx := context.Background()
	obs := &recordingSkipObserver{}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithNodeSkipObserver(obs))

	id, lease := startSkipDiamond(t, ctx, eng, queue)

	// start has exactly one out-edge and is active, so its destination has
	// in-degree 1 with one active input. Nothing can be skipped here.
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "main", Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome(start) error = %v", err)
	}
	// snapshot, not take: the test is only part-way through and freezing the
	// double here would silently drop the skip it is about to assert on.
	if got := obs.snapshot(); len(got) != 0 {
		t.Fatalf("skips after an all-execute advance = %+v, want none", got)
	}

	// split commits on "main", stranding its "other" destination.
	commitOn(t, ctx, eng, queue, id, "split", "main")

	got := obs.take()
	if len(got) != 1 {
		t.Fatalf("skips = %+v, want exactly one for the stranded on_other branch", got)
	}
	if got[0].node != "on_other" {
		t.Fatalf("skip node = %q, want %q", got[0].node, "on_other")
	}
	if got[0].flow != flowAdvance {
		t.Fatalf("skip flow = %q, want %q", got[0].flow, flowAdvance)
	}
	if got[0].count != 1 {
		t.Fatalf("skip count = %d, want 1", got[0].count)
	}
}

// TestNotifySkip_ExecutePathReportsNothing is the negative control for the
// counter's entire reason to exist: an advance that schedules every downstream
// unit for execution must produce no observation at all. A skip metric that
// also moves on the execute path cannot be alerted on, because it would be
// indistinguishable from ordinary traffic.
func TestNotifySkip_ExecutePathReportsNothing(t *testing.T) {
	ctx := context.Background()
	obs := &recordingSkipObserver{}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithNodeSkipObserver(obs))

	id, lease := startSkipDiamond(t, ctx, eng, queue)
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "main", Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}

	if got := obs.take(); len(got) != 0 {
		t.Fatalf("skips = %+v, want none: every destination of this advance was "+
			"scheduled for execution", got)
	}
	// Teeth. A zero skip count is also what an advance that never ran would
	// report, so the transition itself has to be shown to have happened: the
	// advance marker start's advance wrote is the durable trace it leaves.
	marker := "advance/" + string(id) + "/start/0"
	if !state.atomicAdvanced[marker] {
		t.Fatalf("advance marker %q absent (advanced=%v), so this fixture never "+
			"reached an advance and a zero skip count proves nothing",
			marker, state.atomicAdvanced)
	}
}

// TestNotifySkip_NilObserverDoesNotPanic covers the deployment shape the
// observer has to tolerate: an engine with no NodeSkipObserver at all. Every
// production engine that predates this wiring is in that state, and a skip must
// stay a silent no-op for it rather than panicking on the scheduling path.
func TestNotifySkip_NilObserverDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue) // no WithNodeSkipObserver

	id, lease := startSkipDiamond(t, ctx, eng, queue)
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "main", Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	// Reach the notifying branch: split stranding its other port.
	commitOn(t, ctx, eng, queue, id, "split", "main")
}
