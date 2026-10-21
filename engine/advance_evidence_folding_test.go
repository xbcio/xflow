package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileTwoLeafGraph compiles start -> {left, right}, both leaves. Committing
// left while right is still pending is what makes it a leaf commit that does
// not finalize the execution: the case where the folded transition applies with
// no arrivals, so its OutboxIDs hint is empty and only Applied can tell the
// evidence buffer that an advance happened.
func compileTwoLeafGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "folded-leaf-evidence",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "left", Type: "test.echo"},
			{Name: "right", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{
				{Node: "left", Input: "main"},
				{Node: "right", Input: "main"},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// compileSkipLeafGraph compiles start -> split, split.main -> a, split.other ->
// b where b is a leaf. Committing split on main strands b, whose folded skip
// commit then has no downstream work of its own.
func compileSkipLeafGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "folded-skip-leaf-evidence",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "split", Type: "test.echo"},
			{Name: "a", Type: "test.echo"},
			{Name: "b", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "split", Input: "main"}}}},
			"split": {
				"main":  {Targets: []types.Connection{{Node: "a", Input: "main"}}},
				"other": {Targets: []types.Connection{{Node: "b", Input: "main"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// compileLoneRootPair compiles a pinned-or-disabled root with no out-edges next
// to an independent ordinary root, so the hook's commit stays non-finalizing
// while contributing no downstream work of its own.
func compileLoneRootPair(t *testing.T, pinned, disabled bool) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "folded-hook-leaf-evidence",
		Nodes: []types.NodeDef{
			{Name: "fetch", Type: "test.echo", Disabled: disabled},
			{Name: "other", Type: "test.echo"},
		},
	}
	if pinned {
		def.Settings = &types.WorkflowSettings{PinDataMode: types.PinDataModeAlways}
		def.PinData = map[string]any{"fetch": map[string]any{"seed": 1.0}}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// submitAndLeaseRoot submits g, flushes its initial intents and returns the
// lease for the named root. The remaining roots stay undelivered on the queue.
func submitAndLeaseRoot(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, g *graph.Graph, node string) (*TaskLease, types.ExecutionID) {
	t.Helper()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	var root *Task
	for _, task := range queue.Drain() {
		if task.NodeName == node {
			root = task
		}
	}
	if root == nil {
		t.Fatalf("no root delivery for %q", node)
	}
	lease, err := eng.BuildTaskLease(ctx, root)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease(%s) = %v, %v", node, lease, err)
	}
	return lease, id
}

// leaseDelivered leases the one delivered task for node from the last commit's
// flush.
func leaseDelivered(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, node string) *TaskLease {
	t.Helper()
	var found *Task
	for _, task := range queue.Drain() {
		if task.NodeName == node {
			found = task
		}
	}
	if found == nil {
		t.Fatalf("no delivery for %q", node)
	}
	lease, err := eng.BuildTaskLease(ctx, found)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease(%s) = %v, %v", node, lease, err)
	}
	return lease
}

// assertSingleAppliedLeafAdvance requires exactly one advance receipt for node
// with Applied true and an empty candidate hint.
func assertSingleAppliedLeafAdvance(t *testing.T, events []RuntimeEvidenceEvent, node string) {
	t.Helper()
	var advances []RuntimeEvidenceEvent
	for _, ev := range events {
		if ev.Type == RuntimeEvidenceAdvance && ev.NodeName == node {
			advances = append(advances, ev)
		}
	}
	if len(advances) != 1 {
		t.Fatalf("advance receipts for %q = %d, want exactly 1: the folded commit "+
			"is the advance, and a fallback advance task would be a duplicate", node, len(advances))
	}
	got := advances[0]
	if !got.Applied {
		t.Fatalf("advance receipt for %q has Applied=false, want true", node)
	}
	if len(got.OutboxIDs) != 0 {
		t.Fatalf("advance receipt for %q candidates = %v, want empty (a leaf schedules nothing)",
			node, got.OutboxIDs)
	}
}

// TestFoldedLeafAdvanceEvidence pins the evidence parity for the folded path: a
// leaf commit applies with zero arrivals and an empty OutboxIDs, and must still
// be observable as an applied advance, exactly as the legacy advance-task path
// reports it from AdvanceNode.
func TestFoldedLeafAdvanceEvidence(t *testing.T) {
	ctx := context.Background()
	buf := NewRuntimeEvidenceBuffer(32)
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithRuntimeEvidenceBuffer(buf))

	lease, id := submitAndLeaseRoot(t, ctx, eng, queue, compileTwoLeafGraph(t), "start")
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "start"}},
	}); err != nil {
		t.Fatalf("commit start error = %v", err)
	}
	leftLease := leaseDelivered(t, ctx, eng, queue, "left")
	drainEvidence(buf) // start's receipts are not this test's subject

	if _, err := eng.CommitTaskResultWithOutcome(ctx, leftLease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "left"}},
	}); err != nil {
		t.Fatalf("commit left error = %v", err)
	}
	// The execution must still be running: a finalizing commit suppresses the
	// advance intent by design, and then there is no legacy receipt to mirror.
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution after left commit = %+v, %v; want running", snap, err)
	}
	assertSingleAppliedLeafAdvance(t, drainEvidence(buf), "left")
}

// TestFoldedSkipLeafAdvanceEvidence covers the same gate on the skip cascade: a
// skipped leaf's folded commit applies with no downstream work of its own.
func TestFoldedSkipLeafAdvanceEvidence(t *testing.T) {
	ctx := context.Background()
	buf := NewRuntimeEvidenceBuffer(32)
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithRuntimeEvidenceBuffer(buf))

	id, err := eng.Submit(ctx, compileSkipLeafGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	root := queue.Drain()
	if len(root) != 1 || root[0].NodeName != "start" {
		t.Fatalf("root delivery = %+v, want one task for start", root)
	}
	startLease, err := eng.BuildTaskLease(ctx, root[0])
	if err != nil || startLease == nil {
		t.Fatalf("BuildTaskLease(start) = %v, %v", startLease, err)
	}
	if _, err := eng.CommitTaskResultWithOutcome(ctx, startLease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "start"}},
	}); err != nil {
		t.Fatalf("commit start error = %v", err)
	}
	drainEvidence(buf)

	// split commits on main: a is scheduled, b is stranded and its folded skip
	// commit is consumed by split's own flush.
	commitOn(t, ctx, eng, queue, id, "split", "main")

	node, err := state.GetNode(ctx, id, "b")
	if err != nil || node == nil || node.Status != types.NodeStatusSkipped {
		t.Fatalf("b = %+v, %v; want skipped", node, err)
	}
	assertSingleAppliedLeafAdvance(t, drainEvidence(buf), "b")
}

// TestFoldedHookLeafPublishesAdvanceReceipt covers the receipt each hook owes a
// folded commit, on a leaf that stays active because another root is pending.
// The legacy path publishes it from the advance task the hook used to emit, so
// the folded path must not silently drop it.
func TestFoldedHookLeafPublishesAdvanceReceipt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pinned   bool
		disabled bool
	}{
		{name: "pinned", pinned: true},
		{name: "disabled", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			buf := NewRuntimeEvidenceBuffer(16)
			state := newFakeState()
			queue := &fakeQueue{}
			eng := New(state, queue, WithRuntimeEvidenceBuffer(buf))

			g := compileLoneRootPair(t, tc.pinned, tc.disabled)
			id, err := eng.Submit(ctx, g, nil)
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			if err := eng.FlushOutbox(ctx, id); err != nil {
				t.Fatalf("FlushOutbox() after submit error = %v", err)
			}
			var fetchTask *Task
			for _, task := range queue.Drain() {
				if task.NodeName == "fetch" {
					fetchTask = task
				}
			}
			if fetchTask == nil {
				t.Fatal("no root delivery for fetch")
			}
			if _, err := eng.BuildTaskLease(ctx, fetchTask); err != ErrSystemTaskHandled {
				t.Fatalf("BuildTaskLease(fetch) err = %v, want ErrSystemTaskHandled", err)
			}

			snap, err := state.GetExecution(ctx, id)
			if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
				t.Fatalf("execution after the hook commit = %+v, %v; want running", snap, err)
			}
			assertSingleAppliedLeafAdvance(t, drainEvidence(buf), "fetch")
		})
	}
}

// TestFinalizingFoldedCommitPublishesNoAdvanceReceipt pins the other half of
// the gate. A commit that finalizes the execution cannot schedule anything, so
// the backends suppress its advance intent and the legacy path has no
// AdvanceNode receipt to mirror; emitting one here would report an advance that
// did not happen.
func TestFinalizingFoldedCommitPublishesNoAdvanceReceipt(t *testing.T) {
	ctx := context.Background()
	buf := NewRuntimeEvidenceBuffer(16)
	queue := &fakeQueue{}
	eng := New(newFakeState(), queue, WithRuntimeEvidenceBuffer(buf))

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "finalizing-folded-commit",
		Nodes: []types.NodeDef{{Name: "solo", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	lease, _ := submitAndLeaseRoot(t, ctx, eng, queue, g, "solo")
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "solo"}},
	}); err != nil {
		t.Fatalf("commit solo error = %v", err)
	}

	events := drainEvidence(buf)
	var commits int
	for _, ev := range events {
		switch ev.Type {
		case RuntimeEvidenceCommit:
			if ev.Applied {
				commits++
			}
		case RuntimeEvidenceAdvance:
			t.Fatalf("finalizing folded commit published an advance receipt: %+v", ev)
		}
	}
	if commits != 1 {
		t.Fatalf("applied commit receipts = %d, want 1", commits)
	}
}

// TestNonFoldingLeafPublishesAdvanceReceiptOnce is the no-duplicate guard for
// the fallback: the commit never publishes the receipt itself, so the single
// one AdvanceNode produces is the only one.
func TestNonFoldingLeafPublishesAdvanceReceiptOnce(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	buf := NewRuntimeEvidenceBuffer(32)
	state := &nonFoldingCommitRecorder{StateStore: inner, AtomicStateStore: inner, log: &commitRequestLog{}}
	queue := &fakeQueue{}
	eng := New(state, queue, WithRuntimeEvidenceBuffer(buf))

	lease, _ := submitAndLeaseRoot(t, ctx, eng, queue, compileTwoLeafGraph(t), "start")
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "start"}},
	}); err != nil {
		t.Fatalf("commit start error = %v", err)
	}
	leftLease := leaseDelivered(t, ctx, eng, queue, "left")
	drainEvidence(buf)

	if _, err := eng.CommitTaskResultWithOutcome(ctx, leftLease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": "left"}},
	}); err != nil {
		t.Fatalf("commit left error = %v", err)
	}
	assertSingleAppliedLeafAdvance(t, drainEvidence(buf), "left")
}
