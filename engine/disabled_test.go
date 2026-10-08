package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileDisabledChain compiles fetch -> process with fetch disabled.
func compileDisabledChain(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "disabled-chain",
		Nodes: []types.NodeDef{
			{Name: "fetch", Type: "test.echo", Disabled: true},
			{Name: "process", Type: "test.echo"},
		},
		Connections: types.Connections{
			"fetch": {"main": {Targets: []types.Connection{{Node: "process", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestDisabledNode_CommitsSkippedAndSchedulesDownstream drives a disabled root
// through the lease entry point every dispatcher uses and asserts the §3.1
// contract end to end: no lease is issued, the node is terminal skipped with
// no output, and the downstream node still runs -- a disabled node is a
// satisfied dependency, not a skip cascade.
func TestDisabledNode_CommitsSkippedAndSchedulesDownstream(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileDisabledChain(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	root := drainOne(t, ctx, eng, queue, id)
	if root.NodeName != "fetch" {
		t.Fatalf("root task = %q, want fetch", root.NodeName)
	}
	lease, err := eng.BuildTaskLease(ctx, root)
	if err != ErrSystemTaskHandled || lease != nil {
		t.Fatalf("BuildTaskLease(disabled) = %v, %v; want nil, ErrSystemTaskHandled", lease, err)
	}

	node, err := state.GetNode(ctx, id, "fetch")
	if err != nil || node == nil {
		t.Fatalf("GetNode(fetch) = %v, %v", node, err)
	}
	if node.Status != types.NodeStatusSkipped || node.Port != "main" || node.LeaseToken != "" {
		t.Fatalf("fetch = status %q port %q lease %q; want skipped/main/no lease", node.Status, node.Port, node.LeaseToken)
	}
	// No output: downstream must read nil, which is what "empty" means for the
	// input builder.
	if out, err := state.GetOutput(ctx, id, "fetch"); err != nil || len(out) != 0 {
		t.Fatalf("GetOutput(fetch) = %v, %v; want no output", out, err)
	}

	// The downstream node is NOT skipped: the "main" port on the skipped commit
	// keeps it scheduled, and it reads nil for the disabled upstream.
	next := drainOne(t, ctx, eng, queue, id)
	if next.NodeName != "process" {
		t.Fatalf("downstream task = %q, want process", next.NodeName)
	}
	downLease, err := eng.BuildTaskLease(ctx, next)
	if err != nil || downLease == nil {
		t.Fatalf("BuildTaskLease(process) = %v, %v; want a real lease", downLease, err)
	}
	if len(downLease.Input.Data) != 0 {
		t.Fatalf("process input = %v, want empty (the disabled upstream contributes nil)", downLease.Input.Data)
	}

	// A redelivered disabled task resolves against the terminal node instead of
	// committing a second time.
	if lease, err := eng.BuildTaskLease(ctx, root); err != ErrSystemTaskHandled || lease != nil {
		t.Fatalf("redelivered disabled task = %v, %v; want handled", lease, err)
	}

	if _, err := eng.CommitTaskResultWithOutcome(ctx, downLease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatalf("commit process: %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution = %+v, %v; want success (skipped counts as terminal)", snap, err)
	}
}

// TestDisabledNode_InvokeEntryIsSkipped covers the Invoke path, whose entry
// task carries no UnitIdx: the hook must derive the unit from the graph.
func TestDisabledNode_InvokeEntryIsSkipped(t *testing.T) {
	ctx := context.Background()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "disabled-invoke",
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "xflow.start", Disabled: true},
			{Name: "after", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "after", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	id, err := eng.Invoke(ctx, g, "entry", nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	var entry *Task
	for _, task := range queue.Drain() {
		if task.NodeName == "entry" {
			entry = task
		}
	}
	if entry == nil {
		t.Fatal("no delivery for entry")
	}
	if _, err := eng.BuildTaskLease(ctx, entry); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(entry) err = %v, want ErrSystemTaskHandled", err)
	}
	node, _ := state.GetNode(ctx, id, "entry")
	if node == nil || node.Status != types.NodeStatusSkipped {
		t.Fatalf("entry = %+v, want skipped", node)
	}
	next := drainOne(t, ctx, eng, queue, id)
	if next.NodeName != "after" {
		t.Fatalf("downstream = %q, want after", next.NodeName)
	}
}

// TestDisabledNode_BeatsPinData pins the precedence rule on the disabled side:
// a node that is both disabled and pinned is served as skipped with no output
// -- the pin never takes effect, and the graph carries no pin mode at all.
func TestDisabledNode_BeatsPinData(t *testing.T) {
	ctx := context.Background()
	g := compilePinChain(t, types.PinDataModeAlways, true)
	if got := g.PinDataMode(); got != "" {
		t.Fatalf("PinDataMode() = %q, want none (the only pinned node is disabled)", got)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if _, err := eng.BuildTaskLease(ctx, drainOne(t, ctx, eng, queue, id)); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(disabled+pinned) err = %v, want ErrSystemTaskHandled", err)
	}
	node, _ := state.GetNode(ctx, id, "fetch")
	if node == nil || node.Status != types.NodeStatusSkipped || node.Port != "main" {
		t.Fatalf("fetch = %+v, want skipped/main (not pinned)", node)
	}
	if out, _ := state.GetOutput(ctx, id, "fetch"); len(out) != 0 {
		t.Fatalf("GetOutput(fetch) = %v, want no output: the pin must not be served", out)
	}
	next := drainOne(t, ctx, eng, queue, id)
	if next.NodeName != "process" {
		t.Fatalf("downstream = %q, want process", next.NodeName)
	}
}
