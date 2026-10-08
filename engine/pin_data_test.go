package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compilePinChain compiles fetch -> process with fetch pinned under mode.
func compilePinChain(t *testing.T, mode string, fetchDisabled bool) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "pin-chain",
		Settings: &types.WorkflowSettings{PinDataMode: mode},
		Nodes: []types.NodeDef{
			{Name: "fetch", Type: "test.echo", Disabled: fetchDisabled},
			{Name: "process", Type: "test.echo"},
		},
		Connections: types.Connections{
			"fetch": {"main": {Targets: []types.Connection{{Node: "process", Input: "main"}}}},
		},
		PinData: map[string]any{"fetch": map[string]any{"order_id": "ORD-001", "amount": 250.0}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// drainOne flushes the outbox and returns the single delivered task.
func drainOne(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, id types.ExecutionID) *Task {
	t.Helper()
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("delivery = %+v, want exactly one task", tasks)
	}
	return tasks[0]
}

// TestPinnedNode_ServesMockAndSchedulesDownstream drives a pinned root through
// the lease entry point every dispatcher uses and asserts: no lease is issued,
// the node is terminal pinned with the mock output, and the downstream node's
// lease input carries that mock exactly as it would a real upstream output.
func TestPinnedNode_ServesMockAndSchedulesDownstream(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compilePinChain(t, types.PinDataModeAlways, false), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	root := drainOne(t, ctx, eng, queue, id)
	if root.NodeName != "fetch" {
		t.Fatalf("root task = %q, want fetch", root.NodeName)
	}
	lease, err := eng.BuildTaskLease(ctx, root)
	if err != ErrSystemTaskHandled || lease != nil {
		t.Fatalf("BuildTaskLease(pinned) = %v, %v; want nil, ErrSystemTaskHandled", lease, err)
	}

	node, err := state.GetNode(ctx, id, "fetch")
	if err != nil || node == nil {
		t.Fatalf("GetNode(fetch) = %v, %v", node, err)
	}
	if node.Status != types.NodeStatusPinned || node.Port != "main" || node.LeaseToken != "" {
		t.Fatalf("fetch = status %q port %q lease %q; want pinned/main/no lease", node.Status, node.Port, node.LeaseToken)
	}
	want := map[string]any{"order_id": "ORD-001", "amount": 250.0}
	out, err := state.GetOutput(ctx, id, "fetch")
	if err != nil || !reflect.DeepEqual(out, want) {
		t.Fatalf("GetOutput(fetch) = %v, %v; want %v", out, err, want)
	}

	next := drainOne(t, ctx, eng, queue, id)
	if next.NodeName != "process" {
		t.Fatalf("downstream task = %q, want process", next.NodeName)
	}
	downLease, err := eng.BuildTaskLease(ctx, next)
	if err != nil || downLease == nil {
		t.Fatalf("BuildTaskLease(process) = %v, %v", downLease, err)
	}
	if !reflect.DeepEqual(downLease.Input.Data, want) {
		t.Fatalf("process input = %v, want the pinned mock %v", downLease.Input.Data, want)
	}

	// A redelivered pinned task resolves against the terminal node instead of
	// leasing it or committing a second time.
	if lease, err := eng.BuildTaskLease(ctx, root); err != ErrSystemTaskHandled || lease != nil {
		t.Fatalf("redelivered pinned task = %v, %v; want handled", lease, err)
	}

	if _, err := eng.CommitTaskResultWithOutcome(ctx, downLease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatalf("commit process: %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution = %+v, %v; want success (pinned counts as terminal)", snap, err)
	}
}

// TestPinnedNode_TestOnlyFollowsTheExecutionFlag pins the test_only gate: the
// same graph runs fetch for real in an ordinary execution and serves it from
// pin_data in one submitted with WithTestRun.
func TestPinnedNode_TestOnlyFollowsTheExecutionFlag(t *testing.T) {
	ctx := context.Background()
	g := compilePinChain(t, types.PinDataModeTestOnly, false)

	for _, tc := range []struct {
		name     string
		testRun  bool
		wantPins bool
	}{
		{name: "production", testRun: false, wantPins: false},
		{name: "test run", testRun: true, wantPins: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newFakeState()
			queue := &fakeQueue{}
			eng := New(state, queue)
			id, err := eng.Submit(WithTestRun(ctx, tc.testRun), g, nil)
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			snap, _ := state.GetExecution(ctx, id)
			if snap == nil || snap.TestRun != tc.testRun {
				t.Fatalf("snapshot TestRun = %+v, want %v", snap, tc.testRun)
			}
			lease, err := eng.BuildTaskLease(ctx, drainOne(t, ctx, eng, queue, id))
			if tc.wantPins {
				if err != ErrSystemTaskHandled {
					t.Fatalf("BuildTaskLease = %v, %v; want pinned", lease, err)
				}
				return
			}
			if err != nil || lease == nil || lease.Task.NodeName != "fetch" {
				t.Fatalf("BuildTaskLease = %v, %v; want a real lease for fetch", lease, err)
			}
		})
	}
}

// TestPinnedNode_DisabledModeAndDisabledNodeRunForReal pins both precedence
// rules: pin_data_mode=disabled and a disabled node each leave the node with no
// pin, so it is leased like any other node.
func TestPinnedNode_DisabledModeAndDisabledNodeRunForReal(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		mode     string
		disabled bool
	}{
		{name: "mode disabled", mode: types.PinDataModeDisabled},
		{name: "node disabled", mode: types.PinDataModeAlways, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := compilePinChain(t, tc.mode, tc.disabled)
			if g.PinDataMode() != "" {
				t.Fatalf("PinDataMode() = %q, want none", g.PinDataMode())
			}
			if _, ok := g.PinnedOutput(0); ok {
				t.Fatal("fetch carries a pin, want none")
			}
			state := newFakeState()
			queue := &fakeQueue{}
			eng := New(state, queue)
			id, err := eng.Submit(WithTestRun(ctx, true), g, nil)
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			lease, err := eng.BuildTaskLease(ctx, drainOne(t, ctx, eng, queue, id))
			if err != nil || lease == nil {
				t.Fatalf("BuildTaskLease = %v, %v; want a real lease", lease, err)
			}
		})
	}
}

// TestPinnedNode_InvokeEntryIsPinned covers the Invoke path, whose entry task
// carries no UnitIdx: the hook must derive the unit from the graph or the
// backend's marker fence would resolve the wrong unit.
func TestPinnedNode_InvokeEntryIsPinned(t *testing.T) {
	ctx := context.Background()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "pin-invoke",
		Settings: &types.WorkflowSettings{PinDataMode: types.PinDataModeAlways},
		Nodes: []types.NodeDef{
			{Name: "other", Type: "xflow.start"},
			{Name: "entry", Type: "xflow.start"},
			{Name: "after", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "after", Input: "main"}}}},
		},
		PinData: map[string]any{"entry": map[string]any{"seed": 1.0}},
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
	if node == nil || node.Status != types.NodeStatusPinned {
		t.Fatalf("entry = %+v, want pinned", node)
	}
	next := drainOne(t, ctx, eng, queue, id)
	if next.NodeName != "after" {
		t.Fatalf("downstream = %q, want after", next.NodeName)
	}
}
