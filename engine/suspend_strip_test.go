package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// parkWithSpec submits a one-node workflow whose input carries params, leases
// the node and commits a first suspension with spec. It returns the lease, so a
// test can check the input the commit was given, and the output the backend
// stored for the parked node.
func parkWithSpec(t *testing.T, params map[string]any, spec *types.SuspendSpec) (*TaskLease, map[string]any) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "suspend-strip",
		Nodes: []types.NodeDef{{Name: "wait", Type: "test.suspend"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, params)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := eng.BuildTaskLease(ctx, queue.Drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{Suspend: spec}); err != nil {
		t.Fatal(err)
	}
	out, err := state.GetOutput(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	return lease, out
}

func TestFirstSuspendStoresTheWholeInputWithoutStripFromOutput(t *testing.T) {
	params := map[string]any{"approved": true, "amount": 42}
	_, out := parkWithSpec(t, params, &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"},
	})
	if !reflect.DeepEqual(out, params) {
		t.Fatalf("stored output = %#v, want the input unchanged %#v", out, params)
	}
}

func TestFirstSuspendDropsOnlyTheKeysStripFromOutputNames(t *testing.T) {
	params := map[string]any{"approved": true, "approver": "vp", "amount": 42, "reason": "laptop"}
	lease, out := parkWithSpec(t, params, &types.SuspendSpec{
		Mode:            types.ModeSignal,
		Signals:         []string{"go"},
		StripFromOutput: []string{"approved", "approver", "absent"},
	})
	want := map[string]any{"amount": 42, "reason": "laptop"}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("stored output = %#v, want %#v", out, want)
	}
	// The lease is what the commit was handed. Trimming the stored copy must not
	// reach back into it.
	if lease.Input.Data["approved"] != true || lease.Input.Data["approver"] != "vp" {
		t.Fatalf("lease input data = %#v, want the keys still present", lease.Input.Data)
	}
}

func TestFirstSuspendWithAnEmptyStripListKeepsTheInput(t *testing.T) {
	params := map[string]any{"approved": true}
	_, out := parkWithSpec(t, params, &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"}, StripFromOutput: []string{},
	})
	if !reflect.DeepEqual(out, params) {
		t.Fatalf("stored output = %#v, want %#v", out, params)
	}
}
