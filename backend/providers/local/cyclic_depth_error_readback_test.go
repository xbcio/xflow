package local

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// depthLoopHandler unconditionally succeeds, so the only way this workflow can
// end is by tripping MaxAutoDepth.
type depthLoopHandler struct{}

func (depthLoopHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.depth_loop"}
}

func (depthLoopHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestCyclicDepthLimitReasonReachesWaitDone is the end-to-end half of the
// execution-level failure-reason readback: a real cyclic run that trips
// MaxAutoDepth, observed through the caller-facing surfaces.
//
// This scenario is the one where the execution-level reason is the ONLY carrier.
// The node that trips the limit finishes at SUCCESS — engine/scheduler.go rejects
// the downstream activation rather than failing the node — so every node's own
// Error is empty and a caller inspecting node state learns nothing about why the
// run failed.
//
// Both surfaces are asserted because they are separate code paths to the same
// snapshot field: WaitDone reads the snapshot directly (a fast path that bypasses
// Inspect entirely), while Inspect is what the SDK's polling fallback and the
// distributed backend go through. Wiring one and not the other leaves the reason
// visible in local-embedded mode and invisible in distributed mode, or vice versa.
func TestCyclicDepthLimitReasonReachesWaitDone(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.start", depthLoopHandler{})
	reg.RegisterGlobal("test.depth_loop", depthLoopHandler{})

	def := &types.WorkflowDef{
		Name: "cyclic-depth-error-readback",
		Nodes: []types.NodeDef{
			// A cyclic workflow requires exactly one xflow.start node.
			{Name: "start", Type: "xflow.start"},
			{Name: "loop", Type: "test.depth_loop"},
		},
		// start -> loop -> loop: a self-edge that can only be stopped by the
		// depth limit.
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "loop", Input: "main"}}}},
			"loop":  {"main": {Targets: []types.Connection{{Node: "loop", Input: "main"}}}},
		},
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 2},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if res.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %v, want failed (the depth limit must fail the execution)", res.Status)
	}
	if res.Error == "" {
		t.Fatal("Result.Error is empty on a depth-limit failure; the reason has no other carrier " +
			"because the node that tripped the limit finished at success")
	}
	if !strings.Contains(strings.ToLower(res.Error), "depth") {
		t.Errorf("Result.Error = %q, want it to mention the depth limit", res.Error)
	}

	// Inspect is the other readback path (SDK polling fallback, distributed
	// backend); it must agree with WaitDone rather than merely being non-empty.
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if detail.Error != res.Error {
		t.Errorf("ExecutionDetail.Error = %q, WaitDone Result.Error = %q; the two readback paths disagree",
			detail.Error, res.Error)
	}

	// The premise of the whole test: no node carries the reason. If a future
	// change starts failing the tripping node, this assertion flips and the
	// scenario above stops being the sole-carrier case it is written for.
	for _, n := range detail.Nodes {
		if n.Error != "" {
			t.Errorf("node %q carries error %q; this test assumes no node does, "+
				"which is what makes the execution-level reason the only carrier", n.Name, n.Error)
		}
	}
}
