package script_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestScript_NodeIdentityReachesTheEngine pins the seam that makes wasm eval
// cost attributable.
//
// xflow_wasm_eval_duration_seconds and xflow_wasm_eval_stdin_bytes carry only a
// namespace today, so a runner hosting several script nodes reports one merged
// series: "something in this namespace is burning CPU" — which is exactly the
// question the metric was added to answer, left unanswered. The node name is
// known at this layer and nowhere below it, so it has to travel from here.
//
// The assertion is on what the ENGINE sees, not on the context the node builds:
// building the identity correctly and failing to attach it to the context that
// actually reaches Execute is the plausible defect, and it is invisible from the
// node side.
func TestScript_NodeIdentityReachesTheEngine(t *testing.T) {
	captured := captureGlobals(t)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script("code-body").Language("js").Runtime(captureRuntime)
	input := &types.Input{
		Params:       b.RawParams().(map[string]any),
		WorkflowName: "sas-collect",
		NodeName:     "decode",
		Data:         map[string]any{"path": "/admin/users"},
	}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if captured.calls == 0 {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}

	want := engine.NodeIdentity{Workflow: "sas-collect", Node: "decode"}
	if captured.identity != want {
		t.Errorf("engine saw node identity %+v, want %+v; eval cost cannot be "+
			"attributed to a node without it", captured.identity, want)
	}
}

// TestScript_NodeIdentityIsAbsentWhenTheInputCarriesNone is the other half.
//
// The identity is only as good as its source: if the node layer invented a
// placeholder for an input that names no node, the metric would grow a label
// that looks authoritative and means nothing. An empty input must produce an
// empty identity, so the metric side can drop the labels rather than emit
// node="unknown" forever.
func TestScript_NodeIdentityIsAbsentWhenTheInputCarriesNone(t *testing.T) {
	captured := captureGlobals(t)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script("code-body").Language("js").Runtime(captureRuntime)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{},
	}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if captured.calls == 0 {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}
	if captured.identity != (engine.NodeIdentity{}) {
		t.Errorf("engine saw node identity %+v for an input naming no node; want the zero value",
			captured.identity)
	}
}
