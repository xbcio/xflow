package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestMerge_Factory(t *testing.T) {
	b := node.Merge(node.MergeWaitAll)
	if b.NodeType() != "xflow.merge" {
		t.Fatalf("expected xflow.merge, got %s", b.NodeType())
	}
}

func TestMerge_WaitAll(t *testing.T) {
	h, _ := registry.Lookup("xflow.merge")
	b := node.Merge(node.MergeWaitAll)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"from_main": "hello"},
		Inputs: map[string]any{"branch_a": "data_a", "branch_b": "data_b"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["branch_a"] != "data_a" {
		t.Fatalf("expected branch_a data, got %v", out.Data)
	}
	if out.Data["branch_b"] != "data_b" {
		t.Fatalf("expected branch_b data, got %v", out.Data)
	}
	if out.Data["from_main"] != "hello" {
		t.Fatalf("expected from_main data, got %v", out.Data)
	}
}

func TestMerge_WaitAny(t *testing.T) {
	h, _ := registry.Lookup("xflow.merge")
	b := node.Merge(node.MergeWaitAny)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"winner": true},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["winner"] != true {
		t.Fatalf("expected winner=true, got %v", out.Data)
	}
}

// TestMerge_WaitAnyMergesUpstreamInputs runs wait_any against the input shape a
// merge node is actually given.
//
// TestMerge_WaitAny above passes its payload in Data and leaves Inputs nil, but
// engine/input.go:122-145 fills Data only when a node has exactly one incoming
// edge; with two or more it fills Inputs instead, keyed by upstream node name,
// and leaves Data alone. A merge node is a fan-in node by definition, so on
// every real execution the branch payloads arrive exclusively through Inputs —
// the field no wait_any test sets. Deleting mergeAny's whole
// `if input.Inputs != nil` loop therefore leaves this file green while every
// wait_any merge emits an output with none of the winning branch's data in it,
// and the nodes after the merge see the fields they read simply missing.
//
// mergeAll's loop is covered (TestMerge_WaitAll sets Inputs), which is what
// makes the gap easy to miss: the same three lines exist twice, and only one
// copy is exercised.
func TestMerge_WaitAnyMergesUpstreamInputs(t *testing.T) {
	h, _ := registry.Lookup("xflow.merge")
	b := node.Merge(node.MergeWaitAny)
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		// The fan-in shape: no Data, payloads keyed by upstream node name.
		Inputs: map[string]any{
			"scan_branch":  map[string]any{"finding_count": 3.0},
			"audit_branch": map[string]any{"verdict": "clean"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, port := range []string{"scan_branch", "audit_branch"} {
		got, ok := out.Data[port].(map[string]any)
		if !ok {
			t.Fatalf("out.Data[%q] = %#v, want the upstream payload: wait_any "+
				"dropped the branch output on the only path it can arrive by",
				port, out.Data[port])
		}
		if len(got) == 0 {
			t.Errorf("out.Data[%q] is empty", port)
		}
	}
	if got := out.Data["scan_branch"].(map[string]any)["finding_count"]; got != 3.0 {
		t.Errorf("scan_branch.finding_count = %#v, want 3", got)
	}
}

func TestMerge_UnknownMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.merge")
	input := &types.Input{
		Params: map[string]any{"mode": "invalid"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
