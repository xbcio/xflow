package graph_test

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// hiddenMutableStruct reproduces the C2 alias probe from the 2026-07-19
// acceptance: an otherwise innocuous struct that smuggles mutable references
// in unexported fields. Before the fix, the validator skipped unexported fields
// and the cloner shallow-copied them, so the compiled Graph aliased the
// caller-owned maps/slices/pointers.
type hiddenMutableStruct struct {
	Public string
	hidden map[string]any
	items  []string
}

func hiddenMutableValue() hiddenMutableStruct {
	return hiddenMutableStruct{
		Public: "visible",
		hidden: map[string]any{"key": "original"},
		items:  []string{"a", "b"},
	}
}

// baseUnexportedDef returns a minimal valid workflow; callers mutate the
// Parameters/Vars/Config payload to inject the probe value.
func baseUnexportedDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "unexported-alias-probe",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "worker", Type: "test.worker"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "worker", Input: "main"}}},
		},
	}
}

// TestCompileRejectsUnexportedStructFields proves that ANY struct containing an
// unexported field is rejected at Compile time, regardless of whether that
// field holds a mutable reference. This is the fail-closed value-domain gate
// that prevents hidden aliases from entering the Graph.
func TestCompileRejectsUnexportedStructFields(t *testing.T) {
	probe := hiddenMutableValue()

	cases := []struct {
		name   string
		field  string
		mutate func(*types.WorkflowDef)
	}{
		{
			name:  "unexported map in Parameters",
			field: "hidden",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"probe": probe}
			},
		},
		{
			name:  "unexported slice in Parameters",
			field: "items",
			mutate: func(d *types.WorkflowDef) {
				// Use a struct that only has the slice field unexported.
				d.Nodes[1].Parameters = map[string]any{"probe": struct {
					Public string
					items  []string
				}{Public: "x", items: []string{"a"}}}
			},
		},
		{
			name:  "unexported pointer in Parameters",
			field: "next",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"probe": struct {
					Public string
					next   *int
				}{Public: "x", next: new(int)}}
			},
		},
		{
			name:  "unexported field in Vars",
			field: "hidden",
			mutate: func(d *types.WorkflowDef) {
				d.Context = &types.WorkflowContext{Vars: map[string]any{"probe": probe}}
			},
		},
		{
			name:  "unexported field in Config",
			field: "hidden",
			mutate: func(d *types.WorkflowDef) {
				d.Context = &types.WorkflowContext{Config: map[string]any{"probe": probe}}
			},
		},
		{
			name:  "nested unexported field inside slice",
			field: "hidden",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"list": []any{probe}}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := baseUnexportedDef()
			tc.mutate(def)
			_, err := graph.Compile(def)
			if err == nil {
				t.Fatal("Compile succeeded, want value-domain rejection for unexported field")
			}
			msg := err.Error()
			if !strings.Contains(msg, "value domain") {
				t.Fatalf("error = %v, want a value-domain error", err)
			}
			if !strings.Contains(msg, tc.field) {
				t.Fatalf("error = %v, want field path containing %q", err, tc.field)
			}
			if !strings.Contains(msg, "unexported") {
				t.Fatalf("error = %v, want 'unexported' in message", err)
			}
		})
	}
}

// TestCompileRejectsUnexportedFieldBeforeAlias proves the exit condition of
// acceptance C2: because Compile fails at the source, the Graph is never built
// and there is no accessor return value that could alias the caller's mutable
// struct.
func TestCompileRejectsUnexportedFieldBeforeAlias(t *testing.T) {
	probe := hiddenMutableValue()
	def := baseUnexportedDef()
	def.Nodes[1].Parameters = map[string]any{"probe": probe}

	_, err := graph.Compile(def)
	if err == nil {
		t.Fatal("Compile succeeded, want rejection")
	}

	// Mutating the original input after rejection must not affect any Graph,
	// because no Graph was produced. This assertion documents the contract.
	probe.hidden["key"] = "mutated-input"
	if probe.hidden["key"] != "mutated-input" {
		t.Fatal("probe mutation unexpectedly failed")
	}
}
