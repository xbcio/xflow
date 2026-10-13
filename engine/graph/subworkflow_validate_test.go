package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// subWorkflowDef builds a one-node workflow whose node is an xflow.subworkflow
// with the given parameters.
func subWorkflowDef(params map[string]any) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: params,
		}},
	}
}

func TestCompileSubWorkflowParamShape(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr string
	}{
		{"missing workflow", map[string]any{}, "workflow"},
		{"empty workflow", map[string]any{"workflow": ""}, "workflow"},
		{"blank workflow", map[string]any{"workflow": "   "}, "workflow"},
		{"non-string workflow", map[string]any{"workflow": 7}, "workflow"},
		{"non-string version", map[string]any{"workflow": "other", "version": 1}, "version"},
		{"non-string entry", map[string]any{"workflow": "other", "entry": true}, "entry"},
		{"namespace param", map[string]any{"workflow": "other", "namespace": "tenant-b"}, "namespace"},
		{"non-map input", map[string]any{"workflow": "other", "input": "${{ $params.x }}"}, "input"},
		{"non-map output", map[string]any{"workflow": "other", "output": []any{"a"}}, "output"},
		{
			name: "minimal",
			params: map[string]any{
				"workflow": "other",
			},
		},
		{
			name: "full",
			params: map[string]any{
				"workflow": "other",
				"version":  "v1",
				"entry":    "start_node",
				"input":    map[string]any{"order_id": "${{ $params.order_id }}"},
				"output":   map[string]any{"receipt": "${{ $child['charge'].receipt_id }}"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(subWorkflowDef(tt.params))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Compile() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Compile() accepted params %v, want error naming %q", tt.params, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not name %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompileSubWorkflowMemberOfGroupRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "ingest", Type: "kafka", Kind: types.NodeKindTrigger},
			{Name: "child", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "other"}},
			{Name: "store", Type: "db", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest": {"main": {Targets: []types.Connection{{Node: "child", Input: "main"}}}},
			"child":  {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "child"}}},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("a sub-workflow node inside a group was accepted: a group runs inside a " +
			"runner, which cannot create the child execution the node needs")
	}
	if !strings.Contains(err.Error(), `"child"`) || !strings.Contains(err.Error(), "group") {
		t.Fatalf("rejection message %q does not name the offending member and the reason", err)
	}
}

func TestCompileFAFRejectsSubWorkflowNode(t *testing.T) {
	def := validFAFDef()
	def.Nodes[0].Type = types.SubWorkflowNodeType
	def.Nodes[0].Parameters = map[string]any{"workflow": "other"}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("an faf workflow accepted a sub-workflow node: faf persists nothing, " +
			"so there is nowhere to record the child execution")
	}
	if !strings.Contains(err.Error(), types.SubWorkflowNodeType) {
		t.Fatalf("rejection message %q does not name %s", err, types.SubWorkflowNodeType)
	}
}

func TestSubWorkflowNodeIndexes(t *testing.T) {
	plain, err := Compile(&types.WorkflowDef{
		Name:  "plain",
		Nodes: []types.NodeDef{{Name: "work", Type: "xflow.noop"}},
	})
	if err != nil {
		t.Fatalf("compile plain graph: %v", err)
	}
	if got := plain.SubWorkflowNodeIndexes(); got != nil {
		t.Fatalf("plain graph returned sub-workflow indexes %v, want nil", got)
	}

	// Two sub-workflow nodes with an ordinary node between them, so the
	// accessor's result is not just "every index" in declaration order.
	mixed, err := Compile(&types.WorkflowDef{
		Name: "mixed",
		Nodes: []types.NodeDef{
			{Name: "one", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "a"}},
			{Name: "work", Type: "xflow.noop"},
			{Name: "two", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "b"}},
		},
	})
	if err != nil {
		t.Fatalf("compile mixed graph: %v", err)
	}
	got := mixed.SubWorkflowNodeIndexes()
	if len(got) != 2 {
		t.Fatalf("SubWorkflowNodeIndexes() = %v, want the two sub-workflow nodes", got)
	}
	oneIdx, _ := mixed.NodeIndex("one")
	twoIdx, _ := mixed.NodeIndex("two")
	if got[0] != oneIdx || got[1] != twoIdx {
		t.Fatalf("SubWorkflowNodeIndexes() = %v, want [%d %d]", got, oneIdx, twoIdx)
	}
}
