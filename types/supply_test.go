package types

import (
	"encoding/json"
	"testing"
)

func TestDependencyEdgesRoundTrip(t *testing.T) {
	def := WorkflowDef{
		Name: "wf",
		Nodes: []NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: NodeKindSupply},
			{Name: "clean", Type: "xflow.code.script", Kind: NodeKindAction},
		},
		DependencyEdges: []DependencyEdge{{Node: "clean", Supply: "rules"}},
	}
	b, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowDef
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.DependencyEdges) != 1 {
		t.Fatalf("dependency_edges = %d, want 1", len(got.DependencyEdges))
	}
	if got.DependencyEdges[0].Node != "clean" || got.DependencyEdges[0].Supply != "rules" {
		t.Fatalf("edge = %+v", got.DependencyEdges[0])
	}
	if got.Nodes[0].Kind != NodeKindSupply {
		t.Fatalf("kind = %q, want supply", got.Nodes[0].Kind)
	}
}

func TestDependencyEdgesOmittedWhenEmpty(t *testing.T) {
	b, err := json.Marshal(WorkflowDef{Name: "wf"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytesContains(b, "dependency_edges") {
		t.Fatalf("empty DependencyEdges must be omitted, got %s", b)
	}
}

func bytesContains(b []byte, s string) bool {
	return len(b) >= len(s) && stringIndex(string(b), s) >= 0
}

func stringIndex(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
