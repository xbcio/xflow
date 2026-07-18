package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// benchGraph builds a graph with moderately nested Parameters/Vars/Config so
// the defensive-copy cost of the public accessors is representative of a real
// dispatch (not the trivial empty-map case).
func benchGraph(b *testing.B) *Graph {
	b.Helper()
	def := &types.WorkflowDef{
		Name: "bench",
		Context: &types.WorkflowContext{
			Vars: map[string]any{
				"nested": map[string]any{"value": "v", "deep": map[string]any{"x": 1}},
				"list":   []any{map[string]any{"k": "v"}, []string{"a", "b"}},
			},
			Config: map[string]any{"nested": map[string][]string{"labels": {"prod", "critical"}}},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name: "worker",
				Type: "test.worker",
				Parameters: map[string]any{
					"nested": map[string]any{"value": "v", "deep": map[string]any{"x": 1}},
					"list":   []any{map[string]any{"k": "v"}, []string{"a", "b"}},
				},
				Retry: &types.RetrySettings{Enabled: true, MaxAttempts: 3},
			},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "worker", Input: "main"}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		b.Fatal(err)
	}
	return g
}

// BenchmarkNodeName_HotPath measures the zero-copy name accessor that the
// engine dispatch hot path uses to resolve upstream node names. It must be
// allocation-free.
func BenchmarkNodeName_HotPath(b *testing.B) {
	g := benchGraph(b)
	idx, _ := g.NodeIndex("worker")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.NodeName(idx)
	}
}

// BenchmarkNodeAt_DeepCopy measures the cost of the public defensive-copy
// accessor the engine uses once per dispatched task to obtain an isolated
// Parameters map for the handler. This is the intrinsic isolation copy (the
// handler is untrusted), not a redundant defense-in-depth copy.
func BenchmarkNodeAt_DeepCopy(b *testing.B) {
	g := benchGraph(b)
	idx, _ := g.NodeIndex("worker")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.NodeAt(idx).Parameters
	}
}

// BenchmarkVars_DeepCopy measures the public Vars accessor's deep copy.
func BenchmarkVars_DeepCopy(b *testing.B) {
	g := benchGraph(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Vars()
	}
}

// BenchmarkNodeOutEdges_NewSlice measures the public edge accessor.
func BenchmarkNodeOutEdges_NewSlice(b *testing.B) {
	g := benchGraph(b)
	idx, _ := g.NodeIndex("start")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.NodeOutEdges(idx)
	}
}
