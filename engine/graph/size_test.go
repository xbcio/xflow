package graph

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// compileChain builds a linear graph of n nodes, optionally attaching a large
// string parameter to the first node.
func compileChain(t *testing.T, n int, payload string) *Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:        "chain",
		Connections: types.Connections{},
	}
	for i := 0; i < n; i++ {
		node := types.NodeDef{Name: fmt.Sprintf("N%02d", i), Type: "test.a"}
		if i == 0 && payload != "" {
			node.Parameters = map[string]any{"body": payload}
		}
		def.Nodes = append(def.Nodes, node)
		if i > 0 {
			from := fmt.Sprintf("N%02d", i-1)
			to := fmt.Sprintf("N%02d", i)
			def.Connections[from] = map[string]types.PortConnections{
				"main": {Targets: []types.Connection{{Node: to, Input: "main"}}},
			}
		}
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile %d-node chain: %v", n, err)
	}
	return g
}

func TestEstimatedHeapBytes_NilAndEmpty(t *testing.T) {
	var g *Graph
	if got := g.EstimatedHeapBytes(); got != 0 {
		t.Errorf("nil graph = %d bytes, want 0", got)
	}
	if got := (&Graph{}).EstimatedHeapBytes(); got != graphBaseBytes {
		t.Errorf("empty graph = %d bytes, want %d", got, graphBaseBytes)
	}
}

// TestEstimatedHeapBytes_ScalesWithGraph makes the estimate track the node
// layer: it must charge at least a node's base cost per node, and it must never
// come out below the wire form of the same graph (decoded structures include
// everything the JSON carries plus the Go-side scaffolding).
func TestEstimatedHeapBytes_ScalesWithGraph(t *testing.T) {
	small := compileChain(t, 2, "")
	big := compileChain(t, 64, "")

	delta := big.EstimatedHeapBytes() - small.EstimatedHeapBytes()
	if want := int64(62) * nodeBaseBytes; delta < want {
		t.Errorf("2→64 nodes grew the estimate by %d bytes, want at least %d", delta, want)
	}

	for _, g := range []*Graph{small, big} {
		raw, err := json.Marshal(g)
		if err != nil {
			t.Fatalf("marshal %s: %v", g.Name(), err)
		}
		if est := g.EstimatedHeapBytes(); est < int64(len(raw)) {
			t.Errorf("%s: estimate %d < wire form %d; the budget would undercount", g.Name(), est, len(raw))
		}
	}
}

// TestEstimatedHeapBytes_ChargesParameterPayload covers the weight that
// actually dominates real graphs: parameter strings (bodies, scripts,
// templates) must be charged their length, not a flat per-entry cost.
func TestEstimatedHeapBytes_ChargesParameterPayload(t *testing.T) {
	payload := strings.Repeat("x", 8192)
	withPayload := compileChain(t, 1, payload)
	without := compileChain(t, 1, "")

	growth := withPayload.EstimatedHeapBytes() - without.EstimatedHeapBytes()
	if growth < int64(len(payload)) {
		t.Errorf("8 KiB parameter grew the estimate by only %d bytes", growth)
	}
}

// TestEstimatedHeapBytes_SelfReferentialValue proves the depth guard: a
// hand-built parameter map that contains itself must not recurse without bound.
func TestEstimatedHeapBytes_SelfReferentialValue(t *testing.T) {
	loop := map[string]any{}
	loop["self"] = loop
	g := &Graph{
		nodes: []NodeMeta{{Name: "n", Parameters: map[string]any{"loop": loop}}},
	}
	if got := g.EstimatedHeapBytes(); got <= 0 {
		t.Fatalf("estimate = %d, want a positive bounded value", got)
	}
}
