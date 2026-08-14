package xflow

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// A definition without dependency edges must hash exactly as before this
// feature existed — the runtime hash gates workflow-version conflicts.
func TestRuntimeHashUnchangedWithoutDependencyEdges(t *testing.T) {
	def := &types.WorkflowDef{
		Namespace: "default",
		Name:      "wf",
		Version:   "v1",
		Nodes:     []types.NodeDef{{Name: "a", Type: "xflow.transform.set"}},
	}
	h, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("runtimeHash: %v", err)
	}
	// Pinned literally, not merely logged: the runtime hash gates whether an
	// already-registered workflow is considered changed, so a silent payload
	// change would either re-register every workflow or stop detecting real
	// edits. Logging the value cannot detect that; only an equality check can.
	// If this fails after a deliberate payload change, update the constant in
	// the same commit that changes the payload.
	const wantHash = "runtime-sha256:v1:ed9b25378112181dff504e0e587e980b4e3dd30581ad6a251481626c523b822a"
	if h != wantHash {
		t.Fatalf("baseline runtime hash = %q, want %q; runtimeHashPayload changed for a "+
			"dependency-edge-free workflow", h, wantHash)
	}

	payload := runtimeHashPayload{}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal empty payload: %v", err)
	}
	if strings.Contains(string(b), "dependency_edges") {
		t.Fatalf("dependency_edges must be omitted when empty: %s", b)
	}
}

func TestRuntimeHashCoversDependencyEdges(t *testing.T) {
	base := &types.WorkflowDef{
		Namespace: "default",
		Name:      "wf",
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "clean", Type: "xflow.transform.set"},
		},
	}
	h1, err := runtimeHash(base)
	if err != nil {
		t.Fatalf("runtimeHash base: %v", err)
	}

	withEdge := *base
	withEdge.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "rules"}}
	h2, err := runtimeHash(&withEdge)
	if err != nil {
		t.Fatalf("runtimeHash with edge: %v", err)
	}
	if h1 == h2 {
		t.Fatal("dependency edges must change the runtime hash")
	}
}

func TestPreCheckAcceptsSupplyNodes(t *testing.T) {
	wf := Workflow("wf")
	def := &types.WorkflowDef{
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.unregistered", Kind: types.NodeKindSupply},
		},
	}
	if err := preCheckHandlerVersions(def, wf); err != nil {
		t.Fatalf("supply node must not be pre-checked against the action registry: %v", err)
	}
}

func TestDependsOnProducesDependencyEdgeNotConnection(t *testing.T) {
	wf := Workflow("wf")
	rules := wf.LocalNode("rules", nil)
	clean := wf.LocalNode("clean", nil)
	wf.DependsOn(clean, rules)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(def.DependencyEdges) != 1 {
		t.Fatalf("DependencyEdges = %v, want 1", def.DependencyEdges)
	}
	if def.DependencyEdges[0].Node != "clean" || def.DependencyEdges[0].Supply != "rules" {
		t.Fatalf("edge = %+v", def.DependencyEdges[0])
	}
	if len(def.Connections) != 0 {
		t.Fatalf("a dependency edge must not become a dataflow connection: %v", def.Connections)
	}
}
