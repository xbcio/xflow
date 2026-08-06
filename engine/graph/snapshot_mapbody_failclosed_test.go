package graph

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// A snapshot written by a binary from before map bodies were carried on the
// wire has the map node's "body" Parameters (Parameters always travelled) but
// no map_bodies entry. Decoding that silently leaves MapBodyAt nil, and the
// failure surfaces far away and unactionably: BuildSubgraphLease ships an
// empty Package, the runner rejects the batch, and because nothing ever
// re-projects the body it fails identically on every retry, forever.
//
// So decode fails closed, exactly as ErrGroupedSnapshotMissingUnitIR already
// does for a grouped snapshot with no group definitions. Failing closed is
// what makes the condition RECOVERABLE rather than merely loud: the workflow
// registry decodes the Graph separately from the rest of its record precisely
// so an UnmarshalJSON error falls through to recompiling from the stored
// Definition (workflowreg.unmarshalWorkflowRecord), and that recompile runs
// projectMapBodies and restores the body. Returning a bodyless graph instead
// would skip that fallback and strand the workflow.
func TestSnapshotWithABodylessMapNodeFailsClosed(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "wf",
		Nodes: []types.NodeDef{mapNode(map[string]any{"items": "$input.rows", "body": subgraphBody()})},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Strip the map_bodies key the way an older binary's snapshot simply would
	// not have had it, leaving everything else byte-identical.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal to raw: %v", err)
	}
	if _, ok := raw["map_bodies"]; !ok {
		t.Fatal("fixture did not contain map_bodies; the test is not exercising what it claims")
	}
	delete(raw, "map_bodies")
	legacy, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}

	err = json.Unmarshal(legacy, &Graph{})
	if !errors.Is(err, ErrMapBodySnapshotMissingPackage) {
		t.Fatalf("decode error = %v, want ErrMapBodySnapshotMissingPackage; a map node "+
			"whose body did not survive the snapshot must fail closed so the caller can "+
			"recompile from the Definition, not decode into a graph whose every batch fails", err)
	}
	// The error has to name the node, or an operator holding a 200-node graph
	// learns only that "a" body is missing.
	if !strings.Contains(err.Error(), "m") {
		t.Errorf("error %q does not name the offending map node", err)
	}
}

// The guard keys off the map node's own "body" parameter, so a map node using
// the expression form (no body at all) must still decode. Otherwise the
// fail-closed check would reject every expression-form map workflow in
// existence.
func TestSnapshotWithAnExpressionMapNodeStillDecodes(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			mapNode(map[string]any{"items": "$input.rows", "expression": "${{ $item.id }}"}),
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(data, &Graph{}); err != nil {
		t.Fatalf("an expression-form map node has no body to lose, so its snapshot "+
			"must still decode: %v", err)
	}
}
