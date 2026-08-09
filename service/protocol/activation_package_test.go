package protocol

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestActivateDirective_PackageRoundTripsOverJSON verifies the new Package
// field survives a JSON encode/decode cycle (the shape every transport in
// this codebase uses on the wire) and that omitting it produces no
// "package" key at all — old runners must see byte-identical directives to
// before this field existed when NodeType is not a group.
func TestActivateDirective_PackageRoundTripsOverJSON(t *testing.T) {
	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "trig",
		Def: &types.WorkflowDef{
			Name:  "g",
			Nodes: []types.NodeDef{{Name: "trig", Type: "test.trig", Version: 1}},
		},
	}
	d := ActivateDirective{
		Namespace:   "default",
		WorkflowID:  "wf-1",
		EntryUnitID: "g",
		NodeType:    "xflow.group",
		Package:     pkg,
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var got ActivateDirective
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Package == nil || got.Package.GroupName != "g" || got.Package.EntryNode != "trig" {
		t.Fatalf("Package round-trip = %+v, want GroupName=g EntryNode=trig", got.Package)
	}

	// Omitted when nil (backward compatibility: an old runner's directive must
	// not gain an unexpected key).
	noPkg := ActivateDirective{Namespace: "default", WorkflowID: "wf-1", EntryUnitID: "n", NodeType: "kafka.source"}
	data2, err := json.Marshal(noPkg)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data2, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["package"]; ok {
		t.Fatalf("json = %s, must not contain \"package\" when Package is nil", data2)
	}
}
