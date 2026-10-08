package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func pinDef(mode string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "pin",
		Settings: &types.WorkflowSettings{PinDataMode: mode},
		Nodes: []types.NodeDef{
			{Name: "a", Type: "test.echo", OutputSchema: map[string]any{"required": []any{"id", "amount"}}},
			{Name: "b", Type: "test.echo"},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
		PinData: map[string]any{
			"a":       map[string]any{"id": "x"},
			"missing": map[string]any{"v": 1},
		},
	}
}

func hasWarning(g *Graph, sub string) bool {
	for _, w := range g.Warnings() {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

func TestAssignPinData_ModesAndWarnings(t *testing.T) {
	g, err := Compile(pinDef(""))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if g.PinDataMode() != types.PinDataModeTestOnly {
		t.Fatalf("default mode = %q, want test_only", g.PinDataMode())
	}
	if out, ok := g.PinnedOutput(0); !ok || out["id"] != "x" {
		t.Fatalf("PinnedOutput(a) = %v, %v", out, ok)
	}
	if _, ok := g.PinnedOutput(1); ok {
		t.Fatal("b is pinned, want not")
	}
	for _, want := range []string{`"missing" 不存在`, `必填字段 [amount]`} {
		if !hasWarning(g, want) {
			t.Fatalf("warnings %q lack %q", g.Warnings(), want)
		}
	}
	if hasWarning(g, "always") {
		t.Fatalf("test_only graph warns about always: %q", g.Warnings())
	}

	always, err := Compile(pinDef(types.PinDataModeAlways))
	if err != nil {
		t.Fatalf("Compile(always): %v", err)
	}
	if !hasWarning(always, "pin_data_mode: always") {
		t.Fatalf("always graph lacks its warning: %q", always.Warnings())
	}

	if _, err := Compile(pinDef("sometimes")); err == nil {
		t.Fatal("unknown pin_data_mode compiled, want error")
	}
}

// TestAssignPinData_DisabledModeHashesLikeNoPins pins that mode=disabled leaves
// the graph identical to one without pin_data, and that pins themselves are part
// of the graph's identity.
func TestAssignPinData_DisabledModeHashesLikeNoPins(t *testing.T) {
	disabled, err := Compile(pinDef(types.PinDataModeDisabled))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	plain := pinDef(types.PinDataModeDisabled)
	plain.PinData = nil
	none, err := Compile(plain)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if disabled.Hash() != none.Hash() {
		t.Fatalf("disabled-mode hash %s != no-pin hash %s", disabled.Hash(), none.Hash())
	}
	pinned, _ := Compile(pinDef(types.PinDataModeAlways))
	if pinned.Hash() == none.Hash() {
		t.Fatal("pinned graph hashes like an unpinned one")
	}
}

func TestAssignPinData_SnapshotRoundTrip(t *testing.T) {
	g, err := Compile(pinDef(types.PinDataModeAlways))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back Graph
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.PinDataMode() != types.PinDataModeAlways {
		t.Fatalf("round-tripped mode = %q", back.PinDataMode())
	}
	if out, ok := back.PinnedOutput(0); !ok || out["id"] != "x" {
		t.Fatalf("round-tripped pin = %v, %v", out, ok)
	}
}

func TestAssignPinData_UnsupportedShapesAreIgnoredWithWarning(t *testing.T) {
	def := pinDef(types.PinDataModeAlways)
	def.Nodes[0].Disabled = true
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := g.PinnedOutput(0); ok || g.PinDataMode() != "" {
		t.Fatal("disabled node received a pin")
	}
	if !hasWarning(g, "已 disabled 且配了 pin_data，pin 被忽略") {
		t.Fatalf("warnings %q lack the disabled-and-pinned note", g.Warnings())
	}

	def = pinDef(types.PinDataModeAlways)
	def.PinData["a"] = "not an object"
	g, err = Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := g.PinnedOutput(0); ok || !hasWarning(g, "必须是对象") {
		t.Fatalf("non-object pin accepted; warnings %q", g.Warnings())
	}
}
