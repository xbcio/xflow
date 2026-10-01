package node_test

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestBuiltinParamSpecsCoversEveryBuiltin pins BuiltinParamSpecs' fixed table
// to every registered "xflow." builtin plus the supply declarations, so a new
// builtin cannot ship without hash canonicalization knowing its Defaults.
func TestBuiltinParamSpecsCoversEveryBuiltin(t *testing.T) {
	for _, vd := range builtinDescriptors(t) {
		t.Run(vd.desc.Type, func(t *testing.T) {
			got, ok := node.BuiltinParamSpecs(vd.desc.Kind, vd.desc.Type, vd.version)
			if !ok {
				t.Fatalf("BuiltinParamSpecs(%q, %q, %d) not found", vd.desc.Kind, vd.desc.Type, vd.version)
			}
			if !reflect.DeepEqual(got, vd.desc.Params) {
				t.Fatalf("BuiltinParamSpecs(%q, %q, %d) differs from the registered descriptor", vd.desc.Kind, vd.desc.Type, vd.version)
			}
		})
	}
}

func TestBuiltinParamSpecsLookup(t *testing.T) {
	tests := []struct {
		name    string
		kind    types.NodeKind
		typ     string
		version int
		want    bool
	}{
		{"action exact version", types.NodeKindAction, "xflow.http", 1, true},
		{"empty kind is action", "", "xflow.http", 1, true},
		{"version 0 is latest", types.NodeKindAction, "xflow.wait", 0, true},
		{"trigger", types.NodeKindTrigger, "xflow.trigger.cron", 1, true},
		{"supply as the SDK builds it", types.NodeKindSupply, "xflow.supply.static", 1, true},
		{"supply version 0", types.NodeKindSupply, "xflow.supply.external", 0, true},
		{"unknown version", types.NodeKindAction, "xflow.http", 2, false},
		{"kind mismatch", types.NodeKindTrigger, "xflow.http", 1, false},
		{"trigger with empty kind", "", "xflow.trigger.cron", 1, false},
		{"custom type", types.NodeKindAction, "test.custom.node", 1, false},
		{"direct handler", types.NodeKindAction, "__direct__/x", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := node.BuiltinParamSpecs(tt.kind, tt.typ, tt.version); ok != tt.want {
				t.Fatalf("BuiltinParamSpecs(%q, %q, %d) ok = %v, want %v", tt.kind, tt.typ, tt.version, ok, tt.want)
			}
		})
	}
}

func TestBuiltinParamSpecsReturnsACopy(t *testing.T) {
	specs, ok := node.BuiltinParamSpecs(types.NodeKindAction, "xflow.wait", 1)
	if !ok || len(specs) == 0 {
		t.Fatalf("BuiltinParamSpecs(xflow.wait) = (%d specs, %v), want specs", len(specs), ok)
	}
	specs[0].Default = "mutated"
	specs[0].Name = "mutated"
	again, _ := node.BuiltinParamSpecs(types.NodeKindAction, "xflow.wait", 1)
	if again[0].Name == "mutated" || again[0].Default == "mutated" {
		t.Fatalf("mutating a returned spec leaked into the table: %+v", again[0])
	}
}
