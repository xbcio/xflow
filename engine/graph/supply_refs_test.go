package graph

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestDeriveSupplyRefs(t *testing.T) {
	params := map[string]any{
		"expr": `$supplies.rules.items`,
		"nested": map[string]any{
			"a": `{{ $supplies.tags }} and {{ $supplies.rules }}`,
			"b": []any{`$supplies.zzz`, 42, `no refs here`},
		},
	}
	got := deriveSupplyRefs(params)
	want := []string{"rules", "tags", "zzz"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deriveSupplyRefs = %v, want %v", got, want)
	}
	if deriveSupplyRefs(nil) != nil {
		t.Fatal("nil params must yield nil")
	}
	if deriveSupplyRefs(map[string]any{"x": "plain"}) != nil {
		t.Fatal("params with no refs must yield nil")
	}
}

func TestCompileRejectsSupplyUseWithoutEdge(t *testing.T) {
	def := supplyDef()
	// "clean" reads $supplies.rules but the dependency edge is removed.
	def.DependencyEdges = nil
	def.Nodes[2].Parameters = map[string]any{"code": `$supplies.rules.items`}
	_, err := Compile(def)
	if err == nil || !strings.Contains(err.Error(), "no dependency edge") {
		t.Fatalf("err = %v, want missing-dependency-edge rejection", err)
	}
}

func TestCompileAcceptsSupplyUseWithEdge(t *testing.T) {
	def := supplyDef()
	def.Nodes[2].Parameters = map[string]any{"code": `$supplies.rules.items`}
	if _, err := Compile(def); err != nil {
		t.Fatalf("compile: %v", err)
	}
}

func TestCompileRejectsDynamicSupplyName(t *testing.T) {
	for _, expr := range []string{
		`$supplies[$vars.env + "-rules"]`,
		`$supplies[name]`,
		`$supplies`,
	} {
		def := supplyDef()
		def.Nodes[2].Parameters = map[string]any{"code": expr}
		_, err := Compile(def)
		if err == nil || !strings.Contains(err.Error(), "must be a static literal name") {
			t.Fatalf("expr %q: err = %v, want static-literal rejection", expr, err)
		}
	}
}

// A supply node must not itself reference $supplies: it has no dependency edges
// (Task 2 rejects supply-on-supply), so any use is unsatisfiable.
func TestCompileRejectsSupplyNodeUsingSupplies(t *testing.T) {
	def := supplyDef()
	def.Nodes[1].Parameters = map[string]any{"seed": `$supplies.rules`}
	_, err := Compile(def)
	if err == nil || !strings.Contains(err.Error(), "no dependency edge") {
		t.Fatalf("err = %v, want rejection", err)
	}
	_ = types.NodeKindSupply
}
