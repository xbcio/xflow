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

// TestCompileAcceptsLiteralBracketSupplyRef pins task #46's fix A: a quoted
// bracket subscript is just as statically derivable as a dotted name, and a
// hyphenated supply name has no valid dot-form syntax at all (see
// dependency.go's suppliesRefPattern comment for the probed expr-lang
// failure), so the bracket form must be the accepted syntax for it.
func TestCompileAcceptsLiteralBracketSupplyRef(t *testing.T) {
	def := supplyDef()
	def.Nodes[1].Name = "my-supply"
	def.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "my-supply"}}
	def.Nodes[2].Parameters = map[string]any{"code": `$supplies["my-supply"].field`}
	if _, err := Compile(def); err != nil {
		t.Fatalf("compile: %v", err)
	}
}

// TestCompileRejectsLiteralBracketSupplyRefWithoutEdge is fix A step 2's
// teeth: it proves the literal-bracket name actually reaches
// deriveSupplyRefs (and is therefore checked against declared dependency
// edges), not merely that A's exemption from the dynamic-use rejection lets
// it compile unconditionally. Without this test, a change that stops
// extracting the name (but still exempts it from hasDynamicSupplyRef) would
// pass TestCompileAcceptsLiteralBracketSupplyRef while silently breaking the
// two g.supplyRefs consumers named in the task brief
// (entry_activation_manager.go's SupplyRefsFor and subgraph_package.go's
// buildVisibleSupplies), because nothing would ever require the edge to
// exist.
func TestCompileRejectsLiteralBracketSupplyRefWithoutEdge(t *testing.T) {
	def := supplyDef()
	def.Nodes[1].Name = "my-supply"
	// No DependencyEdges entry for "my-supply": the reference is unsatisfiable.
	def.DependencyEdges = nil
	def.Nodes[2].Parameters = map[string]any{"code": `$supplies["my-supply"].field`}
	_, err := Compile(def)
	if err == nil || !strings.Contains(err.Error(), "no dependency edge") {
		t.Fatalf("err = %v, want missing-dependency-edge rejection", err)
	}
}

// TestCompileAcceptsSingleAndDoubleQuotedLiteralSupplyRef pins the probed
// fact (exprx.EvalExpr, run directly against this task, not assumed) that
// expr-lang accepts both quote styles for a map-index string literal:
// $supplies['my-supply'] and $supplies["my-supply"] evaluate identically.
// dependency.go's compile-time acceptance must match that run-time reality
// in both directions, or one quote style would compile but panic/error at
// run time (bug B's whole failure mode, just for the other quote style).
func TestCompileAcceptsSingleAndDoubleQuotedLiteralSupplyRef(t *testing.T) {
	for _, code := range []string{
		`$supplies["my-supply"].field`,
		`$supplies['my-supply'].field`,
	} {
		def := supplyDef()
		def.Nodes[1].Name = "my-supply"
		def.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "my-supply"}}
		def.Nodes[2].Parameters = map[string]any{"code": code}
		if _, err := Compile(def); err != nil {
			t.Fatalf("code %q: compile: %v", code, err)
		}
	}
}

// TestCompileRejectsHyphenatedDotSupplyRef pins fix B: dependency.go's
// suppliesRefPattern historically accepted a hyphen in $supplies.<name>, so
// this used to pass compilation (as long as SOME dependency edge existed with
// that literal name) and only fail once expr-lang evaluated the expression at
// run time. Probed directly (exprx.EvalExpr against an env containing a
// "my-supply" key): $supplies.my-supply.field fails with
// `compile expression: unknown name supply (1:14)` -- expr parses the hyphen
// as subtraction, not a name character. The graph compiler must now catch
// this at compile time, before it ever reaches a runner, and the error must
// point at the fix (bracket indexing) rather than surface as an unrelated
// "no dependency edge".
func TestCompileRejectsHyphenatedDotSupplyRef(t *testing.T) {
	def := supplyDef()
	def.Nodes[1].Name = "my-supply"
	def.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "my-supply"}}
	def.Nodes[2].Parameters = map[string]any{"code": `$supplies.my-supply.field`}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("err = nil, want hyphenated dot-form rejection")
	}
	if !strings.Contains(err.Error(), `$supplies["my-supply"]`) {
		t.Fatalf("err = %v, want message pointing at bracket indexing $supplies[\"my-supply\"]", err)
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
