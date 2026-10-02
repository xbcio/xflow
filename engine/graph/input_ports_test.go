package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// portDef builds A -> M and B -> M, with M declaring whatever the caller wants
// and each edge carrying the label the caller wants.
func portDef(declared []types.PortDecl, aInput, bInput string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "input-ports",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "M", Type: "test.noop", Inputs: declared},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M", Input: aInput}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "M", Input: bInput}}}},
		},
	}
}

// TestCompile_EdgeTargetingAnUndeclaredPortIsRejected pins that a declaration
// is binding: once a node names its ports, an edge onto any other port is a
// typo the compiler must not accept silently.
func TestCompile_EdgeTargetingAnUndeclaredPortIsRejected(t *testing.T) {
	def := portDef([]types.PortDecl{{Name: "inventory"}, {Name: "price"}}, "inventory", "shipping")
	_, err := Compile(def)
	if err == nil {
		t.Fatal("Compile accepted an edge onto the undeclared port \"shipping\"")
	}
	if !strings.Contains(err.Error(), "shipping") {
		t.Fatalf("error = %v, want it to name the offending port", err)
	}
}

// TestCompile_RequiredPortMustBeConnected: `required: true` means the port must
// carry an edge, so a node that declares a required port and never receives one
// fails at compile time rather than running with an input the author said was
// mandatory.
func TestCompile_RequiredPortMustBeConnected(t *testing.T) {
	// Both edges land on the declared "price" port, so the only rule this
	// fixture can trip is the unconnected required port "inventory".
	def := portDef([]types.PortDecl{{Name: "inventory", Required: true}, {Name: "price"}}, "price", "price")

	_, err := Compile(def)
	if err == nil {
		t.Fatal("Compile accepted a node with an unconnected required input port")
	}
	if !strings.Contains(err.Error(), "inventory") || !strings.Contains(err.Error(), "required") {
		t.Fatalf("error = %v, want it to name the unconnected required port", err)
	}
}

// TestCompile_FanInWithoutDeclaredPortsWarns is the authored graph's half of the
// implicit-main rule: the graph still compiles, but the author is told that its
// upstreams share one key.
func TestCompile_FanInWithoutDeclaredPortsWarns(t *testing.T) {
	g, err := Compile(portDef(nil, "", "alt"))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	warnings := g.Warnings()
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, `"M"`) || !strings.Contains(joined, "inputs") {
		t.Fatalf("warnings = %v, want one naming node M and its missing inputs declaration", warnings)
	}
}

// TestCompile_DeclaredPortsSilenceTheFanInWarning: the warning exists to push
// authors toward declaring ports, so declaring them must silence it.
func TestCompile_DeclaredPortsSilenceTheFanInWarning(t *testing.T) {
	g, err := Compile(portDef([]types.PortDecl{{Name: "inventory"}, {Name: "price"}}, "inventory", "price"))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if w := g.Warnings(); len(w) != 0 {
		t.Fatalf("warnings = %v, want none for a node that declares both ports", w)
	}
}

// TestCompile_UndeclaredNodeAcceptsAnyLabel is the compatibility half: every
// graph authored before the declaration existed keeps compiling, labels and
// all, so the contract adds a way to be explicit without invalidating what
// already runs.
func TestCompile_UndeclaredNodeAcceptsAnyLabel(t *testing.T) {
	if _, err := Compile(portDef(nil, "left", "right")); err != nil {
		t.Fatalf("Compile rejected labels on an undeclared node: %v", err)
	}
}
