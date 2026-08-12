package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestProjectedGroupPackageCarriesGroupIdentity pins what $workflow resolves
// to inside a group.
//
// A projected group package compiles to its own Graph, and buildInput reads
// $workflow.name / $workflow.version from THAT graph. So a group member sees
// the group's identity, not the outer workflow's:
//
//	name    = the group's name  (ProjectGroupPackage sets Def.Name = gm.Name)
//	version = the empty string  (the projected Def carries no Version)
//
// The empty version is a real consequence, not an oversight to paper over
// here: adding Version to the projected Def would change the package hash,
// which is the group dispatch's cache and idempotency key. If a group member
// is ever required to read the outer workflow's version, that must be a
// deliberate change to the package format with its hash implications weighed,
// not a quiet field addition. This test exists so that decision cannot be
// made by accident.
func TestProjectedGroupPackageCarriesGroupIdentity(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "outer-flow",
		Version: "v7",
		Nodes: []types.NodeDef{
			{Name: "a", Type: "test.noop"},
			{Name: "b", Type: "test.noop"},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g1", Members: []string{"a", "b"}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	pkg, _, err := ProjectGroupPackage(g, 0)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	inner, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("compile projected: %v", err)
	}

	if inner.Name() != "g1" {
		t.Fatalf("projected graph Name() = %q, want the group's name %q -- this is "+
			"what a group member's $workflow.name resolves to", inner.Name(), "g1")
	}
	if inner.WorkflowVersion() != "" {
		t.Fatalf("projected graph WorkflowVersion() = %q, want the empty string. "+
			"If Version was deliberately added to the projected Def, weigh the "+
			"package-hash change (it is the group dispatch cache and idempotency "+
			"key) and update the DSL spec's $workflow entry", inner.WorkflowVersion())
	}

	// The outer graph keeps its own identity -- projection must not mutate it.
	if g.Name() != "outer-flow" || g.WorkflowVersion() != "v7" {
		t.Fatalf("projection mutated the outer graph's identity: name=%q version=%q",
			g.Name(), g.WorkflowVersion())
	}
}
