package xflow

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
)

// TestDefinitionMatchesTheRegistrationBuild pins Definition to build, the
// function AddWorkflow calls.
//
// Definition exists so a caller outside this package can assert on what its own
// builder function produced -- most usefully, that a config field reached a
// node's parameters. That assertion is only worth anything if Definition sees
// what registration sees: a second, subtly different assembly path would let a
// test go green on a definition nothing ever runs.
func TestDefinitionMatchesTheRegistrationBuild(t *testing.T) {
	build := func() *WorkflowBuilder {
		wf := Workflow("definition-parity")
		items := wf.Node("items", node.Map("$input.rows", 10).Concurrency(4))
		body := Workflow("definition-parity-body")
		body.Node("step", node.Function("return $item"))
		items.Body(body)
		return wf
	}

	viaDefinition, err := build().Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	viaBuild, err := build().build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if !reflect.DeepEqual(viaDefinition, viaBuild) {
		t.Errorf("Definition() and build() disagree.\nDefinition: %#v\nbuild:      %#v",
			viaDefinition, viaBuild)
	}
}

// TestDefinitionCarriesNodeParameters is the use case, at the smallest scale
// that shows it: a parameter set through a builder method is readable from the
// definition without an engine, a backend, or a registry.
func TestDefinitionCarriesNodeParameters(t *testing.T) {
	wf := Workflow("definition-params")
	wf.Node("items", node.Map("$input.rows", 10).Concurrency(4))

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if len(def.Nodes) != 1 {
		t.Fatalf("node count = %d, want 1", len(def.Nodes))
	}
	if got := def.Nodes[0].Parameters["body_concurrency"]; got != 4 {
		t.Errorf("body_concurrency = %#v, want 4", got)
	}
}

// TestDefinitionReportsBuildErrors checks Definition does not swallow what
// registration would have rejected. A body that references itself is a build
// error, not a definition.
func TestDefinitionReportsBuildErrors(t *testing.T) {
	wf := Workflow("definition-cyclic")
	items := wf.Node("items", node.Map("$input.rows", 10))
	items.Body(wf)

	if _, err := wf.Definition(); err == nil {
		t.Fatal("Definition returned no error for a self-referencing body")
	}
}
