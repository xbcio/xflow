package xflow

import (
	"testing"

	"github.com/xbcio/xflow/node"
)

// A workflow carrying a supply node and a dependency edge must register
// successfully: the supply node has no handler by design, and
// preCheckHandlerVersions must not fail closed on it.
func TestWorkflowWithSupplyNodeRegisters(t *testing.T) {
	w := Workflow("wf-supply")
	rules := w.Node("rules", node.SupplyExternal("shared-rules"))
	clean := w.Node("clean", node.Script(`return {}`).Language("js").Runtime("goja"))
	w.DependsOn(clean, rules)

	def, err := w.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(def.DependencyEdges) != 1 {
		t.Fatalf("dependency edges = %#v", def.DependencyEdges)
	}
	if err := preCheckHandlerVersions(def, w); err != nil {
		t.Fatalf("preCheckHandlerVersions rejected a supply node: %v", err)
	}
}
