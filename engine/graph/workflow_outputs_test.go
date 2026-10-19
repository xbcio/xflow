package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestCompileRejectsWorkflowLevelOutputs pins the compile-time contract for
// WorkflowDef.Outputs: the field is declarative only, nothing at runtime reads
// it, so a definition that sets it registered cleanly and silently exposed
// nothing. Compile rejects it instead of pretending the declarations work.
func TestCompileRejectsWorkflowLevelOutputs(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "outputs",
		Nodes: []types.NodeDef{
			{Name: "only", Type: "test.worker"},
		},
		Outputs: map[string]types.WorkflowOutput{
			"status": {Value: "${{ $nodes['only'].status }}", DisplayName: "Status"},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("workflow-level outputs compiled clean")
	}
	if msg := err.Error(); !strings.Contains(msg, "workflow-level outputs are not supported") {
		t.Fatalf("error %q does not say workflow-level outputs are not supported", msg)
	}
}

// TestCompileAcceptsAbsentWorkflowLevelOutputs is the control: the rejection
// must fire on a declared block only, not on the (nil or empty) field every
// other definition carries.
func TestCompileAcceptsAbsentWorkflowLevelOutputs(t *testing.T) {
	for name, outputs := range map[string]map[string]types.WorkflowOutput{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			def := &types.WorkflowDef{
				Name:    "no-outputs",
				Nodes:   []types.NodeDef{{Name: "only", Type: "test.worker"}},
				Outputs: outputs,
			}
			if _, err := Compile(def); err != nil {
				t.Fatalf("Compile: %v", err)
			}
		})
	}
}
