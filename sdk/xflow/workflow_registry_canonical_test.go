package xflow

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// stripDefault returns a copy of def whose first node omits param, the form
// an HTTP or YAML author sends for a builtin param left at its Default.
func stripDefault(t *testing.T, def *types.WorkflowDef, param string) *types.WorkflowDef {
	t.Helper()
	if _, ok := def.Nodes[0].Parameters[param]; !ok {
		t.Fatalf("SDK build has no %q param to strip: %v", param, def.Nodes[0].Parameters)
	}
	out := *def
	out.Nodes = append([]types.NodeDef(nil), def.Nodes...)
	params := make(map[string]any, len(def.Nodes[0].Parameters))
	for k, v := range def.Nodes[0].Parameters {
		if k != param {
			params[k] = v
		}
	}
	out.Nodes[0].Parameters = params
	return &out
}

// An SDK registration matches a stored record that omits a builtin Default
// (as an HTTP registration stores it), and upgrades its legacy hash.
func TestAddWorkflowMatchesStoredDefinitionOmittingBuiltinDefault(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("canonical-stored")
	wf.Node("wait", node.Wait("go"))
	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	id, _ := seedLegacyRecord(t, eng, stripDefault(t, def, "mode"), "sha256:http-path")

	gotID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow against a stored stripped definition: %v", err)
	}
	if gotID != id {
		t.Fatalf("AddWorkflow id = %q, want stored id %q", gotID, id)
	}
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if want := mustRuntimeHash(t, def); rec.DefinitionHash != want {
		t.Fatalf("stored hash = %q, want upgraded %q", rec.DefinitionHash, want)
	}
	if _, present := rec.Definition.Nodes[0].Parameters["mode"]; present {
		t.Fatal("reconciliation rewrote the stored definition")
	}

	// A real param change still conflicts.
	changed := Workflow("canonical-stored")
	changed.Node("wait", node.WaitDuration("1m"))
	if _, err := eng.AddWorkflow(ctx, changed); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow(changed) error = %v, want ErrWorkflowConflict", err)
	}
}
