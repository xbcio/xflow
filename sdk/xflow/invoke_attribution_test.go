package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestInvokeStampsAddressedWorkflowID pins the SDK half of the attribution
// fix: the id this Invoke addresses is the execution's workflow identity even
// when the stored definition still carries a client-supplied id (a record
// written before registration refused them).
func TestInvokeStampsAddressedWorkflowID(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()
	ctx := context.Background()

	def := &types.WorkflowDef{
		ID:      "wf-client-chosen",
		Name:    "stamp-addressed",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	const addressed types.WorkflowID = "wf-addressed"
	if _, err := eng.workflowRegistry.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             addressed,
		Key:            "default/stamp-addressed@v1",
		Namespace:      "default",
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: g.Hash(),
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("seed workflow: %v", err)
	}

	id, err := eng.Invoke(ctx, addressed, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.WorkflowID != addressed {
		t.Fatalf("execution WorkflowID = %q, want the addressed id %q; the stored definition's id must not win",
			detail.WorkflowID, addressed)
	}
	if detail.WorkflowVersion != def.Version {
		t.Fatalf("execution WorkflowVersion = %q, want %q", detail.WorkflowVersion, def.Version)
	}
}
