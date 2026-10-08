package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestEmbeddedReplaceWorkflowClearsEditorMetadata pins the documented behavior
// on Server.ReplaceWorkflow: the embedded path has no editor_metadata input
// (WorkflowBuilder/NodeDef expose none, ADR-D4 D6), so a replace through it
// always writes a nil EditorMetadata -- clearing whatever was stored by an
// earlier HTTP PUT that did carry one.
func TestEmbeddedReplaceWorkflowClearsEditorMetadata(t *testing.T) {
	c := newCrossPath(t)
	wf := crossPathWorkflow("embedded-replace-clears-metadata")

	id := c.post(c.sdkBody(wf), 201)

	withMetadata := c.sdkBody(wf)
	withMetadata["id"] = string(id)
	withMetadata["editor_metadata"] = map[string]any{
		"positions": map[string]any{"start": map[string]any{"x": 1, "y": 2}},
	}
	c.put(id, withMetadata)

	rec, err := c.srv.api.Backend().WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow after PUT: %v", err)
	}
	if rec.EditorMetadata == nil {
		t.Fatal("PUT with editor_metadata did not persist it")
	}

	// A different definition under the same name/version forces a real
	// replace (not a no-op), and ReplaceWorkflow carries no metadata.
	changed := Workflow("embedded-replace-clears-metadata")
	start := changed.Node("start", node.Start())
	wait := changed.Node("wait", node.Wait("stop"))
	changed.Connect(start, wait)

	newID, err := c.srv.ReplaceWorkflow(context.Background(), changed)
	if err != nil {
		t.Fatalf("ReplaceWorkflow: %v", err)
	}

	after, err := c.srv.api.Backend().WorkflowRegistry().GetWorkflow(context.Background(), newID)
	if err != nil {
		t.Fatalf("GetWorkflow after ReplaceWorkflow: %v", err)
	}
	if after.EditorMetadata != nil {
		t.Fatalf("EditorMetadata = %+v, want nil after an embedded ReplaceWorkflow", after.EditorMetadata)
	}
}

// TestEmbeddedReplaceWorkflowWithMetadataPersists is the explicit opt-in
// counterpart to TestEmbeddedReplaceWorkflowClearsEditorMetadata: an embedded
// caller that wants its replace to carry editor_metadata through uses
// ReplaceWorkflowWithMetadata instead of ReplaceWorkflow, and the metadata it
// passes reaches the stored record rather than being cleared.
func TestEmbeddedReplaceWorkflowWithMetadataPersists(t *testing.T) {
	c := newCrossPath(t)
	wf := crossPathWorkflow("embedded-replace-with-metadata")

	c.post(c.sdkBody(wf), 201)

	// A different definition under the same name/version forces a real
	// replace (not a no-op), same as the clears-metadata test.
	changed := Workflow("embedded-replace-with-metadata")
	start := changed.Node("start", node.Start())
	wait := changed.Node("wait", node.Wait("stop"))
	changed.Connect(start, wait)

	metadata := &types.WorkflowEditorMetadata{
		Positions: map[string]types.Position{"start": {X: 1, Y: 2}},
	}

	newID, err := c.srv.ReplaceWorkflowWithMetadata(context.Background(), changed, metadata)
	if err != nil {
		t.Fatalf("ReplaceWorkflowWithMetadata: %v", err)
	}

	after, err := c.srv.api.Backend().WorkflowRegistry().GetWorkflow(context.Background(), newID)
	if err != nil {
		t.Fatalf("GetWorkflow after ReplaceWorkflowWithMetadata: %v", err)
	}
	if after.EditorMetadata == nil {
		t.Fatal("ReplaceWorkflowWithMetadata did not persist the editor metadata")
	}
	got, ok := after.EditorMetadata.Positions["start"]
	if !ok || got != (types.Position{X: 1, Y: 2}) {
		t.Fatalf("EditorMetadata.Positions[\"start\"] = %+v, ok=%v, want {X:1 Y:2}", got, ok)
	}
}
