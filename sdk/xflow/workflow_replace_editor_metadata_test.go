package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
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
