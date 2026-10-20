package apiserver

import (
	"context"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestPostWorkflowsRegisterRejectsWorkflowID pins the registration half of the
// attribution fix: POST /v1/workflows mints the id server-side, so a body that
// carries one is refused with the same code the inline execute route uses.
// Accepting it would let the caller choose the identity every execution of the
// workflow is attributed to.
func TestPostWorkflowsRegisterRejectsWorkflowID(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := validWorkflow()
	def.ID = "wf-client-chosen"
	resp := postWorkflows(t, srv.URL, "tok-full", def)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a registration carrying a workflow id", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_invalid" {
		t.Fatalf("code = %q, want workflow_invalid", env.Code)
	}
	ids, err := cp.WorkflowRegistry().ListWorkflows(context.Background(),
		namespace.Namespace("namespaceA"), backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("rejected registration persisted %d workflow(s): %v", len(ids), ids)
	}
}

// TestExecuteWorkflowByIDStampsServerIdentity pins the execute half: the path
// id is the execution's workflow identity even when the stored definition still
// carries a client-supplied id (a record written before registration refused
// them). Attribution must follow the server address, never the payload.
func TestExecuteWorkflowByIDStampsServerIdentity(t *testing.T) {
	provider, mux := newFAFExecuteTestMux(t)
	ctx := context.Background()

	def := testRunDefinition()
	def.ID = "wf-client-chosen"
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	const addressed types.WorkflowID = "wf-server-addressed"
	if _, err := provider.WorkflowRegistry().AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             addressed,
		Key:            "default/" + def.Name + "@" + def.Version,
		Namespace:      string(namespace.Default),
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: g.Hash(),
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("seed workflow: %v", err)
	}

	const execID types.ExecutionID = "exec-by-id-stamp"
	resp := doFAFExecute(t, mux, "/v1/workflows/"+string(addressed)+"/execute", executeRegisteredRequest{}, execID)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	snap, err := provider.State().GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution(%q): %v", execID, err)
	}
	if snap == nil {
		t.Fatalf("execution %q was not created", execID)
	}
	if snap.WorkflowID != addressed {
		t.Fatalf("execution WorkflowID = %q, want the addressed id %q; the stored definition's id must not win",
			snap.WorkflowID, addressed)
	}
	if snap.WorkflowVersion != def.Version {
		t.Fatalf("execution WorkflowVersion = %q, want %q", snap.WorkflowVersion, def.Version)
	}
}
