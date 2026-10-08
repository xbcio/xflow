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

func testRunDefinition() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "test-run-execute",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "calc", Type: "test.action"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "calc", Input: "main"}}}},
		},
		PinData: map[string]any{"calc": map[string]any{"value": "mock"}},
	}
}

// TestExecuteEndpointsCarryTheTestFlag pins that "test": true on either
// execute route marks the created execution as a test run, which is what
// settings.pin_data_mode: test_only (the default) keys on, and that omitting it
// leaves the execution an ordinary run.
func TestExecuteEndpointsCarryTheTestFlag(t *testing.T) {
	tests := []struct {
		name       string
		registered bool
		entry      string
		test       bool
	}{
		{name: "inline_submit", test: true},
		{name: "inline_invoke", entry: "start", test: true},
		{name: "registered_submit", registered: true, test: true},
		{name: "registered_invoke", registered: true, entry: "start", test: true},
		{name: "inline_submit_not_test"},
		{name: "registered_invoke_not_test", registered: true, entry: "start"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, mux := newFAFExecuteTestMux(t)
			def := testRunDefinition()
			path := PathWorkflowExecute
			var body any = executeWorkflowRequest{Workflow: def, Entry: tt.entry, Test: tt.test}
			if tt.registered {
				g, err := graph.Compile(def)
				if err != nil {
					t.Fatalf("compile: %v", err)
				}
				const workflowID types.WorkflowID = "test-run"
				if _, err := provider.WorkflowRegistry().AddWorkflow(context.Background(), backend.WorkflowRecord{
					ID:             workflowID,
					Key:            "default/test-run-execute@v1",
					Namespace:      string(namespace.Default),
					Name:           def.Name,
					Version:        def.Version,
					DefinitionHash: g.Hash(),
					Definition:     def,
					Graph:          g,
				}); err != nil {
					t.Fatalf("seed workflow: %v", err)
				}
				path = "/v1/workflows/" + string(workflowID) + "/execute"
				body = executeRegisteredRequest{Entry: tt.entry, Test: tt.test}
			}

			executionID := types.ExecutionID("test-run-" + tt.name)
			resp := doFAFExecute(t, mux, path, body, executionID)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusCreated {
				t.Fatalf("status = %d, want a 2xx", resp.StatusCode)
			}
			snap, err := provider.State().GetExecution(context.Background(), executionID)
			if err != nil {
				t.Fatalf("GetExecution(%q): %v", executionID, err)
			}
			if snap == nil {
				t.Fatalf("execution %q was not created", executionID)
			}
			if snap.TestRun != tt.test {
				t.Fatalf("TestRun = %v, want %v", snap.TestRun, tt.test)
			}
		})
	}
}
