package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

func fafExecuteDefinition() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "faf-execute",
		Version: "v1",
		Options: &types.WorkflowOptions{FAF: true},
		Nodes: []types.NodeDef{{
			Name: "only",
			Type: "test.action",
			Kind: types.NodeKindAction,
		}},
	}
}

func newFAFExecuteTestMux(t *testing.T) (*local.Backend, http.Handler) {
	t.Helper()
	provider := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: provider})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	module := newWorkflowControlModule(cp, nil, nil, nil)
	mux := http.NewServeMux()
	module.RegisterHTTP(mux)
	return provider, mux
}

func doFAFExecute(t *testing.T, handler http.Handler, path string, body any, executionID types.ExecutionID) *http.Response {
	t.Helper()
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, &encoded)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(engine.WithExecutionID(req.Context(), executionID))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Result()
}

// TestFAFExecuteEndpointsReturnUnsupportedWithoutDurableState proves that both
// execute routes surface the engine's FAF sentinel as a stable client error.
// The registered cases seed a deliberately stale FAF record directly because
// current registration rejects FAF definitions before they can reach this path.
func TestFAFExecuteEndpointsReturnUnsupportedWithoutDurableState(t *testing.T) {
	tests := []struct {
		name       string
		registered bool
		entry      string
	}{
		{name: "inline_submit"},
		{name: "inline_invoke", entry: "only"},
		{name: "registered_submit", registered: true},
		{name: "registered_invoke", registered: true, entry: "only"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, mux := newFAFExecuteTestMux(t)
			def := fafExecuteDefinition()
			path := PathWorkflowExecute
			var body any = executeWorkflowRequest{Workflow: def, Entry: tt.entry}

			if tt.registered {
				g, err := graph.Compile(def)
				if err != nil {
					t.Fatalf("compile stale FAF record: %v", err)
				}
				const workflowID types.WorkflowID = "faf-stale"
				if _, err := provider.WorkflowRegistry().AddWorkflow(context.Background(), backend.WorkflowRecord{
					ID:             workflowID,
					Key:            "default/faf-execute@v1",
					Namespace:      string(namespace.Default),
					Name:           def.Name,
					Version:        def.Version,
					DefinitionHash: g.Hash(),
					Definition:     def,
					Graph:          g,
				}); err != nil {
					t.Fatalf("seed stale FAF workflow: %v", err)
				}
				path = "/v1/workflows/" + string(workflowID) + "/execute"
				body = executeRegisteredRequest{Entry: tt.entry}
			}

			executionID := types.ExecutionID("faf-" + tt.name)
			resp := doFAFExecute(t, mux, path, body, executionID)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			env := decodeEnvelope(t, resp, nil)
			if env.Success || env.Code != "workflow_faf_unsupported" {
				t.Fatalf("envelope = %+v, want workflow_faf_unsupported failure", env)
			}

			snapshot, err := provider.State().GetExecution(context.Background(), executionID)
			if err != nil {
				t.Fatalf("GetExecution(%q): %v", executionID, err)
			}
			if snapshot != nil {
				t.Fatalf("FAF execute created durable execution state: %+v", snapshot)
			}
		})
	}
}
