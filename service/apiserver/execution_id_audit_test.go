package apiserver

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestAuthzPreallocatedExecutionIDCorrelatesAudit is the R3.1 regression guard.
// Before R3.1, workflow create/invoke mounted with a nil resource resolver, so
// the authz wrapper wrote the admission audit row with an empty ExecutionID
// (the id was only minted inside engine.Submit, after admission, and the
// response body was never read back). Now newExecutionIDResolver pre-allocates
// the id, authzWrap injects it via engine.WithExecutionID, and engine.Submit
// reuses it — so the admission row, the outcome row, and the response
// execution_id must all carry the same non-empty id.
//
// This test wires the real authz path (registerAuthzRoutes + newExecutionIDResolver)
// with a fake facade that echoes the ctx id (the engine-side persistence of the
// ctx id is covered by engine/execution_id_prealloc_test.go).
func TestAuthzPreallocatedExecutionIDCorrelatesAudit(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		body   any
		decode func(t *testing.T, b []byte) string
	}{
		{
			name: "submit",
			path: "/v1/workflows",
			body: submitWorkflowRequest{Workflow: validWorkflow()},
			decode: func(t *testing.T, b []byte) string {
				var out submitWorkflowResponse
				if err := json.Unmarshal(b, &out); err != nil {
					t.Fatal(err)
				}
				return string(out.ExecutionID)
			},
		},
		{
			name: "invoke",
			path: "/v1/workflows/invoke",
			body: invokeRequest{Workflow: validWorkflow(), Entry: "start"},
			decode: func(t *testing.T, b []byte) string {
				var out invokeResponse
				if err := json.Unmarshal(b, &out); err != nil {
					t.Fatal(err)
				}
				return string(out.ExecutionID)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}
			audit := NewInMemoryAuditSink()
			f := &fakeControlFacade{}
			m := authzModule(t, auth, ScopeAuthorizer{}, audit)
			m.eng = f // echo the ctx-pre-allocated id (see fakeControlFacade.Submit/Invoke)

			mux := http.NewServeMux()
			m.registerAuthzRoutes(mux)

			resp := doJSON(t, mux, http.MethodPost, tc.path, tc.body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			bodyBytes := readBody(t, resp.Body)
			execID := tc.decode(t, bodyBytes)
			if execID == "" {
				t.Fatal("response execution_id is empty")
			}

			events := audit.Events()
			if len(events) != 2 {
				t.Fatalf("audit events = %d, want 2 (admission + outcome)", len(events))
			}
			admission, outcome := events[0], events[1]
			if admission.Phase != "admission" || outcome.Phase != "outcome" {
				t.Fatalf("phases = %q/%q, want admission/outcome", admission.Phase, outcome.Phase)
			}
			if admission.ExecutionID == "" {
				t.Fatalf("admission ExecutionID is empty — resolver did not pre-allocate (R3.1 regression)")
			}
			if admission.ExecutionID != execID {
				t.Fatalf("admission ExecutionID = %q, want response execution_id %q", admission.ExecutionID, execID)
			}
			if outcome.ExecutionID != execID {
				t.Fatalf("outcome ExecutionID = %q, want %q", outcome.ExecutionID, execID)
			}
		})
	}
}

func readBody(t *testing.T, r io.Reader) []byte {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}
