package apiserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
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
			name: "execute",
			path: "/v1/workflows/execute",
			body: executeWorkflowRequest{Workflow: validWorkflow()},
			decode: func(t *testing.T, b []byte) string {
				var out executeWorkflowResponse
				if err := json.Unmarshal(extractData(t, b), &out); err != nil {
					t.Fatal(err)
				}
				return string(out.ExecutionID)
			},
		},
		{
			name: "execute_with_entry",
			path: "/v1/workflows/execute",
			body: executeWorkflowRequest{Workflow: validWorkflow(), Entry: "start"},
			decode: func(t *testing.T, b []byte) string {
				var out executeWorkflowResponse
				if err := json.Unmarshal(extractData(t, b), &out); err != nil {
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

// extractData unmarshals an enveloped response body and returns the raw JSON
// of its data field, so a per-route type can be decoded from it.
func extractData(t *testing.T, b []byte) []byte {
	t.Helper()
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("decode envelope: %v (body=%s)", err, b)
	}
	return env.Data
}

// TestInvokeByIDPreallocatesFreshExecutionID guards the by-id invoke route
// against reusing one execution id for every invoke of a workflow.
//
// POST /v1/workflows/{id}/execute counts as workflow create/invoke: it starts a
// NEW execution, so the id it runs under must be freshly minted each time. It
// carried workflowIDResolver, whose executionID slot returns the WORKFLOW id from
// the path; authzWrap injects that as the execution id (authz_wrap.go) and
// engine.Submit/Invoke adopt it instead of minting one (preallocOrNewExecutionID),
// so every by-id invoke of one workflow collapsed onto a single execution id and
// the engine's outbox / node state / advance markers (all keyed by that id) were
// reused. The second and later invokes were admitted — 200, with an execution id —
// but never dispatched, never reached a terminal status, and the caller burned its
// whole budget waiting. The webscan RemoteExecutor invokes this route once per
// batch, so any scan wider than one batch hit exactly this.
//
// The test drives the real engine through the real authz path, so a non-empty id
// is not enough to pass: old code handed out the workflow id and a constant, both
// of which are non-empty. Two invokes must therefore yield two DIFFERENT ids, and
// neither may be the workflow id.
func TestInvokeByIDPreallocatesFreshExecutionID(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var reg registerWorkflowResponse
	decodeEnvelope(t, resp, &reg)
	if reg.WorkflowID == "" {
		t.Fatal("register returned an empty workflow id")
	}

	invoke := func() string {
		t.Helper()
		body, _ := json.Marshal(executeRegisteredRequest{})
		req, _ := http.NewRequest(http.MethodPost,
			srv.URL+"/v1/workflows/"+string(reg.WorkflowID)+"/execute",
			strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer tok-full")
		execResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		defer func() { _ = execResp.Body.Close() }()
		if execResp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", execResp.StatusCode)
		}
		var out executeWorkflowResponse
		decodeEnvelope(t, execResp, &out)
		if out.ExecutionID == "" {
			t.Fatal("invoke returned an empty execution_id")
		}
		return string(out.ExecutionID)
	}

	first := invoke()
	second := invoke()

	if first == string(reg.WorkflowID) || second == string(reg.WorkflowID) {
		t.Fatalf("invoke reused the workflow id as the execution id (%q / %q, workflow %q): "+
			"the engine keys execution state by execution id, so every later invoke "+
			"collapses onto the first one and is never dispatched",
			first, second, reg.WorkflowID)
	}
	if first == second {
		t.Fatalf("two invokes shared one execution id (%q): the second reuses the first "+
			"execution's outbox and node state and so never runs", first)
	}
}
