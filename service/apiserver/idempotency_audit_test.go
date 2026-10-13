package apiserver

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// keyedAuditModule wires the authz routes over a REAL engine on a backend the
// caller keeps, so the test can hold the audit rows against the store's
// executions — the correlation R3.1 exists for.
func keyedAuditModule(t *testing.T, ns string) (http.Handler, *local.Backend, *InMemoryAuditSink) {
	t.Helper()
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newWorkflowControlModule(cp, nil, nil, nil)
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: ns, Scopes: []string{"workflow"}}}
	m.authorizer = ScopeAuthorizer{}
	audit := NewInMemoryAuditSink()
	m.audit = audit
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	return mux, be, audit
}

// requireKeyedRows pins one request's admission + outcome rows to execID.
func requireKeyedRows(t *testing.T, events []AuditEvent, want types.ExecutionID) {
	t.Helper()
	if len(events) != 2 {
		t.Fatalf("audit events = %d, want 2 (admission + outcome)", len(events))
	}
	admission, outcome := events[0], events[1]
	if admission.Phase != "admission" || outcome.Phase != "outcome" {
		t.Fatalf("phases = %q/%q, want admission/outcome", admission.Phase, outcome.Phase)
	}
	if admission.ExecutionID != string(want) {
		t.Fatalf("admission ExecutionID = %q, want the running execution %q", admission.ExecutionID, want)
	}
	if outcome.ExecutionID != string(want) {
		t.Fatalf("outcome ExecutionID = %q, want the running execution %q", outcome.ExecutionID, want)
	}
}

// TestKeyedSubmissionAuditRowsNameTheExecutionThatRuns is the R3.1 guard for
// keyed submissions: the resolver pre-allocates the id the admission row is
// written with, so a keyed request must be resolved to the SAME id the engine
// will run under — engine.IdempotentExecutionID — or both audit rows would name
// a phantom no execution ever had, and reconcile Probe would settle a submission
// as absent while its execution ran.
//
// The test drives the real authz path, a real engine and a real store, so a
// non-empty id is not enough: the id has to be the key-derived one, exist in the
// store, and come back identically on a retry.
func TestKeyedSubmissionAuditRowsNameTheExecutionThatRuns(t *testing.T) {
	const ns = "ns-keyed-audit"
	nsCtx := namespace.WithNamespace(context.Background(), namespace.Namespace(ns))

	check := func(t *testing.T, be *local.Backend, execID types.ExecutionID) {
		t.Helper()
		if !strings.HasPrefix(string(execID), "exec-ik-") {
			t.Fatalf("execution id = %q, want the key-derived exec-ik- id; a pre-allocated id is what the audit rows must not carry", execID)
		}
		if _, err := be.State().GetExecution(nsCtx, execID); err != nil {
			t.Fatalf("GetExecution(%q) error = %v — the audit rows name an execution that does not exist", execID, err)
		}
	}

	t.Run("inline", func(t *testing.T) {
		mux, be, audit := keyedAuditModule(t, ns)
		body := executeWorkflowRequest{Workflow: idempotencyInlineDef(), Params: map[string]any{"order": "o-1"}}

		first := executionData(t, doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, "audit-1"))
		if first.ExecutionID == "" || first.Duplicate {
			t.Fatalf("first keyed execute = %+v, want a new execution", first)
		}
		check(t, be, first.ExecutionID)
		requireKeyedRows(t, audit.Events(), first.ExecutionID)

		// A retry is the same request again: it is answered with the same
		// execution, and its own rows carry that same id — the derivation is
		// stable across attempts, not just on the one that created the execution.
		retry := executionData(t, doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, "audit-1"))
		if !retry.Duplicate || retry.ExecutionID != first.ExecutionID {
			t.Fatalf("retry = %+v, want the duplicate of %q", retry, first.ExecutionID)
		}
		events := audit.Events()
		if len(events) != 4 {
			t.Fatalf("audit events = %d, want 4 after the retry", len(events))
		}
		requireKeyedRows(t, events[2:], first.ExecutionID)
	})

	t.Run("by_id", func(t *testing.T) {
		mux, be, audit := keyedAuditModule(t, ns)

		// Register through the same authz-routed mux, so the definition and the
		// invocation live in the same verified namespace.
		regResp := doKeyed(t, mux, http.MethodPost, PathWorkflows, idempotencyInlineDef(), "")
		if regResp.StatusCode != http.StatusCreated {
			_ = regResp.Body.Close()
			t.Fatalf("register status = %d, want 201", regResp.StatusCode)
		}
		var reg registerWorkflowResponse
		decodeEnvelope(t, regResp, &reg)
		_ = regResp.Body.Close()
		if reg.WorkflowID == "" {
			t.Fatal("register returned an empty workflow id")
		}

		path := "/v1/workflows/" + string(reg.WorkflowID) + "/execute"
		body := executeRegisteredRequest{Params: map[string]any{"order": "o-1"}}
		first := executionData(t, doKeyed(t, mux, http.MethodPost, path, body, "audit-2"))
		if first.ExecutionID == "" || first.Duplicate {
			t.Fatalf("first keyed by-id execute = %+v, want a new execution", first)
		}
		check(t, be, first.ExecutionID)
		// The first two events are the registration's; the execute's follow.
		events := audit.Events()
		if len(events) != 4 {
			t.Fatalf("audit events = %d, want 4 (register + execute)", len(events))
		}
		requireKeyedRows(t, events[2:], first.ExecutionID)

		retry := executionData(t, doKeyed(t, mux, http.MethodPost, path, body, "audit-2"))
		if !retry.Duplicate || retry.ExecutionID != first.ExecutionID {
			t.Fatalf("retry = %+v, want the duplicate of %q", retry, first.ExecutionID)
		}
		events = audit.Events()
		if len(events) != 6 {
			t.Fatalf("audit events = %d, want 6 after the retry", len(events))
		}
		requireKeyedRows(t, events[4:], first.ExecutionID)
	})
}
