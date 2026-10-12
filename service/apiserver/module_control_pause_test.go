package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// These tests cover the two dispatch-control endpoints (design §3): POST
// /v1/executions/{id}/pause and /resume. The store-level contract for what a
// pause holds lives in backend/internal/statestoretest (both backends); what is
// pinned here is the HTTP verdict for each operator-visible state, the view the
// caller gets back, and the namespace boundary the id must cross.

// TestExecutionPauseResumeEndpoints walks one execution through the control
// state machine over HTTP and asserts the view each call answers with.
//
// The two 409s are the point of the table: a repeated RESUME is refused
// (execution_not_paused) while a repeated PAUSE is not — the store treats a
// second pause as an idempotent success, because a retried request whose
// response was lost must not read as a conflict. Cancelling first, then pausing,
// is the other 409: dispatch control is for a live execution, and a cancelled
// one has no dispatch left to hold.
func TestExecutionPauseResumeEndpoints(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)

	inspect := func() engine.ExecutionDetail {
		t.Helper()
		resp := f.do("tok-full", http.MethodGet, path, nil)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("inspect status = %d, want 200", resp.StatusCode)
		}
		var detail engine.ExecutionDetail
		decodeEnvelope(t, resp, &detail)
		return detail
	}

	if got := inspect().Status; got != types.ExecutionStatusRunning {
		t.Fatalf("seeded execution status = %q, want running", got)
	}

	resp := f.do("tok-full", http.MethodPost, path+"/pause", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause status = %d, want 200", resp.StatusCode)
	}
	var paused engine.ExecutionDetail
	decodeEnvelope(t, resp, &paused)
	_ = resp.Body.Close()
	if paused.Status != types.ExecutionStatusPaused {
		t.Fatalf("pause view status = %q, want paused: the response must show the transition, "+
			"not the state before it", paused.Status)
	}
	if paused.ExecutionID != f.execID {
		t.Fatalf("pause view execution_id = %q, want %q", paused.ExecutionID, f.execID)
	}

	// A repeated pause is the store's idempotent success, not a conflict.
	resp = f.do("tok-full", http.MethodPost, path+"/pause", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeated pause status = %d, want 200 (idempotent)", resp.StatusCode)
	}

	resp = f.do("tok-full", http.MethodPost, path+"/resume", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume status = %d, want 200", resp.StatusCode)
	}
	var resumed engine.ExecutionDetail
	decodeEnvelope(t, resp, &resumed)
	_ = resp.Body.Close()
	if resumed.Status != types.ExecutionStatusRunning {
		t.Fatalf("resume view status = %q, want running", resumed.Status)
	}

	resp = f.do("tok-full", http.MethodPost, path+"/resume", nil)
	code := decodeEnvelope(t, resp, nil).Code
	if resp.StatusCode != http.StatusConflict || code != "execution_not_paused" {
		t.Fatalf("repeated resume = %d/%s, want 409/execution_not_paused", resp.StatusCode, code)
	}

	// A cancelled execution refuses the pause for the same reason a queued one
	// does: there is no running dispatch to hold.
	resp = f.do("tok-full", http.MethodPost, path+"/cancel", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}
	resp = f.do("tok-full", http.MethodPost, path+"/pause", nil)
	code = decodeEnvelope(t, resp, nil).Code
	if resp.StatusCode != http.StatusConflict || code != "execution_not_pausable" {
		t.Fatalf("pause of a cancelled execution = %d/%s, want 409/execution_not_pausable",
			resp.StatusCode, code)
	}

	// An absent execution is 404, never 409: a typo'd id must not read as a live
	// execution that refuses to pause.
	resp = f.do("tok-full", http.MethodPost, "/v1/executions/exec-does-not-exist/pause", nil)
	code = decodeEnvelope(t, resp, nil).Code
	if resp.StatusCode != http.StatusNotFound || code != "execution_not_found" {
		t.Fatalf("pause of an absent execution = %d/%s, want 404/execution_not_found",
			resp.StatusCode, code)
	}
	resp = f.do("tok-full", http.MethodPost, "/v1/executions/exec-does-not-exist/resume", nil)
	code = decodeEnvelope(t, resp, nil).Code
	if resp.StatusCode != http.StatusNotFound || code != "execution_not_found" {
		t.Fatalf("resume of an absent execution = %d/%s, want 404/execution_not_found",
			resp.StatusCode, code)
	}
}

// TestExecutionPauseIsScopedByControlScope proves the control scope is enforced
// on both endpoints with the same default-deny shape as every other execution
// mutation: tok-none carries the "execution" scope (it may inspect, signal and
// cancel) and is still refused, because holding a pipeline is a separate grant.
func TestExecutionPauseIsScopedByControlScope(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)

	for _, route := range []string{"/pause", "/resume"} {
		resp := f.do("tok-none", http.MethodPost, path+route, nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("POST %s with only the execution scope = %d, want 403", route, resp.StatusCode)
		}
	}
	// The refusal is a scope decision, not a state side effect: the execution is
	// untouched and still running.
	resp := f.do("tok-full", http.MethodGet, path, nil)
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	decodeEnvelope(t, resp, &detail)
	if detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("status after the refused calls = %q, want running", detail.Status)
	}
}

// TestExecutionPauseCrossNamespaceIsNotFound proves the new endpoints inherit
// the namespace boundary of the id they carry: a principal of another namespace
// holding the control scope gets 404 (not 403) and the execution keeps running.
//
// 404 rather than 403 is the deliberate shape documented on execIDResolver: the
// authorizer cannot resolve an execution's namespace without a store lookup, so
// the namespace-scoped store read is the authoritative IDOR defense — a foreign
// id resolves to not-found, which leaks nothing about whether it exists.
func TestExecutionPauseCrossNamespaceIsNotFound(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)

	for _, route := range []string{"/pause", "/resume"} {
		resp := f.do("tok-other", http.MethodPost, path+route, nil)
		code := decodeEnvelope(t, resp, nil).Code
		if resp.StatusCode != http.StatusNotFound || code != "execution_not_found" {
			t.Fatalf("POST %s across namespaces = %d/%s, want 404/execution_not_found",
				route, resp.StatusCode, code)
		}
	}

	resp := f.do("tok-full", http.MethodGet, path, nil)
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	decodeEnvelope(t, resp, &detail)
	if detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("status after the refused cross-namespace calls = %q, want running", detail.Status)
	}
}

// noControlFacade is a facade with the runner-protocol surface but no dispatch
// control: embedding the interface satisfies every method the module does not
// reach, and the absence of Pause/Resume is exactly what this test is about. It
// is the shape a custom EmbeddingFacade integration has.
type noControlFacade struct{ control.EngineFacade }

// TestExecutionControlWithoutTheCapabilityIs501 pins the answer for a
// deployment that cannot suspend anything: 501 execution_control_unsupported,
// the same code the engine's store-level ErrPauseUnsupported maps to. The
// capability is checked before the execution is looked up, so a store that
// cannot pause refuses in a way that does not depend on which id was asked
// about — including an id that does not exist.
func TestExecutionControlWithoutTheCapabilityIs501(t *testing.T) {
	m := &workflowControlModule{eng: noControlFacade{}}

	for name, call := range map[string]func(w http.ResponseWriter, r *http.Request, id types.ExecutionID){
		"pause":  m.handlePause,
		"resume": m.handleResume,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-any/"+name, nil)
			call(rec, req, types.ExecutionID("exec-any"))

			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501", rec.Code)
			}
			if got := envelopeCode(t, rec); got != "execution_control_unsupported" {
				t.Fatalf("code = %q, want execution_control_unsupported", got)
			}
		})
	}
}

// TestExecutionControlMethodMismatchIsRejected pins requireMethod on the two new
// routes: the handlers are also reachable directly (the legacy bare branch
// adapters call them), so a GET must not be treated as a pause.
//
// The facade deliberately has no control capability, so the 405 proves the
// method check runs BEFORE the capability lookup: if the capability were
// resolved first this would be 501.
func TestExecutionControlMethodMismatchIsRejected(t *testing.T) {
	m := &workflowControlModule{eng: noControlFacade{}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/executions/exec-any/pause", nil)
	m.handlePause(rec, req, types.ExecutionID("exec-any"))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET pause status = %d, want 405", rec.Code)
	}
}
