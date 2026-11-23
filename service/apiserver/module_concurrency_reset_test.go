package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// resetFacadeStub is a control.EngineFacade whose only live method is the reset
// capability the route needs. The embedded interface is nil, so any other
// method panics if the handler ever calls one — which is the point: this stub
// proves the route reaches exactly ResetWorkflowConcurrency.
type resetFacadeStub struct {
	control.EngineFacade
	res  engine.OrphanReclaimResult
	err  error
	call int
	ns   namespace.Namespace
	name string
	opts engine.ResetConcurrencyOptions
}

func (f *resetFacadeStub) ResetWorkflowConcurrency(_ context.Context, ns namespace.Namespace, workflowName string, opts engine.ResetConcurrencyOptions) (engine.OrphanReclaimResult, error) {
	f.call++
	f.ns = ns
	f.name = workflowName
	f.opts = opts
	return f.res, f.err
}

func newResetMux(eng control.EngineFacade) http.Handler {
	m := &workflowControlModule{eng: eng}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

// newAuthzResetMux mounts the reset route through the real authorization
// wrapper, which is the only mount that has a verified principal to put in the
// request context. The stub engine reaches exactly ResetWorkflowConcurrency, so
// a request the wrapper lets through cannot wander into another method.
func newAuthzResetMux(eng control.EngineFacade, log engine.Logger) http.Handler {
	m := &workflowControlModule{eng: eng, log: log}
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"execution"},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	return mux
}

// TestResetWorkflowConcurrencyDefaultsToDryRun pins the safe default: an absent
// body reaches the engine with Apply=false and the response says so. A caller
// that forgets the body must preview, never terminate work.
func TestResetWorkflowConcurrencyDefaultsToDryRun(t *testing.T) {
	f := &resetFacadeStub{res: engine.OrphanReclaimResult{
		Inspected: 1,
		Candidates: []engine.OrphanReclaimCandidate{{
			ExecutionID: "exec-1", Status: types.ExecutionStatusQueued, Age: 1500 * time.Millisecond,
			Evidence: engine.OrphanEvidenceGraphMissing,
		}},
	}}
	mux := newResetMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body concurrencyResetResponse
	env := decodeEnvelope(t, resp, &body)
	if !env.Success {
		t.Fatalf("envelope code = %q, want success", env.Code)
	}
	if f.call != 1 || f.opts.Apply || f.opts.IncludeLive {
		t.Fatalf("engine call = %d opts = %+v, want one dry-run call", f.call, f.opts)
	}
	if f.name != "wf-1" || f.ns != namespace.Default {
		t.Fatalf("engine saw name=%q ns=%q, want wf-1/default", f.name, f.ns)
	}
	if body.Applied || body.IncludeLive || body.Reclaimed != 0 {
		t.Fatalf("response = %+v, want a dry run with no reclaim", body)
	}
	if len(body.Candidates) != 1 {
		t.Fatalf("candidates = %+v, want one preview candidate", body.Candidates)
	}
	c := body.Candidates[0]
	if c.ExecutionID != "exec-1" || c.Status != types.ExecutionStatusQueued || c.AgeMS != 1500 || c.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("candidate = %+v, want the engine result mapped field for field", c)
	}
}

// TestResetWorkflowConcurrencyApplyIncludeLivePassesThrough pins the opt-ins:
// both booleans reach the engine, and the applied response is the engine's own
// count, not a re-derivation.
func TestResetWorkflowConcurrencyApplyIncludeLivePassesThrough(t *testing.T) {
	f := &resetFacadeStub{res: engine.OrphanReclaimResult{
		Inspected: 2, Reclaimed: 1,
		Candidates: []engine.OrphanReclaimCandidate{{
			ExecutionID: "exec-1", Workflow: "wf-1", Status: types.ExecutionStatusRunning,
			Age: 90 * time.Second, Evidence: engine.OrphanEvidenceGraphMissing,
		}},
	}}
	mux := newResetMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset",
		map[string]any{"apply": true, "include_live": true, "confirm_workflow": "wf-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body concurrencyResetResponse
	env := decodeEnvelope(t, resp, &body)
	if !env.Success {
		t.Fatalf("envelope code = %q, want success", env.Code)
	}
	if !f.opts.Apply || !f.opts.IncludeLive {
		t.Fatalf("engine opts = %+v, want apply and include_live both set", f.opts)
	}
	if !body.Applied || !body.IncludeLive || body.Inspected != 2 || body.Reclaimed != 1 {
		t.Fatalf("response = %+v, want the applied engine result", body)
	}
	if len(body.Candidates) != 1 || body.Candidates[0].Workflow != "wf-1" || body.Candidates[0].AgeMS != 90000 {
		t.Fatalf("candidates = %+v, want the engine candidate mapped", body.Candidates)
	}
}

// TestResetWorkflowConcurrencyLiveApplyRequiresConfirm pins the acknowledgement
// gated in front of the one request shape that can terminate executions whose
// graph still exists: apply + include_live. Both refusals are 400s with stable
// codes, and neither reaches the engine (the stub's call count stays zero), so
// a forgotten or misdirected confirmation can never converge anything.
func TestResetWorkflowConcurrencyLiveApplyRequiresConfirm(t *testing.T) {
	cases := []struct {
		name      string
		confirm   string
		sendEmpty bool
		wantCode  string
	}{
		{name: "absent", wantCode: "confirm_workflow_required"},
		{name: "explicitly empty", sendEmpty: true, wantCode: "confirm_workflow_required"},
		{name: "mismatch", confirm: "wf-2", wantCode: "confirm_workflow_mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &resetFacadeStub{}
			mux := newResetMux(f)
			body := map[string]any{"apply": true, "include_live": true}
			if tc.sendEmpty {
				body["confirm_workflow"] = ""
			} else if tc.confirm != "" {
				body["confirm_workflow"] = tc.confirm
			}
			resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset", body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if env := decodeEnvelope(t, resp, nil); env.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Code, tc.wantCode)
			}
			if f.call != 0 {
				t.Fatalf("engine called %d time(s) for a refused confirmation", f.call)
			}
		})
	}
}

// TestResetWorkflowConcurrencyConfirmationNotRequiredWithoutLiveApply pins the
// two shapes whose confirmation is deliberately not required: a preview writes
// nothing, and an apply without include_live stays inside the orphan judgement.
// Both must reach the engine with the flags they carried.
func TestResetWorkflowConcurrencyConfirmationNotRequiredWithoutLiveApply(t *testing.T) {
	cases := []struct {
		name      string
		body      map[string]any
		wantApply bool
		wantLive  bool
	}{
		{
			name:      "dry run with include_live",
			body:      map[string]any{"include_live": true},
			wantApply: false,
			wantLive:  true,
		},
		{
			name:      "apply without include_live",
			body:      map[string]any{"apply": true},
			wantApply: true,
			wantLive:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &resetFacadeStub{}
			mux := newResetMux(f)
			resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset", tc.body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if f.call != 1 || f.opts.Apply != tc.wantApply || f.opts.IncludeLive != tc.wantLive {
				t.Fatalf("engine call = %d opts = %+v, want apply=%v include_live=%v",
					f.call, f.opts, tc.wantApply, tc.wantLive)
			}
		})
	}
}

// TestResetWorkflowConcurrencyLogsPrincipalAndConfirmation covers the audit
// line's caller identity: the verified subject reaches the log, and a live
// apply is visible as confirmed. recordingLogger concatenates operands with
// fmt.Sprint, so "confirmedtrue" is the exact rendering of a true boolean.
func TestResetWorkflowConcurrencyLogsPrincipalAndConfirmation(t *testing.T) {
	log := &recordingLogger{}
	f := &resetFacadeStub{res: engine.OrphanReclaimResult{Inspected: 1, Reclaimed: 1}}
	mux := newAuthzResetMux(f, log)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset",
		map[string]any{"apply": true, "include_live": true, "confirm_workflow": "wf-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if f.ns != namespace.Namespace("namespaceA") {
		t.Fatalf("engine saw namespace %q, want the principal's namespaceA", f.ns)
	}
	if !log.mentionsAll("workflow_concurrency_reset", "principal", "op-a", "confirmedtrue") {
		t.Fatalf("audit line lacks principal op-a or confirmed=true: %v", log.all())
	}
}

// TestResetWorkflowConcurrencyLogsUnknownPrincipalWhenUnauthenticatedMount
// pins the fallback: the bare mount has no authenticated principal, and the
// audit line says unknown instead of dropping the field or inventing a caller.
func TestResetWorkflowConcurrencyLogsUnknownPrincipalWhenUnauthenticatedMount(t *testing.T) {
	log := &recordingLogger{}
	m := &workflowControlModule{eng: &resetFacadeStub{}, log: log}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset",
		map[string]any{"apply": true, "confirm_workflow": "wf-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !log.mentionsAll("workflow_concurrency_reset", "principalunknown", "confirmedfalse") {
		t.Fatalf("audit line lacks the unknown-principal fallback: %v", log.all())
	}
}

// TestResetWorkflowConcurrencyRefusals pins the stable failure shapes: invalid
// name 400, unsupported 501, and an internal failure 500 that leaks nothing
// from the store error.
func TestResetWorkflowConcurrencyRefusals(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		leak       string
	}{
		{
			name:       "invalid name",
			err:        fmt.Errorf("%w: must be between 1 and 128 characters", engine.ErrInvalidWorkflowName),
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_workflow_name",
		},
		{
			name:       "store cannot reclaim",
			err:        fmt.Errorf("wrapped: %w", engine.ErrConcurrencyOrphanReclaimUnsupported),
			wantStatus: http.StatusNotImplemented,
			wantCode:   "concurrency_reset_unsupported",
		},
		{
			name:       "internal failure",
			err:        errors.New("redis: dial tcp 10.0.0.7:6379: connection refused"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
			leak:       "10.0.0.7",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := newResetMux(&resetFacadeStub{err: tc.err})
			resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset",
				map[string]any{"apply": true})
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			var env envelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("decode envelope: %v (body=%s)", err, raw)
			}
			if env.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Code, tc.wantCode)
			}
			if tc.leak != "" && strings.Contains(string(raw), tc.leak) {
				t.Fatalf("response leaked %q: %s", tc.leak, raw)
			}
		})
	}
}

// TestResetWorkflowConcurrencyWithoutCapabilityAnswers501 pins the facade
// absence: an engine without the reset method (the existing fakeControlFacade)
// answers 501, the same operator-facing fact as a store without the capability.
func TestResetWorkflowConcurrencyWithoutCapabilityAnswers501(t *testing.T) {
	mux := newControlMux(&fakeControlFacade{})
	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/wf-1/concurrency/reset", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Code != "concurrency_reset_unsupported" {
		t.Fatalf("code = %q, want concurrency_reset_unsupported", env.Code)
	}
}

// TestResetWorkflowConcurrencyRejectsMalformedBody pins the shared body
// contract: malformed JSON is the same 400 bad_request every other route
// answers, not a 500 or a silent dry run.
func TestResetWorkflowConcurrencyRejectsMalformedBody(t *testing.T) {
	mux := newResetMux(&resetFacadeStub{})
	req, _ := http.NewRequest(http.MethodPost, "/v1/workflows/wf-1/concurrency/reset", strings.NewReader("{"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Code != "bad_request" {
		t.Fatalf("code = %q, want bad_request", env.Code)
	}
}
