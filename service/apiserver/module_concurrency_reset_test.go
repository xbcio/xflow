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
		map[string]any{"apply": true, "include_live": true})
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
