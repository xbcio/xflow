package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// doKeyed sends a JSON request with an Idempotency-Key header. A key of "" sends
// none, so an unkeyed request and a keyed one go through the same helper.
func doKeyed(t *testing.T, mux http.Handler, method, path string, body any, key string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

// idempotencyTestMux wires the control module over a REAL engine on the local
// backend: what these tests are about is the route's behaviour against the claim
// the backend actually holds, not a scripted facade.
func idempotencyTestMux(t *testing.T) (http.Handler, *local.Backend) {
	t.Helper()
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := &workflowControlModule{cp: cp, eng: cp.Engine()}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux, be
}

// idempotencyInlineDef is the definition the inline route compiles and runs: one
// entry, so no node handler is ever needed.
func idempotencyInlineDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "idem-wf",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
}

func executionData(t *testing.T, resp *http.Response) executeWorkflowResponse {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var out executeWorkflowResponse
	decodeEnvelope(t, resp, &out)
	return out
}

// TestExecuteWorkflowIdempotencyKey pins the inline route's keyed contract end
// to end: a retry of the same request answers with the execution the first
// attempt created and duplicate:true, the same key with different content is a
// conflict, and an unkeyed request keeps behaving exactly as before.
func TestExecuteWorkflowIdempotencyKey(t *testing.T) {
	mux, state := idempotencyTestMux(t)
	body := executeWorkflowRequest{Workflow: idempotencyInlineDef(), Params: map[string]any{"order": "o-1"}}

	first := executionData(t, doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, "order-1"))
	if first.ExecutionID == "" {
		t.Fatal("first keyed execute returned no execution id")
	}
	if first.Duplicate {
		t.Fatal("first keyed execute reported duplicate")
	}
	if first.Status == "" {
		t.Fatal("first keyed execute returned no status")
	}

	// The retry is the same request sent again — same body, same key.
	retry := executionData(t, doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, "order-1"))
	if !retry.Duplicate {
		t.Fatal("retry of a keyed execute did not report duplicate")
	}
	if retry.ExecutionID != first.ExecutionID {
		t.Fatalf("retry execution_id = %q, want the original %q", retry.ExecutionID, first.ExecutionID)
	}
	if retry.Status == "" {
		t.Fatal("duplicate answer carried no status: the caller cannot tell queued from running")
	}
	if snap, err := state.State().GetExecution(context.Background(), first.ExecutionID); err != nil || snap == nil {
		t.Fatalf("keyed execution not persisted (%+v, %v)", snap, err)
	}

	// The same key with different params is a conflict, not a duplicate: it is
	// not the request the key was used for.
	conflict := doKeyed(t, mux, http.MethodPost, PathWorkflowExecute,
		executeWorkflowRequest{Workflow: idempotencyInlineDef(), Params: map[string]any{"order": "o-2"}}, "order-1")
	if conflict.StatusCode != http.StatusConflict {
		_ = conflict.Body.Close()
		t.Fatalf("reused key with different params = %d, want 409", conflict.StatusCode)
	}
	if env := decodeEnvelope(t, conflict, nil); env.Code != "idempotency_key_conflict" {
		t.Fatalf("code = %q, want idempotency_key_conflict", env.Code)
	}
	_ = conflict.Body.Close()

	// An unkeyed request is untouched by all of this: it starts a new execution
	// every time, which is what the route did before the header existed.
	unkeyed := executionData(t, doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, ""))
	if unkeyed.Duplicate || unkeyed.ExecutionID == "" || unkeyed.ExecutionID == first.ExecutionID {
		t.Fatalf("unkeyed execute = %+v, want a fresh execution", unkeyed)
	}
}

// TestExecuteWorkflowIdempotencyKeyInvalid: a key outside the accepted shape is
// refused rather than dropped. Silently ignoring it would answer "accepted" to a
// request whose retry the server cannot recognize.
func TestExecuteWorkflowIdempotencyKeyInvalid(t *testing.T) {
	mux, _ := idempotencyTestMux(t)
	body := executeWorkflowRequest{Workflow: idempotencyInlineDef()}
	for _, key := range []string{"has space", "sla/sh", string(bytes.Repeat([]byte("k"), 256))} {
		resp := doKeyed(t, mux, http.MethodPost, PathWorkflowExecute, body, key)
		if resp.StatusCode != http.StatusBadRequest {
			_ = resp.Body.Close()
			t.Fatalf("key %q status = %d, want 400", key, resp.StatusCode)
		}
		if env := decodeEnvelope(t, resp, nil); env.Code != "idempotency_key_invalid" {
			t.Fatalf("key %q code = %q, want idempotency_key_invalid", key, env.Code)
		}
		_ = resp.Body.Close()
	}
}

// TestExecuteRegisteredWorkflowIdempotencyKey is the by-id route's half: the key
// is scoped to the registered workflow, so the retry finds the execution the
// first attempt created.
func TestExecuteRegisteredWorkflowIdempotencyKey(t *testing.T) {
	mux, state := idempotencyTestMux(t)
	def := idempotencyInlineDef()
	registerSeedWorkflowDef(t, state, def)

	path := "/v1/workflows/" + def.Name + "/execute"
	body := executeRegisteredRequest{Params: map[string]any{"order": "o-1"}}

	first := executionData(t, doKeyed(t, mux, http.MethodPost, path, body, "biz-7"))
	if first.ExecutionID == "" || first.Duplicate {
		t.Fatalf("first keyed by-id execute = %+v, want a new execution", first)
	}
	retry := executionData(t, doKeyed(t, mux, http.MethodPost, path, body, "biz-7"))
	if !retry.Duplicate || retry.ExecutionID != first.ExecutionID {
		t.Fatalf("retry = %+v, want duplicate of %q", retry, first.ExecutionID)
	}
	if retry.Status == "" {
		t.Fatal("duplicate answer carried no status")
	}
}

// TestConcurrentKeyedExecutesCreateOneExecution: sixteen concurrent requests with
// one key create exactly one execution. The losers are refused as in-progress —
// retryable — rather than silently creating fifteen more.
func TestConcurrentKeyedExecutesCreateOneExecution(t *testing.T) {
	mux, state := idempotencyTestMux(t)
	body := executeWorkflowRequest{Workflow: idempotencyInlineDef()}

	const n = 16
	// The goroutines only collect; every assertion and decode happens after the
	// wait, because t.Fatalf from a non-test goroutine cannot be honored.
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, PathWorkflowExecute,
				bytes.NewReader(encodeJSON(t, body)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(IdempotencyKeyHeader, "concurrent-1")
			mux.ServeHTTP(rec, req)
			resp := rec.Result()
			defer func() { _ = resp.Body.Close() }()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}
			statuses[i], bodies[i] = resp.StatusCode, raw
		}(i)
	}
	wg.Wait()

	created := 0
	var want types.ExecutionID
	inProgress := 0
	for i := range statuses {
		var env struct {
			Code string                  `json:"code"`
			Data executeWorkflowResponse `json:"data"`
		}
		if len(bodies[i]) > 0 {
			if err := json.Unmarshal(bodies[i], &env); err != nil {
				t.Fatalf("request %d body %s: %v", i, bodies[i], err)
			}
		}
		switch {
		case statuses[i] == http.StatusOK && !env.Data.Duplicate:
			created++
			want = env.Data.ExecutionID
		case statuses[i] == http.StatusOK && env.Data.Duplicate:
			if want != "" && env.Data.ExecutionID != want {
				t.Fatalf("request %d answered with %q, want the created %q", i, env.Data.ExecutionID, want)
			}
		case statuses[i] == http.StatusConflict && env.Code == "idempotency_key_in_progress":
			inProgress++
		default:
			t.Fatalf("request %d = %d/%s (id %q), want 200 or 409 in-progress", i, statuses[i], env.Code, env.Data.ExecutionID)
		}
	}
	if created != 1 {
		t.Fatalf("%d of %d concurrent keyed executes created an execution, want exactly 1 (%d in progress)", created, n, inProgress)
	}
	if _, err := state.State().GetExecution(context.Background(), want); err != nil {
		t.Fatalf("GetExecution(%q) error = %v", want, err)
	}
}

func encodeJSON(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestIdempotencyFailuresMapToStableCodes drives the refusal mapping the execute
// routes share. The two 409s are distinct on purpose: a conflict is a caller bug
// retrying cannot fix, whereas an in-progress claim belongs to a request with the
// same key and the same fingerprint that is still working, and that answer
// carries a Retry-After.
func TestIdempotencyFailuresMapToStableCodes(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		want      int
		wantCode  string
		retryable bool
	}{
		{"conflict", engine.ErrIdempotencyConflict, http.StatusConflict, "idempotency_key_conflict", false},
		{"in progress", engine.ErrIdempotencyInProgress, http.StatusConflict, "idempotency_key_in_progress", true},
		{"unsupported", engine.ErrIdempotencyUnsupported, http.StatusServiceUnavailable, "idempotency_unsupported", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := newControlMux(&fakeControlFacade{submitErr: tc.err})
			resp := doKeyed(t, mux, http.MethodPost, PathWorkflowExecute,
				executeWorkflowRequest{Workflow: validWorkflow()}, "some-key")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if env := decodeEnvelope(t, resp, nil); env.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Code, tc.wantCode)
			}
			if got := resp.Header.Get("Retry-After"); (got != "") != tc.retryable {
				t.Fatalf("Retry-After = %q, want retryable=%v", got, tc.retryable)
			}
		})
	}
}

// TestRerunIdempotencyKeyReachesTheEngineAndTheResponse pins both halves of the
// rerun route's keyed behaviour: the header becomes the engine's IdempotencyKey,
// and an engine-reported duplicate is what the caller reads.
func TestRerunIdempotencyKeyReachesTheEngineAndTheResponse(t *testing.T) {
	fake := &fakeControlFacade{rerunRes: engine.RerunResult{ExecutionID: "exec-rerun", Duplicate: true}}
	m := &workflowControlModule{eng: fake}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	resp := doKeyed(t, mux, http.MethodPost, "/v1/executions/exec-parent/rerun",
		rerunRequest{FromNode: "start"}, "rerun-1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fake.rerunReq.IdempotencyKey != "rerun-1" {
		t.Fatalf("engine saw IdempotencyKey = %q, want rerun-1", fake.rerunReq.IdempotencyKey)
	}
	var body rerunExecutionResponse
	decodeEnvelope(t, resp, &body)
	if !body.Duplicate || body.ExecutionID != "exec-rerun" {
		t.Fatalf("rerun response = %+v, want the duplicate of exec-rerun", body)
	}

	// A rerun without the header is the unkeyed rerun it always was.
	fake.rerunRes = engine.RerunResult{ExecutionID: "exec-fresh"}
	plain := doKeyed(t, mux, http.MethodPost, "/v1/executions/exec-parent/rerun",
		rerunRequest{FromNode: "start"}, "")
	defer func() { _ = plain.Body.Close() }()
	if plain.StatusCode != http.StatusOK {
		t.Fatalf("unkeyed rerun status = %d, want 200", plain.StatusCode)
	}
	if fake.rerunReq.IdempotencyKey != "" {
		t.Fatalf("unkeyed rerun carried IdempotencyKey = %q, want none", fake.rerunReq.IdempotencyKey)
	}
	var plainBody rerunExecutionResponse
	decodeEnvelope(t, plain, &plainBody)
	if plainBody.Duplicate {
		t.Fatal("unkeyed rerun reported duplicate")
	}
}

// TestSubmitFailureMappingIsShared: every refusal the engine can raise for a
// submission reaches the caller through one mapper, so a route cannot answer
// internal_error for a refusal another route names. These are the pre-existing
// cases the shared mapper must keep intact.
func TestSubmitFailureMappingIsShared(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		want     int
		wantCode string
	}{
		{"invalid params", graph.ErrInvalidParams, http.StatusBadRequest, "workflow_params_invalid"},
		{"entry not found", engine.ErrEntryNotFound, http.StatusBadRequest, "workflow_entry_not_found"},
		{"faf unsupported", engine.ErrFAFRequiresDirectDispatch, http.StatusBadRequest, "workflow_faf_unsupported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := newControlMux(&fakeControlFacade{invokeErr: tc.err})
			resp := doKeyed(t, mux, http.MethodPost, PathWorkflowExecute,
				executeWorkflowRequest{Workflow: validWorkflow(), Entry: "start"}, "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if env := decodeEnvelope(t, resp, nil); env.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Code, tc.wantCode)
			}
		})
	}
}
