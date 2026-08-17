package apiserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestManagementAndExecutionInspectAgree is the spec §7.2 drift guard. The two
// inspect routes — GET /v1/executions/{id} (OpExecutionRead) and
// GET /v1/management/executions/{id} (OpManagementRead) — both call eng.Inspect
// and MUST return byte-identical bodies for the same execution. They had drifted
// once (management emitted a bare writeJSON; control emitted the writeData
// envelope), and this test would have caught it.
//
// The assertion is byte-identity, not "both 200": a test that only checks status
// proves nothing. Both bodies are decoded as envelopes afterwards so a regression
// where BOTH drift to the same bare shape cannot pass bytes.Equal vacuously.
//
// Both Ops stay distinct (spec §7.2: ops vs business token may be granted
// separately); only the implementation is merged.
func TestManagementAndExecutionInspectAgree(t *testing.T) {
	detail := engine.ExecutionDetail{
		ExecutionID: "exec-1",
		Status:      types.ExecutionStatusRunning,
	}
	f := &fakeControlFacade{inspect: detail}

	// Both modules share the same engine facade so the same execution resolves
	// identically. principalAuth==nil selects the bare registration branch in
	// each module (management: direct handler; control: DisabledWorkflowAuth),
	// which is the branch that previously diverged.
	mgmt := &managementModule{eng: f}
	ctrl := &workflowControlModule{eng: f}
	mux := http.NewServeMux()
	mgmt.RegisterHTTP(mux)
	ctrl.RegisterHTTP(mux)

	// Found case: both 200, identical envelopes carrying the detail.
	execBody := inspectRoute(t, mux, "/v1/executions/exec-1", http.StatusOK)
	mgmtBody := inspectRoute(t, mux, "/v1/management/executions/exec-1", http.StatusOK)
	if !bytes.Equal(execBody, mgmtBody) {
		t.Fatalf("found-case bodies differ (drift):\n  executions:  %s\n  management:  %s", execBody, mgmtBody)
	}
	var found envelope
	if err := json.Unmarshal(execBody, &found); err != nil {
		t.Fatalf("found body is not an envelope: %v (%s)", err, execBody)
	}
	if !found.Success || found.Code != "200" {
		t.Fatalf("found envelope = %+v, want success=true code=200", found)
	}

	// Not-found case: the management side previously used writeEngineError
	// (writeError shim → code "not_found", empty trace_id) while the control
	// side used writeExecEngineFail (code "execution_not_found"). They MUST now
	// agree. This is the drift the merge closes.
	f.inspectErr = engine.ErrExecutionNotFound
	execNF := inspectRoute(t, mux, "/v1/executions/missing", http.StatusNotFound)
	mgmtNF := inspectRoute(t, mux, "/v1/management/executions/missing", http.StatusNotFound)
	if !bytes.Equal(execNF, mgmtNF) {
		t.Fatalf("not-found-case bodies differ (drift):\n  executions:  %s\n  management:  %s", execNF, mgmtNF)
	}
	var nfEnv envelope
	if err := json.Unmarshal(execNF, &nfEnv); err != nil {
		t.Fatalf("not-found body is not an envelope: %v (%s)", err, execNF)
	}
	if nfEnv.Success || nfEnv.Code != "execution_not_found" {
		t.Fatalf("not-found envelope = %+v, want success=false code=execution_not_found", nfEnv)
	}
}

// inspectRoute issues a GET and returns the response body with the json encoder
// trailing newline trimmed so byte comparisons are exact. The same X-Request-Id
// is sent on both routes; it is echoed in the response HEADER (not the body),
// so it does not perturb the body comparison.
func inspectRoute(t *testing.T, mux http.Handler, path string, wantStatus int) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Request-Id", "req-inspect-agree")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("%s: status = %d, want %d, body=%s", path, rec.Code, wantStatus, rec.Body.String())
	}
	return bytes.TrimRight(rec.Body.Bytes(), "\n")
}
