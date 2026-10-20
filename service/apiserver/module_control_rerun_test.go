package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// jsonBody encodes a request body the way a real client would send it.
func jsonBody(t *testing.T, v any) *bytes.Reader {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	return bytes.NewReader(buf.Bytes())
}

// rerunData decodes a success envelope's data into a rerun response.
func rerunData(t *testing.T, rec *httptest.ResponseRecorder) rerunExecutionResponse {
	t.Helper()
	var out rerunExecutionResponse
	if err := json.Unmarshal(envelopeData(t, rec), &out); err != nil {
		t.Fatalf("decode rerun data: %v (body=%s)", err, rec.Body.String())
	}
	return out
}

// These tests cover POST /v1/executions/{id}/rerun (design §3): derive a NEW
// execution from a terminal one, starting at a named node. What the engine
// decides (the ancestor closure, the admission, never touching the parent) is
// pinned in engine/rerun_test.go and the two backends' seed contracts; what is
// pinned here is the HTTP verdict for each operator-visible state, the body
// mapping, and the scope the operation needs.

// TestExecutionRerunEndpoint walks a real execution through the endpoint: a
// live one refuses, a terminal one reruns into a new execution that carries the
// parent link, and the parent is left exactly as the rerun found it.
func TestExecutionRerunEndpoint(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)
	rerunPath := path + "/rerun"

	rerun := func(token string, body any) (int, string, rerunExecutionResponse) {
		t.Helper()
		resp := f.do(token, http.MethodPost, rerunPath, body)
		var out rerunExecutionResponse
		env := decodeEnvelope(t, resp, &out)
		_ = resp.Body.Close()
		return resp.StatusCode, env.Code, out
	}
	inspect := func(id types.ExecutionID) engine.ExecutionDetail {
		t.Helper()
		resp := f.do("tok-full", http.MethodGet, "/v1/executions/"+string(id), nil)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("inspect %s status = %d, want 200", id, resp.StatusCode)
		}
		var detail engine.ExecutionDetail
		decodeEnvelope(t, resp, &detail)
		return detail
	}

	if got := inspect(f.execID).Status; got != types.ExecutionStatusRunning {
		t.Fatalf("seeded execution status = %q, want running", got)
	}

	// A live execution is not rerunnable: it is allowed to finish (or paused
	// and cancelled), and deriving from a moving target would copy a prefix that
	// is still being written.
	status, code, _ := rerun("tok-full", rerunRequest{FromNode: "start"})
	if status != http.StatusConflict || code != "execution_not_rerunnable" {
		t.Fatalf("rerun of a running execution = %d/%s, want 409/execution_not_rerunnable", status, code)
	}

	// The status gate runs before the graph is consulted, so a bad node name on
	// a live execution reports the status, not the name: the caller's first
	// problem is that this execution cannot be rerun at all.
	status, code, _ = rerun("tok-full", rerunRequest{FromNode: "no-such-node"})
	if status != http.StatusConflict || code != "execution_not_rerunnable" {
		t.Fatalf("rerun from a bad node on a running execution = %d/%s, want 409/execution_not_rerunnable", status, code)
	}

	// The body is required and from_node is its one mandatory field: defaulting
	// a rerun point would silently re-run the whole workflow.
	status, code, _ = rerun("tok-full", rerunRequest{})
	if status != http.StatusBadRequest || code != "from_node_required" {
		t.Fatalf("rerun without from_node = %d/%s, want 400/from_node_required", status, code)
	}

	resp := f.do("tok-full", http.MethodPost, path+"/cancel", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}

	status, code, _ = rerun("tok-full", rerunRequest{FromNode: "no-such-node"})
	if status != http.StatusBadRequest || code != "rerun_node_not_found" {
		t.Fatalf("rerun from an unknown node = %d/%s, want 400/rerun_node_not_found", status, code)
	}

	status, code, out := rerun("tok-full", rerunRequest{FromNode: "start"})
	if status != http.StatusOK {
		t.Fatalf("rerun status = %d/%s, want 200", status, code)
	}
	if out.ExecutionID == "" || out.ExecutionID == f.execID {
		t.Fatalf("rerun execution_id = %q, want a NEW id distinct from the parent %q", out.ExecutionID, f.execID)
	}
	if out.QueuePosition != 0 {
		t.Fatalf("queue_position = %d, want 0: this graph declares no concurrency limit", out.QueuePosition)
	}

	// The new execution is the answer the caller acts on, so it must be
	// inspectable and must carry the link back to what it derives from.
	child := inspect(out.ExecutionID)
	if child.ParentID != f.execID || child.RerunFrom != "start" {
		t.Fatalf("rerun child parent/rerun_from = %q/%q, want %q/start",
			child.ParentID, child.RerunFrom, f.execID)
	}

	// The parent is untouched: rerun creates rather than rewrites, which is what
	// keeps its audit trail and any in-flight lease unambiguous.
	parent := inspect(f.execID)
	if parent.Status != types.ExecutionStatusCanceled {
		t.Fatalf("parent status after rerun = %q, want canceled (the parent must not be modified)", parent.Status)
	}
	if parent.ParentID != "" || parent.RerunFrom != "" {
		t.Fatalf("parent gained a rerun link = %q/%q, want empty", parent.ParentID, parent.RerunFrom)
	}

	// An absent execution is 404, never 409: a typo'd id must not read as an
	// execution that refuses to be rerun.
	status, code, _ = rerunOn(t, f, "/v1/executions/exec-does-not-exist/rerun", rerunRequest{FromNode: "start"})
	if status != http.StatusNotFound || code != "execution_not_found" {
		t.Fatalf("rerun of an absent execution = %d/%s, want 404/execution_not_found", status, code)
	}
}

// rerunOn posts a rerun body to an arbitrary path. It exists so the absent-id
// case reuses the decode helper without a second closure over a different path.
func rerunOn(t *testing.T, f *execRouteFixture, path string, body any) (int, string, rerunExecutionResponse) {
	t.Helper()
	resp := f.do("tok-full", http.MethodPost, path, body)
	var out rerunExecutionResponse
	env := decodeEnvelope(t, resp, &out)
	_ = resp.Body.Close()
	return resp.StatusCode, env.Code, out
}

// TestExecutionRerunRefusalsAreDistinct maps each engine verdict onto its HTTP
// shape. The refusals are four different operator mistakes — the wrong source
// status, an unknown node, a graph that cannot be seeded, and a missing
// upstream result — and one code for all of them would tell the caller to fix
// the wrong thing.
func TestExecutionRerunRefusalsAreDistinct(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		want     int
		wantCode string
	}{
		{"not rerunnable", engine.ErrExecutionNotRerunnable, http.StatusConflict, "execution_not_rerunnable"},
		{"unknown node", engine.ErrRerunNodeNotFound, http.StatusBadRequest, "rerun_node_not_found"},
		{"graph unsupported", engine.ErrRerunGraphUnsupported, http.StatusConflict, "rerun_graph_unsupported"},
		{"upstream incomplete", engine.ErrRerunUpstreamIncomplete, http.StatusConflict, "rerun_upstream_incomplete"},
		{"engine unsupported", engine.ErrRerunUnsupported, http.StatusNotImplemented, "execution_rerun_unsupported"},
		{"concurrency limit", engine.ErrConcurrencyLimit, http.StatusTooManyRequests, "workflow_concurrency_limit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// errors.Is is what the handler uses, so the sentinel must survive
			// the engine's wrapping — which it does today, and this pins it.
			fake := &fakeControlFacade{rerunErr: c.err}
			m := &workflowControlModule{eng: fake}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-1/rerun",
				jsonBody(t, rerunRequest{FromNode: "start"}))
			m.handleRerun(rec, req, types.ExecutionID("exec-1"))

			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d (body=%s)", rec.Code, c.want, rec.Body.String())
			}
			if got := envelopeCode(t, rec); got != c.wantCode {
				t.Fatalf("code = %q, want %q", got, c.wantCode)
			}
		})
	}
}

// TestExecutionRerunBodyReachesTheEngine pins the request mapping: the path id
// is the source execution, and both body fields reach the engine unchanged.
// include_success is the one that can only be observed here — a failed,
// canceled or timeout source is accepted either way, so an inverted or dropped
// flag would look correct against every source but a successful one.
func TestExecutionRerunBodyReachesTheEngine(t *testing.T) {
	fake := &fakeControlFacade{
		rerunRes: engine.RerunResult{ExecutionID: "exec-child"},
		inspect:  engine.ExecutionDetail{ExecutionID: "exec-child", Status: types.ExecutionStatusRunning},
	}
	m := &workflowControlModule{eng: fake}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-parent/rerun",
		jsonBody(t, rerunRequest{FromNode: "n2", IncludeSuccess: true}))
	m.handleRerun(rec, req, types.ExecutionID("exec-parent"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if fake.rerunReq.ExecutionID != "exec-parent" {
		t.Fatalf("engine saw execution_id %q, want exec-parent (the path id)", fake.rerunReq.ExecutionID)
	}
	if fake.rerunReq.FromNode != "n2" {
		t.Fatalf("engine saw from_node %q, want n2", fake.rerunReq.FromNode)
	}
	if !fake.rerunReq.IncludeSuccess {
		t.Fatal("engine saw include_success=false, want true: a successful source is only accepted when the caller asks")
	}

	out := rerunData(t, rec)
	if out.ExecutionID != "exec-child" {
		t.Fatalf("response execution_id = %q, want the engine's new execution exec-child", out.ExecutionID)
	}
}

// TestExecutionRerunReportsTheQueuePosition pins the two-field answer for a
// parked execution. The position is only reported while the execution is still
// queued: once it has been promoted the position names a place it has already
// left, and reporting it would invite a caller to wait on a line it is no
// longer in.
func TestExecutionRerunReportsTheQueuePosition(t *testing.T) {
	cases := []struct {
		name         string
		res          engine.RerunResult
		inspected    types.ExecutionStatus
		wantStatus   types.ExecutionStatus
		wantQueuePos int
	}{
		{
			name:         "parked",
			res:          engine.RerunResult{ExecutionID: "exec-q", Queued: true, QueuePosition: 3},
			inspected:    types.ExecutionStatusQueued,
			wantStatus:   types.ExecutionStatusQueued,
			wantQueuePos: 3,
		},
		{
			name:         "promoted while the response was built",
			res:          engine.RerunResult{ExecutionID: "exec-q", Queued: true, QueuePosition: 3},
			inspected:    types.ExecutionStatusRunning,
			wantStatus:   types.ExecutionStatusRunning,
			wantQueuePos: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeControlFacade{
				rerunRes: c.res,
				inspect:  engine.ExecutionDetail{ExecutionID: c.res.ExecutionID, Status: c.inspected},
			}
			m := &workflowControlModule{eng: fake}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-1/rerun",
				jsonBody(t, rerunRequest{FromNode: "start"}))
			m.handleRerun(rec, req, types.ExecutionID("exec-1"))

			out := rerunData(t, rec)
			if out.Status != c.wantStatus || out.QueuePosition != c.wantQueuePos {
				t.Fatalf("status/queue_position = %q/%d, want %q/%d",
					out.Status, out.QueuePosition, c.wantStatus, c.wantQueuePos)
			}
		})
	}
}

// TestExecutionRerunWithoutTheCapabilityIs501 pins the answer for a deployment
// whose facade cannot seed an execution: 501 execution_rerun_unsupported. The
// capability is resolved before anything else, so the refusal does not depend
// on which id was asked about.
func TestExecutionRerunWithoutTheCapabilityIs501(t *testing.T) {
	m := &workflowControlModule{eng: noControlFacade{}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-any/rerun",
		jsonBody(t, rerunRequest{FromNode: "start"}))
	m.handleRerun(rec, req, types.ExecutionID("exec-any"))

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
	if got := envelopeCode(t, rec); got != "execution_rerun_unsupported" {
		t.Fatalf("code = %q, want execution_rerun_unsupported", got)
	}
}

// TestExecutionRerunMethodMismatchIsRejected pins requireMethod on the new
// route: the handler is also reachable directly (the bare branch adapters call
// it), so a GET must not be treated as a rerun. The facade has no rerun
// capability, so the 405 proves the method check runs before the capability
// lookup — otherwise this would be 501.
func TestExecutionRerunMethodMismatchIsRejected(t *testing.T) {
	m := &workflowControlModule{eng: noControlFacade{}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/executions/exec-any/rerun", nil)
	m.handleRerun(rec, req, types.ExecutionID("exec-any"))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET rerun status = %d, want 405", rec.Code)
	}
}

// TestExecutionRerunIsScopedByItsOwnScope proves rerun does NOT ride on the
// control scope: tok-control may hold and release dispatch (both 200) and is
// still refused a rerun, while tok-rerun holds the rerun grant and is refused
// a pause. Deriving a new execution from a terminal one and suspending a live
// one are different trust, and one scope for both would grant the wider of the
// two to every caller that needs the narrower.
func TestExecutionRerunIsScopedByItsOwnScope(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)

	// tok-rerun cannot pause, and tok-control cannot rerun.
	resp := f.do("tok-rerun", http.MethodPost, path+"/pause", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("pause with execution.rerun = %d, want 403", resp.StatusCode)
	}
	resp = f.do("tok-control", http.MethodPost, path+"/rerun", rerunRequest{FromNode: "start"})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("rerun with execution.control = %d, want 403 (rerun needs its own scope)", resp.StatusCode)
	}

	// The refusals were scope decisions, not side effects: the execution is
	// still running and tok-control's control grant is intact.
	resp = f.do("tok-control", http.MethodPost, path+"/pause", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause with execution.control = %d, want 200: the token's own grant must work", resp.StatusCode)
	}
	resp = f.do("tok-full", http.MethodPost, path+"/resume", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume status = %d, want 200", resp.StatusCode)
	}

	// A terminal source on the rerun-scoped token succeeds.
	resp = f.do("tok-full", http.MethodPost, path+"/cancel", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}
	resp = f.do("tok-rerun", http.MethodPost, path+"/rerun", rerunRequest{FromNode: "start"})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rerun with execution.rerun = %d, want 200", resp.StatusCode)
	}
}

// TestExecutionRerunCrossNamespaceIsNotFound proves the endpoint inherits the
// namespace boundary of the id it carries: a principal of another namespace
// holding the rerun scope gets 404 (not 403) and the source execution is
// untouched. No execution is created for it either — the store read that
// resolves the foreign id is the same one the rerun would seed against.
func TestExecutionRerunCrossNamespaceIsNotFound(t *testing.T) {
	f := newExecRouteFixture(t)
	path := "/v1/executions/" + string(f.execID)

	resp := f.do("tok-full", http.MethodPost, path+"/cancel", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}

	resp = f.do("tok-other", http.MethodPost, path+"/rerun", rerunRequest{FromNode: "start"})
	code := decodeEnvelope(t, resp, nil).Code
	if resp.StatusCode != http.StatusNotFound || code != "execution_not_found" {
		t.Fatalf("cross-namespace rerun = %d/%s, want 404/execution_not_found", resp.StatusCode, code)
	}

	// The source keeps its own status: a refused rerun leaves nothing behind.
	resp = f.do("tok-full", http.MethodGet, path, nil)
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	decodeEnvelope(t, resp, &detail)
	if detail.Status != types.ExecutionStatusCanceled {
		t.Fatalf("status after the refused cross-namespace rerun = %q, want canceled", detail.Status)
	}
}

// TestRerunInheritsTheParentWorkflowIdentity pins the attribution the rerun
// projection depends on. A rerun seed carries only a compiled graph, so the
// workflow id can come only from the definition this handler attaches from the
// parent row; without it every rerun execution is projected unattributed and
// invisible to any workflow_id filter. The parent's workflow_id column is the
// sole server-recorded identity: a stored definition payload is never read for
// an id (a legacy inline submission chose those freely), so a row written
// before the column existed stays unattributed instead of inheriting it.
func TestRerunInheritsTheParentWorkflowIdentity(t *testing.T) {
	mem := memstore.New()
	m := &workflowControlModule{executions: mem}
	ctx := context.Background()

	for _, tc := range []struct {
		name, wantID, wantName string
		rec                    store.ExecutionRecord
	}{
		{"payload id is never inherited", "", "", store.ExecutionRecord{
			ExecutionID: "exec-def",
			WorkflowDef: []byte(`{"id":"wf-client-chosen","name":"approval"}`),
		}},
		{"identity column only", "wf-column", "approval", store.ExecutionRecord{
			ExecutionID: "exec-column", WorkflowID: "wf-column", WorkflowName: "approval",
		}},
		{"identity column beats a client-supplied payload id", "wf-column", "approval", store.ExecutionRecord{
			ExecutionID:  "exec-both",
			WorkflowID:   "wf-column",
			WorkflowName: "approval",
			WorkflowDef:  []byte(`{"id":"wf-client-chosen","name":"approval"}`),
		}},
		{"unattributed parent", "", "", store.ExecutionRecord{ExecutionID: "exec-none"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tc.rec
			if err := mem.CreateExecution(ctx, &rec); err != nil {
				t.Fatal(err)
			}
			def, ok := engine.WorkflowDefFromContext(m.withParentWorkflowDef(ctx, rec.ExecutionID))
			if tc.wantID == "" {
				if ok {
					t.Fatalf("context carries workflow %q for an unattributed parent; the rerun must stay unattributed, not borrow a name", def.ID)
				}
				return
			}
			if !ok || def.ID != tc.wantID || def.Name != tc.wantName {
				t.Fatalf("attached definition = %+v (present=%v), want id %q name %q",
					def, ok, tc.wantID, tc.wantName)
			}
		})
	}
}
