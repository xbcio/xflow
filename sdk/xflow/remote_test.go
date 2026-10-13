package xflow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// These tests drive the Remote against a fake HTTP server, one route at a
// time. They are the only place the wire shapes are pinned: the server tests
// own the other side, and the two together are what a change to either breaks.

// remoteControlAlignment is the method set the Remote shares with *Engine, at
// the same signatures. Both assertions below go through the compiler: if a
// stored *Engine were to be swapped for a *Remote (or the reverse), the swap is
// a type-level no-op — or it does not compile, which is the point of pinning
// this here rather than in prose.
type remoteControlAlignment interface {
	AddWorkflow(context.Context, *WorkflowBuilder) (types.WorkflowID, error)
	RemoveWorkflow(context.Context, types.WorkflowID) error
	Invoke(context.Context, types.WorkflowID, Entry, map[string]any, ...InvokeOption) (types.ExecutionID, error)
	Wait(context.Context, types.ExecutionID) (types.Result, error)
	Signal(context.Context, types.ExecutionID, string, map[string]any) error
	RevokeSignal(context.Context, types.ExecutionID, string) error
	Cancel(context.Context, types.ExecutionID) error
	Pause(context.Context, types.ExecutionID) error
	Resume(context.Context, types.ExecutionID) error
	Rerun(context.Context, types.ExecutionID, string, ...RerunOption) (engine.RerunResult, error)
	Inspect(context.Context, types.ExecutionID, ...string) (engine.ExecutionDetail, error)
	Stop()
}

var (
	// ReplaceWorkflow, InvokeWithResult and ListExecutions are deliberately NOT
	// in the interface: the first two are Remote-specific deviations the HTTP
	// face forces (documented on each method), the last has no Engine
	// counterpart at all.
	_ remoteControlAlignment = (*Remote)(nil)
	_ remoteControlAlignment = (*Engine)(nil)
)

// remoteFakeRequest is one request the fake server received, recorded whole so
// a test can assert the method, route, query, headers and body of one call.
type remoteFakeRequest struct {
	method string
	uri    string
	path   string
	query  url.Values
	header http.Header
	body   []byte
}

// remoteFakeServer answers the Remote's requests with whatever the test's
// handler writes. It asserts the bearer header on every request: the token is
// the whole of a Remote's identity, so a route that forgot it would be
// answered 401 by a real server and could not be distinguished from a wrong
// route by a test that only checks paths.
type remoteFakeServer struct {
	srv     *httptest.Server
	mu      sync.Mutex
	handled []remoteFakeRequest
	handler func(w http.ResponseWriter, r *http.Request, body []byte)
}

func newRemoteFakeServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, body []byte)) *remoteFakeServer {
	t.Helper()
	f := &remoteFakeServer{handler: handler}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("fake server: read %s %s body: %v", r.Method, r.URL.Path, err)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("%s %s carried Authorization %q, want the configured bearer token",
				r.Method, r.URL.Path, got)
		}
		f.mu.Lock()
		f.handled = append(f.handled, remoteFakeRequest{
			method: r.Method,
			uri:    r.URL.RequestURI(),
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
			body:   body,
		})
		f.mu.Unlock()
		f.handler(w, r, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *remoteFakeServer) calls() []remoteFakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]remoteFakeRequest(nil), f.handled...)
}

func (f *remoteFakeServer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.handled)
}

func newRemoteForTest(t *testing.T, f *remoteFakeServer, opts ...RemoteOption) *Remote {
	t.Helper()
	r, err := NewRemote(f.srv.URL, append([]RemoteOption{WithRemoteToken("test-token")}, opts...)...)
	if err != nil {
		t.Fatalf("NewRemote: %v", err)
	}
	t.Cleanup(r.Stop)
	return r
}

// writeRemoteOK writes a success envelope, the shape every user-facing /v1
// route answers with.
func writeRemoteOK(t *testing.T, w http.ResponseWriter, status int, data any) {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Errorf("fake server: marshal data: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(`{"success":true,"code":"200","message":"ok","data":` + string(raw) + `}`)); err != nil {
		t.Errorf("fake server: write envelope: %v", err)
	}
}

func writeRemoteFail(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "code": code, "message": message})
}

func remoteTestWorkflow(name string) *WorkflowBuilder {
	wf := Workflow(name)
	wf.Node("start", node.Start())
	return wf
}

func TestRemoteAddWorkflowPostsTheDefinition(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusCreated, map[string]any{"workflow_id": "wf-1"})
	})
	r := newRemoteForTest(t, f)

	id, err := r.AddWorkflow(context.Background(), remoteTestWorkflow("remote-test"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	if id != "wf-1" {
		t.Fatalf("AddWorkflow returned %q, want the server-minted wf-1", id)
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(calls))
	}
	if calls[0].method != http.MethodPost || calls[0].path != "/v1/workflows" {
		t.Fatalf("request was %s %s, want POST /v1/workflows", calls[0].method, calls[0].uri)
	}
	var def types.WorkflowDef
	if err := json.Unmarshal(calls[0].body, &def); err != nil {
		t.Fatalf("request body is not a WorkflowDef: %v\nbody: %s", err, calls[0].body)
	}
	if def.Name != "remote-test" {
		t.Fatalf("body carries workflow name %q, want remote-test", def.Name)
	}
}

func TestRemoteReplaceWorkflowPutsTheExplicitID(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{"workflow_id": "wf-1"})
	})
	r := newRemoteForTest(t, f)

	id, err := r.ReplaceWorkflow(context.Background(), "wf-1", remoteTestWorkflow("remote-test"))
	if err != nil {
		t.Fatalf("ReplaceWorkflow: %v", err)
	}
	if id != "wf-1" {
		t.Fatalf("ReplaceWorkflow returned %q, want wf-1", id)
	}
	calls := f.calls()
	if len(calls) != 1 || calls[0].method != http.MethodPut || calls[0].path != "/v1/workflows/wf-1" {
		t.Fatalf("request was %+v, want PUT /v1/workflows/wf-1", calls)
	}
}

func TestRemoteRemoveWorkflowDeletesTheResource(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{"removed": true})
	})
	r := newRemoteForTest(t, f)

	if err := r.RemoveWorkflow(context.Background(), "wf-1"); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	calls := f.calls()
	if len(calls) != 1 || calls[0].method != http.MethodDelete || calls[0].path != "/v1/workflows/wf-1" {
		t.Fatalf("request was %+v, want DELETE /v1/workflows/wf-1", calls)
	}
}

func TestRemoteInvokeSendsEntryInputAndAFreshKey(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{
			"execution_id":   "exec-1",
			"status":         "queued",
			"queue_position": 2,
			"duplicate":      false,
		})
	})
	r := newRemoteForTest(t, f)

	id, err := r.Invoke(context.Background(), "wf-1", Start(), map[string]any{"rows": 3}, WithTestRun())
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if id != "exec-1" {
		t.Fatalf("Invoke returned %q, want exec-1", id)
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(calls))
	}
	call := calls[0]
	if call.method != http.MethodPost || call.path != "/v1/workflows/wf-1/execute" {
		t.Fatalf("request was %s %s, want POST /v1/workflows/wf-1/execute", call.method, call.uri)
	}
	// Every Invoke carries a key even when the caller set none: without one, a
	// retried request that lost its response creates a second execution nobody
	// can reconcile. The shape is 128 bits of hex.
	key := call.header.Get("Idempotency-Key")
	if len(key) != 32 || strings.Trim(key, "0123456789abcdef") != "" {
		t.Fatalf("Idempotency-Key = %q, want 32 hex characters", key)
	}
	var body map[string]any
	if err := json.Unmarshal(call.body, &body); err != nil {
		t.Fatalf("request body: %v", err)
	}
	want := map[string]any{"entry": "start", "input": map[string]any{"rows": float64(3)}, "test": true}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("request body = %#v, want %#v", body, want)
	}
}

func TestRemoteInvokeHonoursTheCallersKeyAndReportsDuplicates(t *testing.T) {
	var calls int32
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		dup := atomic.AddInt32(&calls, 1) > 1
		writeRemoteOK(t, w, http.StatusOK, map[string]any{
			"execution_id": "exec-1",
			"status":       "running",
			"duplicate":    dup,
		})
	})
	r := newRemoteForTest(t, f)

	first, err := r.InvokeWithResult(context.Background(), "wf-1", Start(), nil, WithIdempotencyKey("caller-key"))
	if err != nil {
		t.Fatalf("first InvokeWithResult: %v", err)
	}
	if first.ExecutionID != "exec-1" || first.Duplicate {
		t.Fatalf("first submission = %+v, want exec-1, not a duplicate", first)
	}
	// The same key again is the retry the caller is entitled to make: the
	// server answers with the execution the first call started, and that is a
	// success, not an error.
	second, err := r.InvokeWithResult(context.Background(), "wf-1", Start(), nil, WithIdempotencyKey("caller-key"))
	if err != nil {
		t.Fatalf("replayed InvokeWithResult: %v", err)
	}
	if second.ExecutionID != "exec-1" || !second.Duplicate {
		t.Fatalf("replayed submission = %+v, want the original exec-1 with Duplicate set", second)
	}
	for i, call := range f.calls() {
		if got := call.header.Get("Idempotency-Key"); got != "caller-key" {
			t.Fatalf("request %d carried Idempotency-Key %q, want the caller's key", i+1, got)
		}
	}
}

func TestRemoteInvokeRefusesOptionsWithNoRemoteEquivalent(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{"execution_id": "exec-1"})
	})
	r := newRemoteForTest(t, f)

	for _, tc := range []struct {
		name string
		opt  InvokeOption
		want string
	}{
		{"execution ttl", WithExecutionTTL(time.Hour), "WithExecutionTTL"},
		{"runtime", WithRuntimeVars(map[string]any{"k": "v"}), "WithRuntime"},
		{"trace id", WithTraceID("trace-1"), "WithTraceID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := r.Invoke(context.Background(), "wf-1", Start(), nil, tc.opt)
			if err == nil {
				t.Fatalf("Invoke accepted %s remotely", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to name %s", err, tc.want)
			}
		})
	}
	// Refused before any request: a half-expressed submission must not reach
	// the server and start an execution with different semantics.
	if got := f.callCount(); got != 0 {
		t.Fatalf("server saw %d requests for refused options, want 0", got)
	}
}

func TestRemoteInvokeRetriesATransportFailureWithTheSameKey(t *testing.T) {
	var attempts int32
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			// Drop the connection without a response: the client cannot know
			// whether the server started the execution.
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("fake server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("fake server: hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		writeRemoteOK(t, w, http.StatusOK, map[string]any{"execution_id": "exec-1", "status": "running"})
	})
	r := newRemoteForTest(t, f)

	id, err := r.Invoke(context.Background(), "wf-1", Start(), nil)
	if err != nil {
		t.Fatalf("Invoke after a dropped connection: %v", err)
	}
	if id != "exec-1" {
		t.Fatalf("Invoke returned %q, want exec-1", id)
	}
	calls := f.calls()
	if len(calls) != 2 {
		t.Fatalf("server saw %d requests, want exactly the one retry", len(calls))
	}
	first, second := calls[0].header.Get("Idempotency-Key"), calls[1].header.Get("Idempotency-Key")
	if first == "" || second == "" {
		t.Fatalf("retry attempt missing an Idempotency-Key: %q, %q", first, second)
	}
	if first != second {
		t.Fatalf("retry carried Idempotency-Key %q after %q; a retry with a new key "+
			"creates a second execution — the exact outcome the key exists to prevent", second, first)
	}
}

func TestRemoteWaitLoopsUntilTerminal(t *testing.T) {
	var calls int32
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		switch atomic.AddInt32(&calls, 1) {
		case 1, 2:
			writeRemoteOK(t, w, http.StatusAccepted, map[string]any{
				"execution_id": "exec-1", "status": "running", "timed_out": true,
			})
		case 3:
			// A 200 that is not terminal is a contract violation; treating it
			// as terminal would hand the caller a result claiming a finished
			// execution, so the loop must ask again rather than return it.
			writeRemoteOK(t, w, http.StatusOK, map[string]any{"execution_id": "exec-1", "status": "running"})
		default:
			writeRemoteOK(t, w, http.StatusOK, map[string]any{
				"execution_id": "exec-1",
				"status":       "success",
				"nodes": []map[string]any{
					{"name": "start", "status": "success", "output": map[string]any{"ok": true}},
				},
			})
		}
	})
	r := newRemoteForTest(t, f)

	res, err := r.Wait(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.ExecutionID != "exec-1" || res.Status != "success" {
		t.Fatalf("Wait result = %+v, want exec-1/success", res)
	}
	wantOut := map[string]any{"ok": true}
	if !reflect.DeepEqual(res.Output["start"], wantOut) {
		t.Fatalf("Wait output for start = %#v, want %#v (the same rebuild the embedded engine applies)",
			res.Output["start"], wantOut)
	}
	seen := f.calls()
	if len(seen) != 4 {
		t.Fatalf("wait loop made %d requests, want 4 (two holds, one non-terminal 200, then the detail)", len(seen))
	}
	for i, call := range seen {
		if call.method != http.MethodGet || call.path != "/v1/executions/exec-1/wait" {
			t.Fatalf("wait request %d was %s %s", i+1, call.method, call.uri)
		}
		if got := call.query.Get("timeout"); got != "30s" {
			t.Fatalf("wait request %d asked for timeout=%q, want 30s", i+1, got)
		}
	}
}

func TestRemoteWaitReturnsTheContextErrorOnCancel(t *testing.T) {
	requestSeen := make(chan struct{}, 1)
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		select {
		case requestSeen <- struct{}{}:
		default:
		}
		// Hold the request the way the real route holds one, until the client
		// goes away (or far longer than the test can legitimately need).
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	r := newRemoteForTest(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := r.Wait(ctx, "exec-1")
		done <- err
	}()

	select {
	case <-requestSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait request never reached the fake server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait returned %v, want context.Canceled; cancellation must not "+
				"be reported as a server failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after cancel; the in-flight hold is not aborted by ctx")
	}
}

func TestRemoteInspectFiltersTheNodesClientSide(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{
			"execution_id": "exec-1",
			"status":       "running",
			"nodes": []map[string]any{
				{"name": "alpha", "status": "success"},
				{"name": "beta", "status": "running", "attempt": 2},
				{"name": "gamma", "status": "pending"},
			},
		})
	})
	r := newRemoteForTest(t, f)

	detail, err := r.Inspect(context.Background(), "exec-1", "beta")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(detail.Nodes) != 1 || detail.Nodes[0].Name != "beta" || detail.Nodes[0].Attempt != 2 {
		t.Fatalf("Inspect with a node filter = %+v, want only beta (one request, filtered here)", detail.Nodes)
	}
	if calls := f.calls(); len(calls) != 1 || calls[0].path != "/v1/executions/exec-1" {
		t.Fatalf("Inspect made %+v, want one GET /v1/executions/exec-1", calls)
	}

	// Without names the same snapshot comes back whole.
	detail, err = r.Inspect(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Inspect (all nodes): %v", err)
	}
	if len(detail.Nodes) != 3 {
		t.Fatalf("unfiltered Inspect returned %d nodes, want 3", len(detail.Nodes))
	}
}

func TestRemoteSignalAndControlRoutes(t *testing.T) {
	cases := []struct {
		name     string
		call     func(*Remote, context.Context) error
		method   string
		path     string
		wantBody map[string]any
	}{
		{
			name: "signal",
			call: func(r *Remote, ctx context.Context) error {
				return r.Signal(ctx, "exec-1", "approve", map[string]any{"by": "alice"})
			},
			method: http.MethodPost,
			path:   "/v1/executions/exec-1/signals",
			wantBody: map[string]any{
				"name": "approve",
				"data": map[string]any{"by": "alice"},
			},
		},
		{
			name:   "revoke signal",
			call:   func(r *Remote, ctx context.Context) error { return r.RevokeSignal(ctx, "exec-1", "approve") },
			method: http.MethodDelete,
			path:   "/v1/executions/exec-1/signals/approve",
		},
		{
			name:   "cancel",
			call:   func(r *Remote, ctx context.Context) error { return r.Cancel(ctx, "exec-1") },
			method: http.MethodPost,
			path:   "/v1/executions/exec-1/cancel",
		},
		{
			name:   "pause",
			call:   func(r *Remote, ctx context.Context) error { return r.Pause(ctx, "exec-1") },
			method: http.MethodPost,
			path:   "/v1/executions/exec-1/pause",
		},
		{
			name:   "resume",
			call:   func(r *Remote, ctx context.Context) error { return r.Resume(ctx, "exec-1") },
			method: http.MethodPost,
			path:   "/v1/executions/exec-1/resume",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				writeRemoteOK(t, w, http.StatusOK, map[string]any{"execution_id": "exec-1", "status": "paused"})
			})
			r := newRemoteForTest(t, f)

			if err := tc.call(r, context.Background()); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			calls := f.calls()
			if len(calls) != 1 || calls[0].method != tc.method || calls[0].path != tc.path {
				t.Fatalf("request was %+v, want %s %s", calls, tc.method, tc.path)
			}
			if tc.wantBody != nil {
				var body map[string]any
				if err := json.Unmarshal(calls[0].body, &body); err != nil {
					t.Fatalf("request body: %v", err)
				}
				if !reflect.DeepEqual(body, tc.wantBody) {
					t.Fatalf("request body = %#v, want %#v", body, tc.wantBody)
				}
			} else if len(calls[0].body) != 0 {
				t.Fatalf("request carried body %s, want none", calls[0].body)
			}
		})
	}
}

func TestRemoteRerunSendsFromNodeAndTheKey(t *testing.T) {
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{
			"execution_id":   "exec-2",
			"status":         "queued",
			"queue_position": 1,
			"duplicate":      true,
		})
	})
	r := newRemoteForTest(t, f)

	res, err := r.Rerun(context.Background(), "exec-1", "node-b", RerunIncludeSuccess(), RerunIdempotencyKey("rerun-key"))
	if err != nil {
		t.Fatalf("Rerun: %v", err)
	}
	want := engine.RerunResult{ExecutionID: "exec-2", Queued: true, QueuePosition: 1, Duplicate: true}
	if res != want {
		t.Fatalf("Rerun result = %+v, want %+v", res, want)
	}
	calls := f.calls()
	if len(calls) != 1 || calls[0].method != http.MethodPost || calls[0].path != "/v1/executions/exec-1/rerun" {
		t.Fatalf("request was %+v, want POST /v1/executions/exec-1/rerun", calls)
	}
	if got := calls[0].header.Get("Idempotency-Key"); got != "rerun-key" {
		t.Fatalf("Idempotency-Key = %q; the route reads the key from the header, not the body", got)
	}
	var body map[string]any
	if err := json.Unmarshal(calls[0].body, &body); err != nil {
		t.Fatalf("request body: %v", err)
	}
	wantBody := map[string]any{"from_node": "node-b", "include_success": true}
	if !reflect.DeepEqual(body, wantBody) {
		t.Fatalf("request body = %#v, want %#v", body, wantBody)
	}
}

func TestRemoteListExecutionsEncodesFiltersAndDecodesThePage(t *testing.T) {
	after := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	before := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeRemoteOK(t, w, http.StatusOK, map[string]any{
			"list": []map[string]any{
				{
					"execution_id":  "exec-1",
					"namespace":     "team-a",
					"workflow_name": "nightly",
					"status":        "failed",
					"error":         "node failed",
					"created_at":    "2026-10-01T12:00:00Z",
					"updated_at":    "2026-10-01T12:00:05Z",
				},
				{"execution_id": "exec-2", "status": "success"},
			},
			"total": 5,
		})
	})
	r := newRemoteForTest(t, f)

	page, err := r.ListExecutions(context.Background(), RemoteExecutionQuery{
		Status:        "failed",
		CreatedAfter:  after,
		CreatedBefore: before,
		Page:          2,
		PageSize:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutions: %v", err)
	}
	if page.Total != 5 || len(page.Executions) != 2 {
		t.Fatalf("page = %+v, want 2 rows of total 5", page)
	}
	first := page.Executions[0]
	if first.ExecutionID != "exec-1" || first.WorkflowName != "nightly" || first.Status != "failed" ||
		!first.CreatedAt.Equal(after) {
		t.Fatalf("first row = %+v, want the server's fields decoded", first)
	}
	calls := f.calls()
	if len(calls) != 1 || calls[0].method != http.MethodGet || calls[0].path != "/v1/executions" {
		t.Fatalf("request was %+v, want GET /v1/executions", calls)
	}
	wantQuery := url.Values{
		"status":         {"failed"},
		"created_after":  {after.Format(time.RFC3339Nano)},
		"created_before": {before.Format(time.RFC3339Nano)},
		"page":           {"2"},
		"page_size":      {"10"},
	}
	if got := calls[0].query; !reflect.DeepEqual(got, wantQuery) {
		t.Fatalf("query = %v, want %v", got, wantQuery)
	}
}

func TestRemoteErrorMappingTable(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		code       string
		retryAfter string
		want       error
		unmapped   bool
		retryable  bool
	}{
		{"workflow not found", http.StatusNotFound, "workflow_not_found", "", store.ErrNotFound, false, false},
		{"execution not found", http.StatusNotFound, "execution_not_found", "", store.ErrNotFound, false, false},
		{"route not found", http.StatusNotFound, "route_not_found", "", store.ErrNotFound, false, false},
		{"workflow conflict", http.StatusConflict, "workflow_conflict", "", backend.ErrWorkflowConflict, false, false},
		{"idempotency conflict", http.StatusConflict, "idempotency_key_conflict", "", engine.ErrIdempotencyConflict, false, false},
		{"idempotency in progress", http.StatusConflict, "idempotency_key_in_progress", "1", engine.ErrIdempotencyInProgress, false, false},
		// idempotency_unsupported is the capability answer a bare status rule
		// would call retryable: it arrives as 503, and a caller looping on
		// Retryable would ask this server for a key it cannot hold forever.
		{"idempotency unsupported", http.StatusServiceUnavailable, "idempotency_unsupported", "", engine.ErrIdempotencyUnsupported, false, false},
		{"concurrency limit", http.StatusTooManyRequests, "workflow_concurrency_limit", "3", engine.ErrConcurrencyLimit, false, false},
		{"concurrency queue full", http.StatusTooManyRequests, "workflow_concurrency_queue_full", "3", engine.ErrConcurrencyQueueFull, false, false},
		{"signal consumed", http.StatusConflict, "signal_consumed", "", engine.ErrSignalConsumed, false, false},
		{"not pausable", http.StatusConflict, "execution_not_pausable", "", engine.ErrExecutionNotPausable, false, false},
		{"not paused", http.StatusConflict, "execution_not_paused", "", engine.ErrExecutionNotPaused, false, false},
		// The two 501 rows are capability answers: the code maps to its
		// sentinel, but Retryable is false because asking again cannot change
		// the deployment's mind.
		{"control unsupported", http.StatusNotImplemented, "execution_control_unsupported", "", engine.ErrPauseUnsupported, false, false},
		{"not rerunnable", http.StatusConflict, "execution_not_rerunnable", "", engine.ErrExecutionNotRerunnable, false, false},
		{"rerun node not found", http.StatusBadRequest, "rerun_node_not_found", "", engine.ErrRerunNodeNotFound, false, false},
		{"rerun upstream incomplete", http.StatusConflict, "rerun_upstream_incomplete", "", engine.ErrRerunUpstreamIncomplete, false, false},
		{"rerun graph unsupported", http.StatusBadRequest, "rerun_graph_unsupported", "", engine.ErrRerunGraphUnsupported, false, false},
		{"rerun unsupported", http.StatusNotImplemented, "execution_rerun_unsupported", "", engine.ErrRerunUnsupported, false, false},
		{"unauthenticated", http.StatusUnauthorized, "unauthorized", "", ErrRemoteUnauthenticated, false, false},
		{"forbidden", http.StatusForbidden, "forbidden", "", ErrRemoteForbidden, false, false},
		{"unmapped 500", http.StatusInternalServerError, "internal_error", "", nil, true, true},
		{"unmapped 400", http.StatusBadRequest, "bad_request", "", nil, true, false},
		{"unmapped 503", http.StatusServiceUnavailable, "unavailable", "2", nil, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				writeRemoteFail(w, tc.status, tc.code, "server says "+tc.code)
			})
			r := newRemoteForTest(t, f)

			err := r.RemoveWorkflow(context.Background(), "wf-1")
			if err == nil {
				t.Fatalf("RemoveWorkflow succeeded on HTTP %d", tc.status)
			}
			var remoteErr *RemoteError
			if !errors.As(err, &remoteErr) {
				t.Fatalf("error %v is not a *RemoteError", err)
			}
			if remoteErr.Status != tc.status || remoteErr.Code != tc.code || remoteErr.Message != "server says "+tc.code {
				t.Fatalf("RemoteError = %+v, want status %d code %q with the server's message",
					remoteErr, tc.status, tc.code)
			}
			if remoteErr.Retryable() != tc.retryable {
				t.Fatalf("Retryable() = %v for HTTP %d, want %v", remoteErr.Retryable(), tc.status, tc.retryable)
			}
			if tc.retryAfter != "" {
				want, err := time.ParseDuration(tc.retryAfter + "s")
				if err != nil {
					t.Fatalf("bad test row: %v", err)
				}
				if remoteErr.RetryAfter != want {
					t.Fatalf("RetryAfter = %s, want %s", remoteErr.RetryAfter, want)
				}
			} else if remoteErr.RetryAfter != 0 {
				t.Fatalf("RetryAfter = %s without a header, want 0", remoteErr.RetryAfter)
			}
			// A 429 is deliberately not in Retryable(): the server sent a
			// Retry-After because the retry is a question of when, and only the
			// caller knows whether it can wait. The sentinel still maps, so a
			// caller's errors.Is judgement is unchanged.
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("errors.Is(%v, %v) = false, want true", err, tc.want)
				}
				// A code maps to exactly the sentinel it names: the 401 rows
				// must not also read as forbidden, and vice versa.
				for _, other := range []error{store.ErrNotFound, engine.ErrConcurrencyLimit, ErrRemoteUnauthenticated, ErrRemoteForbidden} {
					if other != tc.want && errors.Is(err, other) {
						t.Fatalf("errors.Is(err, %v) = true for a %s row; mapping is not exclusive", other, tc.code)
					}
				}
			} else if tc.unmapped {
				for _, other := range []error{store.ErrNotFound, backend.ErrWorkflowConflict, engine.ErrConcurrencyLimit,
					ErrRemoteUnauthenticated, ErrRemoteForbidden, engine.ErrRerunUnsupported} {
					if errors.Is(err, other) {
						t.Fatalf("unmapped code %q matched sentinel %v; an invented correspondence is worse than none",
							tc.code, other)
					}
				}
			}
		})
	}
}

func TestRemoteRetryAfterParsing(t *testing.T) {
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-30 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRemoteRetryAfter("3"); got != 3*time.Second {
		t.Fatalf(`parseRemoteRetryAfter("3") = %s, want 3s`, got)
	}
	if got := parseRemoteRetryAfter(" 3 "); got != 3*time.Second {
		t.Fatalf(`parseRemoteRetryAfter(" 3 ") = %s, want 3s`, got)
	}
	for _, bad := range []string{"", "-1", "banana", past} {
		if got := parseRemoteRetryAfter(bad); got != 0 {
			t.Fatalf("parseRemoteRetryAfter(%q) = %s, want 0", bad, got)
		}
	}
	if got := parseRemoteRetryAfter(future); got <= 0 || got > 31*time.Second {
		t.Fatalf("parseRemoteRetryAfter(HTTP-date %q) = %s, want a positive wait bounded by the date", future, got)
	}
}

func TestRemoteRefusesNonEnvelopeAnswers(t *testing.T) {
	t.Run("2xx html is not a success", func(t *testing.T) {
		f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html><body>captive portal</body></html>"))
		})
		r := newRemoteForTest(t, f)

		_, err := r.Inspect(context.Background(), "exec-1")
		var remoteErr *RemoteError
		if !errors.As(err, &remoteErr) {
			t.Fatalf("error %v is not a *RemoteError", err)
		}
		if remoteErr.Status != http.StatusOK || !strings.Contains(remoteErr.Message, "unexpected non-envelope response") {
			t.Fatalf("RemoteError = %+v, want a 200 non-envelope refusal naming the shape", remoteErr)
		}
	})

	t.Run("2xx success false is the server speaking", func(t *testing.T) {
		f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":false,"code":"odd","message":"nope"}`))
		})
		r := newRemoteForTest(t, f)

		_, err := r.Inspect(context.Background(), "exec-1")
		var remoteErr *RemoteError
		if !errors.As(err, &remoteErr) {
			t.Fatalf("error %v is not a *RemoteError", err)
		}
		if remoteErr.Code != "odd" || remoteErr.Message != "nope" {
			t.Fatalf("RemoteError = %+v, want the envelope's code and message preserved", remoteErr)
		}
	})

	t.Run("non-envelope failure body is bounded", func(t *testing.T) {
		f := newRemoteFakeServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
		})
		r := newRemoteForTest(t, f)

		err := r.RemoveWorkflow(context.Background(), "wf-1")
		var remoteErr *RemoteError
		if !errors.As(err, &remoteErr) {
			t.Fatalf("error %v is not a *RemoteError", err)
		}
		if remoteErr.Status != http.StatusBadGateway {
			t.Fatalf("Status = %d, want 502", remoteErr.Status)
		}
		if len(remoteErr.Message) > len("unexpected response: ")+maxRemoteErrorBody+len("…") {
			t.Fatalf("message is %d bytes; a proxy's body must not be copied whole into an error", len(remoteErr.Message))
		}
		if !strings.HasSuffix(remoteErr.Message, "…") {
			t.Fatalf("message %q does not mark the truncation", remoteErr.Message)
		}
	})
}

func TestRemoteEndpointAndOptionValidation(t *testing.T) {
	bad := []string{"", "   ", "ftp://host", "http://", "http://host/a?x=1", "http://host/a#f", "://bad"}
	for _, endpoint := range bad {
		if _, err := NewRemote(endpoint); err == nil {
			t.Fatalf("NewRemote(%q) succeeded, want a validation error", endpoint)
		}
	}

	for _, tc := range []struct{ endpoint, want string }{
		{"http://host:8080/", "http://host:8080"},
		{"http://host:8080/base/", "http://host:8080/base"},
	} {
		r, err := NewRemote(tc.endpoint)
		if err != nil {
			t.Fatalf("NewRemote(%q): %v", tc.endpoint, err)
		}
		if got := r.Endpoint(); got != tc.want {
			t.Fatalf("Endpoint() = %q for %q, want %q", got, tc.endpoint, tc.want)
		}
	}

	if _, err := NewRemote("http://host", WithRemoteTimeout(-time.Second)); err == nil {
		t.Fatal("NewRemote accepted a negative timeout")
	}
	if _, err := NewRemote("http://host", WithRemoteHTTPClient(&http.Client{}), WithRemoteCA("/nonexistent/ca.pem")); err == nil {
		t.Fatal("NewRemote accepted an injected client together with a CA file; which wins would be ambiguous")
	}
	if _, err := NewRemote("http://host", WithRemoteCA("/nonexistent/ca.pem")); err == nil {
		t.Fatal("NewRemote accepted an unreadable CA file")
	}
	if _, err := NewRemote("http://host", WithRemoteTLSCert("/nonexistent/cert.pem", "")); err == nil {
		t.Fatal("NewRemote accepted a client certificate without its key")
	}
}
