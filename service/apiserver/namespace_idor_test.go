package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// errFailingQueue is returned by failingQueue.Enqueue to simulate a permanent
// queue outage so outbox entries dead-letter after maxAttempts.
var errFailingQueue = errors.New("namespace idor test: queue unavailable")

type failingQueue struct{}

func (failingQueue) Enqueue(context.Context, *engine.Task) error { return errFailingQueue }
func (failingQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return errFailingQueue
}

// namespaceIDORFixture wires a miniredis-backed distributed backend + control
// plane + APIServer with a multi-namespace token registry: tok-a → namespaceA,
// tok-b → namespaceB. Both carry the workflow/execution/management scopes so
// authz passes and IDOR is exercised at the namespace layer.
type namespaceIDORFixture struct {
	t       *testing.T
	srv     *APIServer
	httpSrv *httptest.Server
	seedEng *engine.Engine
	backend *distributed.Backend
}

func newNamespaceORFixture(t *testing.T) *namespaceIDORFixture {
	t.Helper()
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	backend, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}

	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	scopes := []string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.write"}
	principalAuth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: scopes},
		{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: scopes},
	})

	srv, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}, WithControlPlane(cp), WithManagement())
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	// Seed engine shares the same Redis-backed State() so dead-letter entries
	// written here are visible to the APIServer's management endpoints.
	seedEng := engine.New(backend.State(), failingQueue{}, engine.WithOutboxMaxDeliveryAttempts(1))

	return &namespaceIDORFixture{t: t, srv: srv, httpSrv: httpSrv, seedEng: seedEng, backend: backend}
}

func (f *namespaceIDORFixture) submitWorkflow(token string) types.ExecutionID {
	body := executeWorkflowRequest{Workflow: &types.WorkflowDef{
		Name:  "idor-wf",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	}}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req, _ := http.NewRequest(http.MethodPost, f.httpSrv.URL+"/v1/workflows/execute", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submit status = %d, want 200", resp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	var out executeWorkflowResponse
	_ = json.Unmarshal(extractData(f.t, bodyBytes), &out)
	return out.ExecutionID
}

// submitWorkflowWithNamespaceField submits a workflow whose request body carries
// a forged namespace field. The server must ignore it and create the execution
// under the principal's namespace.
func (f *namespaceIDORFixture) submitWorkflowWithNamespaceField(token, forgedNamespace string) types.ExecutionID {
	// The execute request body has no Namespace field, so submit a raw JSON
	// object with an extra namespace field to prove it is ignored.
	raw := map[string]any{
		"workflow":  map[string]any{"name": "idor-wf", "nodes": []map[string]any{{"name": "start", "type": "test.echo"}}},
		"namespace": forgedNamespace,
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(raw)
	req, _ := http.NewRequest(http.MethodPost, f.httpSrv.URL+"/v1/workflows/execute", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submit status = %d, want 200 (forged namespace field must be ignored)", resp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	var out executeWorkflowResponse
	_ = json.Unmarshal(extractData(f.t, bodyBytes), &out)
	return out.ExecutionID
}

func (f *namespaceIDORFixture) getExecution(token string, execID types.ExecutionID) (int, engine.ExecutionDetail) {
	req, _ := http.NewRequest(http.MethodGet, f.httpSrv.URL+"/v1/executions/"+string(execID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("get execution: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	_ = json.NewDecoder(resp.Body).Decode(&detail)
	return resp.StatusCode, detail
}

func (f *namespaceIDORFixture) getManagementExecution(token string, execID types.ExecutionID) int {
	req, _ := http.NewRequest(http.MethodGet, f.httpSrv.URL+"/v1/management/executions/"+string(execID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("mgmt get execution: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func (f *namespaceIDORFixture) listDeadLetters(token string, execID types.ExecutionID) (int, deadLetterListResponse) {
	req, _ := http.NewRequest(http.MethodGet, f.httpSrv.URL+"/v1/management/dead-letters/"+string(execID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("list dead letters: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// The list success body is enveloped (spec §3.1, Step 1 decision A): the
	// {entries,next_cursor} payload rides inside envelope.data.
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	var list deadLetterListResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err == nil && len(env.Data) > 0 {
		_ = json.Unmarshal(env.Data, &list)
	}
	return resp.StatusCode, list
}

func (f *namespaceIDORFixture) replayDeadLetter(token string, execID types.ExecutionID, entryID string) (int, deadLetterReplayResponse) {
	body := deadLetterReplayRequest{EntryID: entryID, Reason: "idor-test", RequestID: "req-" + entryID}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req, _ := http.NewRequest(http.MethodPost, f.httpSrv.URL+"/v1/management/dead-letters/"+string(execID)+"/replay", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("replay dead letter: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// The replay result is enveloped (spec §3.1): unwrap data before decoding
	// the typed replay response, including on the 404 not_found outcome path.
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	var out deadLetterReplayResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err == nil && len(env.Data) > 0 {
		_ = json.Unmarshal(env.Data, &out)
	}
	return resp.StatusCode, out
}

// TestNamespaceORExecutionInspectCrossNamespace proves the core IDOR matrix:
// namespaceA submits a workflow; namespaceA can inspect it; namespaceB cannot (404 on
// both the control /v1/executions/{id} and management /v1/management/executions/{id}
// endpoints). 404 — not 403 — so existence is not leaked (security policy §1a).
func TestNamespaceORExecutionInspectCrossNamespace(t *testing.T) {
	f := newNamespaceORFixture(t)
	execID := f.submitWorkflow("tok-a") // namespaceA principal

	// Same namespace: control + management inspect succeed.
	if code, _ := f.getExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA control inspect = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA management inspect = %d, want 200", code)
	}

	// Cross-namespace: control inspect → 404 (namespace-scoped GetExecution miss).
	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB control inspect = %d, want 404 (IDOR, no existence leak)", code)
	}
	// Cross-namespace: management inspect → 404.
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB management inspect = %d, want 404 (IDOR, no existence leak)", code)
	}
}

// TestNamespaceORRequestBodyNamespaceIgnored proves a client-supplied namespace field
// in the request body is ignored: the execution is created under the principal's
// namespace (security policy §1a: identity must come from the server). namespaceB
// cannot see the execution namespaceA's principal created even though the body
// claimed namespaceB.
func TestNamespaceORRequestBodyNamespaceIgnored(t *testing.T) {
	f := newNamespaceORFixture(t)
	// namespaceA principal submits with a body that lies about the namespace.
	execID := f.submitWorkflowWithNamespaceField("tok-a", "namespaceB")

	// namespaceA (the principal's namespace) can inspect.
	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA inspect (forged body namespace) = %d, want 200", code)
	}
	// namespaceB cannot inspect — the forged body namespace was ignored.
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB inspect (forged body namespace) = %d, want 404 (body namespace ignored)", code)
	}
}

// TestNamespaceORDifferentNamespacesSameWorkflowName proves two namespaces can each
// submit a workflow with the same name without colliding — the namespace prefix
// isolates them (design §7 matrix: "namespace A 提交 workflow，namespace B 提交同名
// workflow → 不冲突").
func TestNamespaceORDifferentNamespacesSameWorkflowName(t *testing.T) {
	f := newNamespaceORFixture(t)
	execA := f.submitWorkflow("tok-a")
	execB := f.submitWorkflow("tok-b")
	if execA == execB {
		t.Fatalf("namespaceA and namespaceB executions share id %q (want distinct)", execA)
	}
	// Each namespace sees only its own execution.
	if code := f.getManagementExecution("tok-a", execA); code != http.StatusOK {
		t.Fatalf("namespaceA sees execA = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-a", execB); code != http.StatusNotFound {
		t.Fatalf("namespaceA sees namespaceB's execB = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execB); code != http.StatusOK {
		t.Fatalf("namespaceB sees execB = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-b", execA); code != http.StatusNotFound {
		t.Fatalf("namespaceB sees namespaceA's execA = %d, want 404", code)
	}
}

// TestNamespaceORDeadLetterCrossNamespace proves the dead-letter list/replay
// endpoints enforce namespace IDOR: namespaceA's dead-letter entry is invisible to
// namespaceB (404), and namespaceB replay of namespaceA's entry is rejected (404). The
// dead-letter entry is seeded by a failing queue driving the entry to dead
// under namespaceA's namespace (xflow:ns:namespaceA:exec:{<id>}:outbox:dead).
func TestNamespaceORDeadLetterCrossNamespace(t *testing.T) {
	f := newNamespaceORFixture(t)

	// Seed: namespaceA execution whose initial outbox entry dead-letters because
	// the queue is unavailable and maxAttempts=1.
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespaceA"))
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "idor-dead",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	execID, err := f.seedEng.Submit(ctxA, g, nil)
	if err != nil {
		t.Fatalf("seed Submit: %v", err)
	}
	// Submit's best-effort flush already dead-lettered the entry; an explicit
	// flush is a no-op once the entry has moved to dead.
	_ = f.seedEng.FlushOutbox(ctxA, execID)

	// namespaceA lists its dead-letter entry.
	code, list := f.listDeadLetters("tok-a", execID)
	if code != http.StatusOK {
		t.Fatalf("namespaceA list dead letters = %d, want 200", code)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("namespaceA dead letters = %d entries, want 1", len(list.Entries))
	}
	entryID := list.Entries[0].ID

	// namespaceB cannot list namespaceA's dead-letters → 404 (exec not in namespaceB).
	if code, _ := f.listDeadLetters("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB list namespaceA dead letters = %d, want 404 (IDOR)", code)
	}

	// namespaceB replay of namespaceA's entry → 404 (exec existence check fails first).
	if code, _ := f.replayDeadLetter("tok-b", execID, entryID); code != http.StatusNotFound {
		t.Fatalf("namespaceB replay namespaceA dead letter = %d, want 404 (IDOR)", code)
	}
}
