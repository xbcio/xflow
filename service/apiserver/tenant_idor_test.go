package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// errFailingQueue is returned by failingQueue.Enqueue to simulate a permanent
// queue outage so outbox entries dead-letter after maxAttempts.
var errFailingQueue = errors.New("tenant idor test: queue unavailable")

type failingQueue struct{}

func (failingQueue) Enqueue(context.Context, *engine.Task) error { return errFailingQueue }
func (failingQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return errFailingQueue
}

// tenantIDORFixture wires a miniredis-backed distributed backend + control
// plane + APIServer with a multi-tenant token registry: tok-a → tenantA,
// tok-b → tenantB. Both carry the workflow/execution/management scopes so
// authz passes and IDOR is exercised at the tenant layer.
type tenantIDORFixture struct {
	t        *testing.T
	srv      *APIServer
	httpSrv  *httptest.Server
	seedEng  *engine.Engine
	backend  *distributed.Backend
}

func newTenantIDORFixture(t *testing.T) *tenantIDORFixture {
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
		{Token: "tok-a", Subject: "op-a", TenantID: "tenantA", Scopes: scopes},
		{Token: "tok-b", Subject: "op-b", TenantID: "tenantB", Scopes: scopes},
	})

	srv, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    TenantAwareAuthorizer{},
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

	return &tenantIDORFixture{t: t, srv: srv, httpSrv: httpSrv, seedEng: seedEng, backend: backend}
}

func (f *tenantIDORFixture) submitWorkflow(token string) types.ExecutionID {
	body := submitWorkflowRequest{Workflow: &types.WorkflowDef{
		Name:  "idor-wf",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	}}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req, _ := http.NewRequest(http.MethodPost, f.httpSrv.URL+"/v1/workflows", &buf)
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
	var out submitWorkflowResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.ExecutionID
}

// submitWorkflowWithTenantField submits a workflow whose request body carries
// a forged tenant field. The server must ignore it and create the execution
// under the principal's tenant.
func (f *tenantIDORFixture) submitWorkflowWithTenantField(token, forgedTenant string) types.ExecutionID {
	// submitWorkflowRequest has no Tenant field, so submit a raw JSON object
	// with an extra tenant field to prove it is ignored.
	raw := map[string]any{
		"workflow": map[string]any{"name": "idor-wf", "nodes": []map[string]any{{"name": "start", "type": "test.echo"}}},
		"tenant":   forgedTenant,
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(raw)
	req, _ := http.NewRequest(http.MethodPost, f.httpSrv.URL+"/v1/workflows", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submit status = %d, want 200 (forged tenant field must be ignored)", resp.StatusCode)
	}
	var out submitWorkflowResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.ExecutionID
}

func (f *tenantIDORFixture) getExecution(token string, execID types.ExecutionID) (int, engine.ExecutionDetail) {
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

func (f *tenantIDORFixture) getManagementExecution(token string, execID types.ExecutionID) int {
	req, _ := http.NewRequest(http.MethodGet, f.httpSrv.URL+"/v1/management/executions/"+string(execID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("mgmt get execution: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func (f *tenantIDORFixture) listDeadLetters(token string, execID types.ExecutionID) (int, deadLetterListResponse) {
	req, _ := http.NewRequest(http.MethodGet, f.httpSrv.URL+"/v1/management/dead-letters/"+string(execID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("list dead letters: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var list deadLetterListResponse
	_ = json.NewDecoder(resp.Body).Decode(&list)
	return resp.StatusCode, list
}

func (f *tenantIDORFixture) replayDeadLetter(token string, execID types.ExecutionID, entryID string) (int, deadLetterReplayResponse) {
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
	var out deadLetterReplayResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestTenantIDORExecutionInspectCrossTenant proves the core IDOR matrix:
// tenantA submits a workflow; tenantA can inspect it; tenantB cannot (404 on
// both the control /v1/executions/{id} and management /v1/management/executions/{id}
// endpoints). 404 — not 403 — so existence is not leaked (security policy §1a).
func TestTenantIDORExecutionInspectCrossTenant(t *testing.T) {
	f := newTenantIDORFixture(t)
	execID := f.submitWorkflow("tok-a") // tenantA principal

	// Same tenant: control + management inspect succeed.
	if code, _ := f.getExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA control inspect = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA management inspect = %d, want 200", code)
	}

	// Cross-tenant: control inspect → 404 (tenant-scoped GetExecution miss).
	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB control inspect = %d, want 404 (IDOR, no existence leak)", code)
	}
	// Cross-tenant: management inspect → 404.
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB management inspect = %d, want 404 (IDOR, no existence leak)", code)
	}
}

// TestTenantIDORRequestBodyTenantIgnored proves a client-supplied tenant field
// in the request body is ignored: the execution is created under the principal's
// tenant (security policy §1a: identity must come from the server). tenantB
// cannot see the execution tenantA's principal created even though the body
// claimed tenantB.
func TestTenantIDORRequestBodyTenantIgnored(t *testing.T) {
	f := newTenantIDORFixture(t)
	// tenantA principal submits with a body that lies about the tenant.
	execID := f.submitWorkflowWithTenantField("tok-a", "tenantB")

	// tenantA (the principal's tenant) can inspect.
	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA inspect (forged body tenant) = %d, want 200", code)
	}
	// tenantB cannot inspect — the forged body tenant was ignored.
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB inspect (forged body tenant) = %d, want 404 (body tenant ignored)", code)
	}
}

// TestTenantIDORDifferentTenantsSameWorkflowName proves two tenants can each
// submit a workflow with the same name without colliding — the tenant prefix
// isolates them (design §7 matrix: "tenant A 提交 workflow，tenant B 提交同名
// workflow → 不冲突").
func TestTenantIDORDifferentTenantsSameWorkflowName(t *testing.T) {
	f := newTenantIDORFixture(t)
	execA := f.submitWorkflow("tok-a")
	execB := f.submitWorkflow("tok-b")
	if execA == execB {
		t.Fatalf("tenantA and tenantB executions share id %q (want distinct)", execA)
	}
	// Each tenant sees only its own execution.
	if code := f.getManagementExecution("tok-a", execA); code != http.StatusOK {
		t.Fatalf("tenantA sees execA = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-a", execB); code != http.StatusNotFound {
		t.Fatalf("tenantA sees tenantB's execB = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execB); code != http.StatusOK {
		t.Fatalf("tenantB sees execB = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-b", execA); code != http.StatusNotFound {
		t.Fatalf("tenantB sees tenantA's execA = %d, want 404", code)
	}
}

// TestTenantIDORDeadLetterCrossTenant proves the dead-letter list/replay
// endpoints enforce tenant IDOR: tenantA's dead-letter entry is invisible to
// tenantB (404), and tenantB replay of tenantA's entry is rejected (404). The
// dead-letter entry is seeded by a failing queue driving the entry to dead
// under tenantA's namespace (xflow:ttenantA:exec:{<id>}:outbox:dead).
func TestTenantIDORDeadLetterCrossTenant(t *testing.T) {
	f := newTenantIDORFixture(t)

	// Seed: tenantA execution whose initial outbox entry dead-letters because
	// the queue is unavailable and maxAttempts=1.
	ctxA := tenant.WithTenant(context.Background(), tenant.TenantID("tenantA"))
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

	// tenantA lists its dead-letter entry.
	code, list := f.listDeadLetters("tok-a", execID)
	if code != http.StatusOK {
		t.Fatalf("tenantA list dead letters = %d, want 200", code)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("tenantA dead letters = %d entries, want 1", len(list.Entries))
	}
	entryID := list.Entries[0].ID

	// tenantB cannot list tenantA's dead-letters → 404 (exec not in tenantB).
	if code, _ := f.listDeadLetters("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB list tenantA dead letters = %d, want 404 (IDOR)", code)
	}

	// tenantB replay of tenantA's entry → 404 (exec existence check fails first).
	if code, _ := f.replayDeadLetter("tok-b", execID, entryID); code != http.StatusNotFound {
		t.Fatalf("tenantB replay tenantA dead letter = %d, want 404 (IDOR)", code)
	}
}
