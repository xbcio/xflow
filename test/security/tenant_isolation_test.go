package security

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// errFailingQueue is returned by failingQueue.Enqueue to simulate a permanent
// queue outage so outbox entries dead-letter after maxAttempts.
var errFailingQueue = errors.New("tenant isolation test: queue unavailable")

type failingQueue struct{}

func (failingQueue) Enqueue(context.Context, *engine.Task) error { return errFailingQueue }
func (failingQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return errFailingQueue
}

const (
	testTenantA = "tenantA"
	testTenantB = "tenantB"
)

var testScopes = []string{
	"workflow",
	"execution",
	"deadletter.list",
	"deadletter.replay",
	"management.read",
	"management.write",
}

// tenantIsolationFixture wires a miniredis-backed distributed backend +
// control plane + APIServer with a multi-tenant token registry:
// tok-a -> tenantA, tok-b -> tenantB. Both tokens carry the scopes required to
// exercise the workflow, execution, dead-letter, and management surfaces.
type tenantIsolationFixture struct {
	t        *testing.T
	srv      *apiserver.APIServer
	httpSrv  *httptest.Server
	backend  *distributed.Backend
	cp       *control.ControlPlane
	seedEng  *engine.Engine
}

func newTenantIsolationFixture(t *testing.T) *tenantIsolationFixture {
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

	principalAuth := apiserver.NewBearerPrincipalAuthMulti([]apiserver.TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", TenantID: testTenantA, Scopes: testScopes},
		{Token: "tok-b", Subject: "op-b", TenantID: testTenantB, Scopes: testScopes},
	})

	srv, err := apiserver.New(apiserver.Config{
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    apiserver.TenantAwareAuthorizer{},
		AuditSink:     apiserver.NewInMemoryAuditSink(),
	}, apiserver.WithControlPlane(cp), apiserver.WithManagement())
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	// Seed engine shares the same Redis-backed State() so dead-letter entries
	// written here are visible to the APIServer's management endpoints.
	seedEng := engine.New(backend.State(), failingQueue{}, engine.WithOutboxMaxDeliveryAttempts(1))

	return &tenantIsolationFixture{
		t:       t,
		srv:     srv,
		httpSrv: httpSrv,
		backend: backend,
		cp:      cp,
		seedEng: seedEng,
	}
}

func (f *tenantIsolationFixture) url(path string) string {
	return f.httpSrv.URL + path
}

func (f *tenantIsolationFixture) newRequest(method, token, path string, body any) *http.Request {
	var bodyReader io.Reader
	if body != nil {
		buf := new(bytes.Buffer)
		_ = json.NewEncoder(buf).Encode(body)
		bodyReader = buf
	}
	req, err := http.NewRequest(method, f.url(path), bodyReader)
	if err != nil {
		f.t.Fatalf("newRequest(%s %s): %v", method, path, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

type submitWorkflowRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params,omitempty"`
}

type submitWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

func testWorkflow(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:  name,
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	}
}

func (f *tenantIsolationFixture) submitWorkflow(token, name string) types.ExecutionID {
	body := submitWorkflowRequest{Workflow: testWorkflow(name)}
	req := f.newRequest(http.MethodPost, token, "/v1/workflows", body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submitWorkflow status = %d, want 200", resp.StatusCode)
	}
	var out submitWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		f.t.Fatalf("decode submit response: %v", err)
	}
	return out.ExecutionID
}

func (f *tenantIsolationFixture) submitWorkflowForgedTenant(token, name, forgedTenant string) types.ExecutionID {
	raw := map[string]any{
		"workflow": map[string]any{
			"name":  name,
			"nodes": []map[string]any{{"name": "start", "type": "test.echo"}},
		},
		"tenant": forgedTenant,
	}
	req := f.newRequest(http.MethodPost, token, "/v1/workflows", raw)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submitWorkflowForgedTenant status = %d, want 200", resp.StatusCode)
	}
	var out submitWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		f.t.Fatalf("decode submit response: %v", err)
	}
	return out.ExecutionID
}

func (f *tenantIsolationFixture) do(req *http.Request) *http.Response {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("http do %s %s: %v", req.Method, req.URL.Path, err)
	}
	return resp
}

func (f *tenantIsolationFixture) status(method, token, path string, body any) int {
	req := f.newRequest(method, token, path, body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func (f *tenantIsolationFixture) getExecution(token string, execID types.ExecutionID) (int, engine.ExecutionDetail) {
	req := f.newRequest(http.MethodGet, token, "/v1/executions/"+string(execID), nil)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	_ = json.NewDecoder(resp.Body).Decode(&detail)
	return resp.StatusCode, detail
}

func (f *tenantIsolationFixture) getManagementExecution(token string, execID types.ExecutionID) int {
	return f.status(http.MethodGet, token, "/v1/management/executions/"+string(execID), nil)
}

func (f *tenantIsolationFixture) cancelExecution(token string, execID types.ExecutionID) int {
	return f.status(http.MethodPost, token, "/v1/executions/"+string(execID)+"/cancel", nil)
}

type deadLetterListResponse struct {
	Entries    []engine.OutboxEntry `json:"entries"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

type deadLetterReplayRequest struct {
	EntryID   string `json:"entry_id"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

type deadLetterReplayResponse struct {
	Outcome      string `json:"outcome"`
	AuditID      string `json:"audit_id,omitempty"`
	ExecutionID  string `json:"execution_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	ActivationID string `json:"activation_id,omitempty"`
}

func (f *tenantIsolationFixture) listDeadLetters(token string, execID types.ExecutionID) (int, []engine.OutboxEntry) {
	req := f.newRequest(http.MethodGet, token, "/v1/management/dead-letters/"+string(execID), nil)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var list deadLetterListResponse
	_ = json.NewDecoder(resp.Body).Decode(&list)
	return resp.StatusCode, list.Entries
}

func (f *tenantIsolationFixture) replayDeadLetter(token string, execID types.ExecutionID, entryID string) (int, deadLetterReplayResponse) {
	body := deadLetterReplayRequest{EntryID: entryID, RequestID: "req-" + entryID, Reason: "tenant-isolation-test"}
	req := f.newRequest(http.MethodPost, token, "/v1/management/dead-letters/"+string(execID)+"/replay", body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var out deadLetterReplayResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestTenantIsolationOwnerCanAccessWorkflow covers the matrix row:
//   tenant A submits workflow -> tenant A can get/exec.
func TestTenantIsolationOwnerCanAccessWorkflow(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "owner-wf")

	if code, detail := f.getExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA control get = %d, want 200", code)
	} else if detail.ExecutionID != execID {
		t.Fatalf("tenantA control get execution_id = %q, want %q", detail.ExecutionID, execID)
	}

	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA management get = %d, want 200", code)
	}

	if code := f.cancelExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA cancel (exec) = %d, want 200", code)
	}
}

// TestTenantIsolationCrossTenantGetNotFound covers:
//   tenant B attempts to get tenant A's workflow -> NotFound.
func TestTenantIsolationCrossTenantGetNotFound(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "get-wf")

	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB control get = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB management get = %d, want 404", code)
	}
}

// TestTenantIsolationCrossTenantExecuteRejected covers:
//   tenant B attempts to exec tenant A's workflow -> Forbidden / NotFound.
// The cancel endpoint exercises the execution mutation surface.
func TestTenantIsolationCrossTenantExecuteRejected(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "exec-wf")

	if code := f.cancelExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB cancel tenantA workflow = %d, want 404", code)
	}
}

// TestTenantIsolationCrossTenantInspectNotFound covers:
//   tenant B attempts to inspect tenant A's execution -> 404.
func TestTenantIsolationCrossTenantInspectNotFound(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "inspect-wf")

	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB inspect control = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB inspect management = %d, want 404", code)
	}
}

// TestTenantIsolationDeadLetterCrossTenant covers:
//   tenant B attempts to list/replay tenant A's dead-letters -> 404.
func TestTenantIsolationDeadLetterCrossTenant(t *testing.T) {
	f := newTenantIsolationFixture(t)

	ctxA := tenant.WithTenant(context.Background(), tenant.TenantID(testTenantA))
	g, err := graph.Compile(testWorkflow("dead-wf"))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	execID, err := f.seedEng.Submit(ctxA, g, nil)
	if err != nil {
		t.Fatalf("seed Submit: %v", err)
	}
	_ = f.seedEng.FlushOutbox(ctxA, execID)

	code, entries := f.listDeadLetters("tok-a", execID)
	if code != http.StatusOK {
		t.Fatalf("tenantA list dead letters = %d, want 200", code)
	}
	if len(entries) != 1 {
		t.Fatalf("tenantA dead letters = %d entries, want 1", len(entries))
	}
	entryID := entries[0].ID

	if code, _ := f.listDeadLetters("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB list tenantA dead letters = %d, want 404", code)
	}

	if code, _ := f.replayDeadLetter("tok-b", execID, entryID); code != http.StatusNotFound {
		t.Fatalf("tenantB replay tenantA dead letter = %d, want 404", code)
	}

	// Same-tenant replay succeeds.
	if code, out := f.replayDeadLetter("tok-a", execID, entryID); code != http.StatusOK {
		t.Fatalf("tenantA replay own dead letter = %d, want 200", code)
	} else if out.Outcome != string(engine.ReplayReplayed) {
		t.Fatalf("tenantA replay outcome = %q, want replayed", out.Outcome)
	}
}

// TestTenantIsolationSameWorkflowNameNoConflict covers:
//   tenant A submits workflow, tenant B submits same-name workflow -> no conflict.
func TestTenantIsolationSameWorkflowNameNoConflict(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execA := f.submitWorkflow("tok-a", "same-name-wf")
	execB := f.submitWorkflow("tok-b", "same-name-wf")

	if execA == execB {
		t.Fatalf("tenantA and tenantB executions share id %q (want distinct)", execA)
	}

	// Each tenant sees only its own execution.
	for _, tc := range []struct {
		token  string
		ownID  types.ExecutionID
		otherID types.ExecutionID
	}{
		{"tok-a", execA, execB},
		{"tok-b", execB, execA},
	} {
		if code := f.getManagementExecution(tc.token, tc.ownID); code != http.StatusOK {
			t.Fatalf("%s sees own exec = %d, want 200", tc.token, code)
		}
		if code := f.getManagementExecution(tc.token, tc.otherID); code != http.StatusNotFound {
			t.Fatalf("%s sees other tenant exec = %d, want 404", tc.token, code)
		}
	}
}

// TestTenantIsolationDuplicateSubmitDoesNotCrossTenantFence covers:
//   duplicate submit / lease-token fencing does not cross tenant boundaries.
// We deliberately create two executions with the SAME execution ID under
// different tenants and verify that a lease token acquired for tenant A's node
// cannot be used to commit tenant B's node.
func TestTenantIsolationDuplicateSubmitDoesNotCrossTenantFence(t *testing.T) {
	f := newTenantIsolationFixture(t)

	eng := f.cp.Engine().(*engine.Engine)
	state := eng.State()

	g, err := graph.Compile(testWorkflow("fence-wf"))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	sharedExecID := types.ExecutionID("exec-fence-shared")
	snap := &engine.ExecutionSnapshot{
		ID:     sharedExecID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}

	ctxA := tenant.WithTenant(context.Background(), tenant.TenantID(testTenantA))
	ctxB := tenant.WithTenant(context.Background(), tenant.TenantID(testTenantB))

	if err := state.CreateExecution(ctxA, snap); err != nil {
		t.Fatalf("create execution tenantA: %v", err)
	}
	if err := state.CreateExecution(ctxB, snap); err != nil {
		t.Fatalf("create execution tenantB: %v", err)
	}

	leaseA := &engine.TaskLease{
		LeaseID:    "lease-a",
		LeaseToken: "token-a",
		Task: engine.Task{
			ExecutionID:  sharedExecID,
			NodeName:     "start",
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		IssuedAt: time.Now().UTC().Add(-time.Second),
		TTL:      time.Minute,
	}

	_, acquired, err := state.AcquireTaskLease(ctxA, leaseA)
	if err != nil {
		t.Fatalf("acquire lease A: %v", err)
	}
	if !acquired {
		t.Fatal("acquire lease A = false, want true")
	}

	atomicState := state.(engine.AtomicStateStore)

	// Tenant A commits its own node successfully.
	commitA := engine.CommitNodeRequest{
		ExecutionID:  sharedExecID,
		NodeName:     "start",
		NodeIdx:      0,
		ActivationID: 1,
		LeaseID:      leaseA.LeaseID,
		LeaseToken:   leaseA.LeaseToken,
		Attempt:      1,
		Status:       types.NodeStatusSuccess,
	}
	resA, err := atomicState.CommitNode(ctxA, commitA)
	if err != nil {
		t.Fatalf("commit tenantA node: %v", err)
	}
	if resA.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("commit tenantA outcome = %v, want accepted", resA.Outcome)
	}

	// Tenant B using tenant A's lease token must NOT be able to commit its node.
	resB, err := atomicState.CommitNode(ctxB, commitA)
	if err != nil {
		t.Fatalf("commit tenantB node: %v", err)
	}
	if resB.Outcome != engine.CommitOutcomeStaleToken {
		t.Fatalf("cross-tenant commit outcome = %v, want stale_token", resB.Outcome)
	}
}

// TestTenantIsolationSweeperScanDoesNotCrossTenant covers:
//   tenant A's sweeper SCAN does not see tenant B's keys.
func TestTenantIsolationSweeperScanDoesNotCrossTenant(t *testing.T) {
	f := newTenantIsolationFixture(t)
	ctx := context.Background()

	sharedExecID := "sweep-test-exec"
	member := fmt.Sprintf("%s|start", sharedExecID)
	now := time.Now().UnixMilli()

	rdb := f.backend.RedisClient()

	// Write identical lease-index keys for tenant A and tenant B.
	aKey := fmt.Sprintf("xflow:t%s:exec:{%s}:leases", testTenantA, sharedExecID)
	bKey := fmt.Sprintf("xflow:t%s:exec:{%s}:leases", testTenantB, sharedExecID)
	if err := rdb.ZAdd(ctx, aKey, redis.Z{Score: float64(now), Member: member}).Err(); err != nil {
		t.Fatalf("write tenantA lease index: %v", err)
	}
	if err := rdb.ZAdd(ctx, bKey, redis.Z{Score: float64(now), Member: member}).Err(); err != nil {
		t.Fatalf("write tenantB lease index: %v", err)
	}

	// A tenant-scoped SCAN pattern must return only tenant A's key.
	aPattern := fmt.Sprintf("xflow:t%s:exec:{*}:leases", testTenantA)
	aKeys, err := scanAllKeys(ctx, rdb, aPattern)
	if err != nil {
		t.Fatalf("scan tenantA: %v", err)
	}
	if len(aKeys) != 1 || aKeys[0] != aKey {
		t.Fatalf("tenantA scan = %v, want [%s]", aKeys, aKey)
	}

	// The cross-tenant wildcard confirms both keys exist globally.
	allKeys, err := scanAllKeys(ctx, rdb, "xflow:*:exec:{*}:leases")
	if err != nil {
		t.Fatalf("scan global: %v", err)
	}
	if len(allKeys) != 2 {
		t.Fatalf("global scan = %d keys, want 2", len(allKeys))
	}
}

func scanAllKeys(ctx context.Context, rdb redis.Cmdable, pattern string) ([]string, error) {
	var out []string
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// TestTenantIsolationRunnerAssignmentDoesNotCrossTenant covers:
//   runner assigned to tenant A does not consume tenant B's assignment.
func TestTenantIsolationRunnerAssignmentDoesNotCrossTenant(t *testing.T) {
	f := newTenantIsolationFixture(t)
	ctx := context.Background()

	dir := f.cp.RunnerDirectory()

	sessionA, err := dir.Register(ctx, control.RegisterRunnerRequest{
		RunnerID:     "runner-a",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Tenants:      []tenant.TenantID{tenant.TenantID(testTenantA)},
	})
	if err != nil {
		t.Fatalf("register runner-a: %v", err)
	}

	// Enqueue an assignment that belongs to tenant B.
	task := engine.Task{
		ExecutionID:  "exec-b",
		NodeName:     "start",
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: 1,
	}
	assignment := control.Assignment{
		AssignmentID: control.BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
		TenantID:     tenant.TenantID(testTenantB),
	}
	if enqueued, err := dir.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("enqueue tenantB assignment: %v", err)
	} else if !enqueued {
		t.Fatal("tenantB assignment enqueued = false, want true")
	}

	_, ok, err := dir.ClaimForRunner(ctx, control.ClaimRequest{
		RunnerID:     sessionA.RunnerID,
		SessionID:    sessionA.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("runner-a claim: %v", err)
	}
	if ok {
		t.Fatal("runner-a claimed tenantB assignment, want cross-tenant isolation")
	}

	// Runner B serving tenant B can claim the assignment.
	sessionB, err := dir.Register(ctx, control.RegisterRunnerRequest{
		RunnerID:     "runner-b",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Tenants:      []tenant.TenantID{tenant.TenantID(testTenantB)},
	})
	if err != nil {
		t.Fatalf("register runner-b: %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, control.ClaimRequest{
		RunnerID:     sessionB.RunnerID,
		SessionID:    sessionB.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("runner-b claim: %v", err)
	}
	if !ok {
		t.Fatal("runner-b did not claim tenantB assignment")
	}
	if claim.Assignment.TenantID != tenant.TenantID(testTenantB) {
		t.Fatalf("claimed assignment tenant = %q, want tenantB", claim.Assignment.TenantID)
	}
}

// TestTenantIsolationRequestBodyTenantIgnored covers:
//   request body forges tenant: "B" (principal is A) -> ignored, execute as A.
func TestTenantIsolationRequestBodyTenantIgnored(t *testing.T) {
	f := newTenantIsolationFixture(t)
	execID := f.submitWorkflowForgedTenant("tok-a", "forged-wf", testTenantB)

	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("tenantA inspect (forged body tenant) = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("tenantB inspect (forged body tenant) = %d, want 404", code)
	}
}
