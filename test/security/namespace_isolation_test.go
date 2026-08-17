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

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// errFailingQueue is returned by failingQueue.Enqueue to simulate a permanent
// queue outage so outbox entries dead-letter after maxAttempts.
var errFailingQueue = errors.New("namespace isolation test: queue unavailable")

type failingQueue struct{}

func (failingQueue) Enqueue(context.Context, *engine.Task) error { return errFailingQueue }
func (failingQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return errFailingQueue
}

const (
	testNamespaceA = "namespaceA"
	testNamespaceB = "namespaceB"
)

var testScopes = []string{
	"workflow",
	"execution",
	"deadletter.list",
	"deadletter.replay",
	"management.read",
	"management.write",
}

// namespaceIsolationFixture wires a miniredis-backed distributed backend +
// control plane + APIServer with a multi-namespace token registry:
// tok-a -> namespaceA, tok-b -> namespaceB. Both tokens carry the scopes required to
// exercise the workflow, execution, dead-letter, and management surfaces.
type namespaceIsolationFixture struct {
	t       *testing.T
	srv     *apiserver.APIServer
	httpSrv *httptest.Server
	backend *distributed.Backend
	cp      *control.ControlPlane
	seedEng *engine.Engine
}

func newNamespaceIsolationFixture(t *testing.T) *namespaceIsolationFixture {
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
		{Token: "tok-a", Subject: "op-a", Namespace: testNamespaceA, Scopes: testScopes},
		{Token: "tok-b", Subject: "op-b", Namespace: testNamespaceB, Scopes: testScopes},
	})

	srv, err := apiserver.New(apiserver.Config{
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    apiserver.NamespaceAwareAuthorizer{},
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

	return &namespaceIsolationFixture{
		t:       t,
		srv:     srv,
		httpSrv: httpSrv,
		backend: backend,
		cp:      cp,
		seedEng: seedEng,
	}
}

func (f *namespaceIsolationFixture) url(path string) string {
	return f.httpSrv.URL + path
}

func (f *namespaceIsolationFixture) newRequest(method, token, path string, body any) *http.Request {
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

func (f *namespaceIsolationFixture) submitWorkflow(token, name string) types.ExecutionID {
	body := submitWorkflowRequest{Workflow: testWorkflow(name)}
	req := f.newRequest(http.MethodPost, token, "/v1/workflows/execute", body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submitWorkflow status = %d, want 200", resp.StatusCode)
	}
	var env struct {
		Data submitWorkflowResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		f.t.Fatalf("decode submit response: %v", err)
	}
	return env.Data.ExecutionID
}

func (f *namespaceIsolationFixture) submitWorkflowForgedNamespace(token, name, forgedNamespace string) types.ExecutionID {
	raw := map[string]any{
		"workflow": map[string]any{
			"name":  name,
			"nodes": []map[string]any{{"name": "start", "type": "test.echo"}},
		},
		"namespace": forgedNamespace,
	}
	req := f.newRequest(http.MethodPost, token, "/v1/workflows/execute", raw)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		f.t.Fatalf("submitWorkflowForgedNamespace status = %d, want 200", resp.StatusCode)
	}
	var env struct {
		Data submitWorkflowResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		f.t.Fatalf("decode submit response: %v", err)
	}
	return env.Data.ExecutionID
}

func (f *namespaceIsolationFixture) do(req *http.Request) *http.Response {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("http do %s %s: %v", req.Method, req.URL.Path, err)
	}
	return resp
}

func (f *namespaceIsolationFixture) status(method, token, path string, body any) int {
	req := f.newRequest(method, token, path, body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func (f *namespaceIsolationFixture) getExecution(token string, execID types.ExecutionID) (int, engine.ExecutionDetail) {
	req := f.newRequest(http.MethodGet, token, "/v1/executions/"+string(execID), nil)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	decodeEnvelopeData(f.t, resp, &detail)
	return resp.StatusCode, detail
}

// decodeEnvelopeData unwraps the spec §3.1 envelope and decodes its data field
// into out. Every user-face route in this fixture returns the envelope, so a
// direct decode into the typed target would silently yield a zero value — the
// field names live one level down under "data". A failure envelope carries
// data:null, which leaves out at its zero value without an error; the caller
// asserts on the status code in that case.
//
// The decode errors are asserted rather than discarded: these helpers back
// IDOR assertions, and a helper that swallows a decode failure turns a real
// cross-namespace leak into a passing test.
func decodeEnvelopeData(t *testing.T, resp *http.Response, out any) {
	t.Helper()
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		t.Fatalf("decode envelope data: %v (data=%s)", err, env.Data)
	}
}

func (f *namespaceIsolationFixture) getManagementExecution(token string, execID types.ExecutionID) int {
	return f.status(http.MethodGet, token, "/v1/management/executions/"+string(execID), nil)
}

func (f *namespaceIsolationFixture) cancelExecution(token string, execID types.ExecutionID) int {
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

func (f *namespaceIsolationFixture) listDeadLetters(token string, execID types.ExecutionID) (int, []engine.OutboxEntry) {
	req := f.newRequest(http.MethodGet, token, "/v1/management/dead-letters/"+string(execID), nil)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var list deadLetterListResponse
	decodeEnvelopeData(f.t, resp, &list)
	return resp.StatusCode, list.Entries
}

func (f *namespaceIsolationFixture) replayDeadLetter(token string, execID types.ExecutionID, entryID string) (int, deadLetterReplayResponse) {
	body := deadLetterReplayRequest{EntryID: entryID, RequestID: "req-" + entryID, Reason: "namespace-isolation-test"}
	req := f.newRequest(http.MethodPost, token, "/v1/management/dead-letters/"+string(execID)+"/replay", body)
	resp := f.do(req)
	defer func() { _ = resp.Body.Close() }()
	var out deadLetterReplayResponse
	decodeEnvelopeData(f.t, resp, &out)
	return resp.StatusCode, out
}

// TestNamespaceIsolationOwnerCanAccessWorkflow covers the matrix row:
//
//	namespace A submits workflow -> namespace A can get/exec.
func TestNamespaceIsolationOwnerCanAccessWorkflow(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "owner-wf")

	if code, detail := f.getExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA control get = %d, want 200", code)
	} else if detail.ExecutionID != execID {
		t.Fatalf("namespaceA control get execution_id = %q, want %q", detail.ExecutionID, execID)
	}

	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA management get = %d, want 200", code)
	}

	if code := f.cancelExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA cancel (exec) = %d, want 200", code)
	}
}

// TestNamespaceIsolationCrossNamespaceGetNotFound covers:
//
//	namespace B attempts to get namespace A's workflow -> NotFound.
func TestNamespaceIsolationCrossNamespaceGetNotFound(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "get-wf")

	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB control get = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB management get = %d, want 404", code)
	}
}

// TestNamespaceIsolationCrossNamespaceExecuteRejected covers:
//
//	namespace B attempts to exec namespace A's workflow -> Forbidden / NotFound.
//
// The cancel endpoint exercises the execution mutation surface.
func TestNamespaceIsolationCrossNamespaceExecuteRejected(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "exec-wf")

	if code := f.cancelExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB cancel namespaceA workflow = %d, want 404", code)
	}
}

// TestNamespaceIsolationCrossNamespaceInspectNotFound covers:
//
//	namespace B attempts to inspect namespace A's execution -> 404.
func TestNamespaceIsolationCrossNamespaceInspectNotFound(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execID := f.submitWorkflow("tok-a", "inspect-wf")

	if code, _ := f.getExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB inspect control = %d, want 404", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB inspect management = %d, want 404", code)
	}
}

// TestNamespaceIsolationDeadLetterCrossNamespace covers:
//
//	namespace B attempts to list/replay namespace A's dead-letters -> 404.
func TestNamespaceIsolationDeadLetterCrossNamespace(t *testing.T) {
	f := newNamespaceIsolationFixture(t)

	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace(testNamespaceA))
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
		t.Fatalf("namespaceA list dead letters = %d, want 200", code)
	}
	if len(entries) != 1 {
		t.Fatalf("namespaceA dead letters = %d entries, want 1", len(entries))
	}
	entryID := entries[0].ID

	if code, _ := f.listDeadLetters("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB list namespaceA dead letters = %d, want 404", code)
	}

	if code, _ := f.replayDeadLetter("tok-b", execID, entryID); code != http.StatusNotFound {
		t.Fatalf("namespaceB replay namespaceA dead letter = %d, want 404", code)
	}

	// Same-namespace replay succeeds.
	if code, out := f.replayDeadLetter("tok-a", execID, entryID); code != http.StatusOK {
		t.Fatalf("namespaceA replay own dead letter = %d, want 200", code)
	} else if out.Outcome != string(engine.ReplayReplayed) {
		t.Fatalf("namespaceA replay outcome = %q, want replayed", out.Outcome)
	}
}

// TestNamespaceIsolationSameWorkflowNameNoConflict covers:
//
//	namespace A submits workflow, namespace B submits same-name workflow -> no conflict.
func TestNamespaceIsolationSameWorkflowNameNoConflict(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execA := f.submitWorkflow("tok-a", "same-name-wf")
	execB := f.submitWorkflow("tok-b", "same-name-wf")

	if execA == execB {
		t.Fatalf("namespaceA and namespaceB executions share id %q (want distinct)", execA)
	}

	// Each namespace sees only its own execution.
	for _, tc := range []struct {
		token   string
		ownID   types.ExecutionID
		otherID types.ExecutionID
	}{
		{"tok-a", execA, execB},
		{"tok-b", execB, execA},
	} {
		if code := f.getManagementExecution(tc.token, tc.ownID); code != http.StatusOK {
			t.Fatalf("%s sees own exec = %d, want 200", tc.token, code)
		}
		if code := f.getManagementExecution(tc.token, tc.otherID); code != http.StatusNotFound {
			t.Fatalf("%s sees other namespace exec = %d, want 404", tc.token, code)
		}
	}
}

// TestNamespaceIsolationDuplicateSubmitDoesNotCrossNamespaceFence covers:
//
//	duplicate submit / lease-token fencing does not cross namespace boundaries.
//
// We deliberately create two executions with the SAME execution ID under
// different namespaces and verify that a lease token acquired for namespace A's node
// cannot be used to commit namespace B's node.
func TestNamespaceIsolationDuplicateSubmitDoesNotCrossNamespaceFence(t *testing.T) {
	f := newNamespaceIsolationFixture(t)

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

	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace(testNamespaceA))
	ctxB := namespace.WithNamespace(context.Background(), namespace.Namespace(testNamespaceB))

	if err := state.CreateExecution(ctxA, snap); err != nil {
		t.Fatalf("create execution namespaceA: %v", err)
	}
	if err := state.CreateExecution(ctxB, snap); err != nil {
		t.Fatalf("create execution namespaceB: %v", err)
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

	// Namespace A commits its own node successfully.
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
		t.Fatalf("commit namespaceA node: %v", err)
	}
	if resA.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("commit namespaceA outcome = %v, want accepted", resA.Outcome)
	}

	// Namespace B using namespace A's lease token must NOT be able to commit its node.
	resB, err := atomicState.CommitNode(ctxB, commitA)
	if err != nil {
		t.Fatalf("commit namespaceB node: %v", err)
	}
	if resB.Outcome != engine.CommitOutcomeStaleToken {
		t.Fatalf("cross-namespace commit outcome = %v, want stale_token", resB.Outcome)
	}
}

// TestNamespaceIsolationSweeperScanDoesNotCrossNamespace covers:
//
//	namespace A's sweeper SCAN does not see namespace B's keys.
func TestNamespaceIsolationSweeperScanDoesNotCrossNamespace(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	ctx := context.Background()

	sharedExecID := "sweep-test-exec"
	member := fmt.Sprintf("%s|start", sharedExecID)
	now := time.Now().UnixMilli()

	rdb := f.backend.RedisClient()

	// Write identical lease-index keys for namespace A and namespace B.
	aKey := fmt.Sprintf("xflow:ns:%s:exec:{%s}:leases", testNamespaceA, sharedExecID)
	bKey := fmt.Sprintf("xflow:ns:%s:exec:{%s}:leases", testNamespaceB, sharedExecID)
	if err := rdb.ZAdd(ctx, aKey, redis.Z{Score: float64(now), Member: member}).Err(); err != nil {
		t.Fatalf("write namespaceA lease index: %v", err)
	}
	if err := rdb.ZAdd(ctx, bKey, redis.Z{Score: float64(now), Member: member}).Err(); err != nil {
		t.Fatalf("write namespaceB lease index: %v", err)
	}

	// A namespace-scoped SCAN pattern must return only namespace A's key.
	aPattern := fmt.Sprintf("xflow:ns:%s:exec:{*}:leases", testNamespaceA)
	aKeys, err := scanAllKeys(ctx, rdb, aPattern)
	if err != nil {
		t.Fatalf("scan namespaceA: %v", err)
	}
	if len(aKeys) != 1 || aKeys[0] != aKey {
		t.Fatalf("namespaceA scan = %v, want [%s]", aKeys, aKey)
	}

	// The cross-namespace wildcard confirms both keys exist globally.
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

// TestNamespaceIsolationRunnerAssignmentDoesNotCrossNamespace covers:
//
//	runner assigned to namespace A does not consume namespace B's assignment.
func TestNamespaceIsolationRunnerAssignmentDoesNotCrossNamespace(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	ctx := context.Background()

	dir := f.cp.RunnerDirectory()

	sessionA, err := dir.Register(ctx, control.RegisterRunnerRequest{
		RunnerID:     "runner-a",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Namespaces:   []namespace.Namespace{namespace.Namespace(testNamespaceA)},
	})
	if err != nil {
		t.Fatalf("register runner-a: %v", err)
	}

	// Enqueue an assignment that belongs to namespace B.
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
		Namespace:    namespace.Namespace(testNamespaceB),
	}
	if enqueued, err := dir.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("enqueue namespaceB assignment: %v", err)
	} else if !enqueued {
		t.Fatal("namespaceB assignment enqueued = false, want true")
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
		t.Fatal("runner-a claimed namespaceB assignment, want cross-namespace isolation")
	}

	// Runner B serving namespace B can claim the assignment.
	sessionB, err := dir.Register(ctx, control.RegisterRunnerRequest{
		RunnerID:     "runner-b",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Namespaces:   []namespace.Namespace{namespace.Namespace(testNamespaceB)},
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
		t.Fatal("runner-b did not claim namespaceB assignment")
	}
	if claim.Assignment.Namespace != namespace.Namespace(testNamespaceB) {
		t.Fatalf("claimed assignment namespace = %q, want namespaceB", claim.Assignment.Namespace)
	}
}

// TestNamespaceIsolationRequestBodyNamespaceIgnored covers:
//
//	request body forges namespace: "B" (principal is A) -> ignored, execute as A.
func TestNamespaceIsolationRequestBodyNamespaceIgnored(t *testing.T) {
	f := newNamespaceIsolationFixture(t)
	execID := f.submitWorkflowForgedNamespace("tok-a", "forged-wf", testNamespaceB)

	if code := f.getManagementExecution("tok-a", execID); code != http.StatusOK {
		t.Fatalf("namespaceA inspect (forged body namespace) = %d, want 200", code)
	}
	if code := f.getManagementExecution("tok-b", execID); code != http.StatusNotFound {
		t.Fatalf("namespaceB inspect (forged body namespace) = %d, want 404", code)
	}
}
