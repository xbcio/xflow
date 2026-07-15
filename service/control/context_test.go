package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

type requestContextKey struct{}

const requestContextMetadataKey = "x-xflow-request-context"

type contextRecordingRunnerDirectory struct {
	RunnerDirectory

	mu       sync.RWMutex
	contexts map[string]context.Context
}

func newContextRecordingRunnerDirectory(directory RunnerDirectory) *contextRecordingRunnerDirectory {
	return &contextRecordingRunnerDirectory{
		RunnerDirectory: directory,
		contexts:        make(map[string]context.Context),
	}
}

func (d *contextRecordingRunnerDirectory) Register(ctx context.Context, req RegisterRunnerRequest) (RunnerSession, error) {
	d.record("register", ctx)
	return d.RunnerDirectory.Register(ctx, req)
}

func (d *contextRecordingRunnerDirectory) Heartbeat(ctx context.Context, req HeartbeatRequest) error {
	d.record("heartbeat", ctx)
	return d.RunnerDirectory.Heartbeat(ctx, req)
}

func (d *contextRecordingRunnerDirectory) ClaimForRunner(ctx context.Context, req ClaimRequest) (Claim, bool, error) {
	d.record("claim", ctx)
	return d.RunnerDirectory.ClaimForRunner(ctx, req)
}

func (d *contextRecordingRunnerDirectory) FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	d.record("finalize", ctx)
	return d.RunnerDirectory.FinalizeClaim(ctx, claimID, lease)
}

func (d *contextRecordingRunnerDirectory) ReleaseClaim(ctx context.Context, claimID ClaimID, reason ReleaseClaimReason) error {
	d.record("release", ctx)
	return d.RunnerDirectory.ReleaseClaim(ctx, claimID, reason)
}

func (d *contextRecordingRunnerDirectory) record(operation string, ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.contexts[operation] = ctx
}

func (d *contextRecordingRunnerDirectory) contextFor(operation string) context.Context {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.contexts[operation]
}

type contextRecordingEngine struct {
	*fakeControlEngine

	mu                sync.RWMutex
	buildLeaseContext context.Context
}

func (e *contextRecordingEngine) BuildTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error) {
	e.mu.Lock()
	e.buildLeaseContext = ctx
	e.mu.Unlock()
	return e.fakeControlEngine.BuildTaskLease(ctx, task)
}

func (e *contextRecordingEngine) contextForBuildLease() context.Context {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.buildLeaseContext
}

func TestHTTPRunnerOperationsPropagateRequestContext(t *testing.T) {
	lease := contextPropagationLease()
	engine := &contextRecordingEngine{fakeControlEngine: &fakeControlEngine{buildLease: lease}}
	runners := newContextRecordingRunnerDirectory(NewMemoryRunnerDirectory())
	handler := NewServer(engine, runners).Handler()

	var registered protocol.RegisterRunnerResponse
	postJSONWithContext(t, handler, httpContext("register"), protocol.RegisterRunnerPath, contextPropagationRegisterRequest(), &registered)
	if registered.SessionID == "" {
		t.Fatal("Register returned an empty session ID")
	}

	postJSONWithContext(t, handler, httpContext("heartbeat"), protocol.HeartbeatPath, protocol.HeartbeatRequest{
		RunnerID:  registered.RunnerID,
		SessionID: registered.SessionID,
		Capacity:  1,
	}, nil)

	enqueueContextPropagationAssignment(t, runners, lease)
	var polled protocol.PollTaskResponse
	postJSONWithContext(t, handler, httpContext("poll"), protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     registered.RunnerID,
		SessionID:    registered.SessionID,
		Capacity:     1,
		Capabilities: contextPropagationCapabilities(),
	}, &polled)
	if polled.Lease == nil {
		t.Fatal("PollTask returned no lease")
	}

	assertHTTPContextValue(t, runners.contextFor("register"), "register")
	assertHTTPContextValue(t, runners.contextFor("heartbeat"), "heartbeat")
	assertHTTPContextValue(t, runners.contextFor("claim"), "poll")
	assertHTTPContextValue(t, runners.contextFor("finalize"), "poll")
	assertHTTPContextValue(t, engine.contextForBuildLease(), "poll")
}

func TestGRPCRunnerOperationsPropagateIncomingContext(t *testing.T) {
	lease := contextPropagationLease()
	engine := &contextRecordingEngine{fakeControlEngine: &fakeControlEngine{buildLease: lease}}
	runners := newContextRecordingRunnerDirectory(NewMemoryRunnerDirectory())
	client := startGRPCTestServer(t, engine, runners)

	registered, err := client.Register(grpcContext("register"), contextPropagationRegisterRequest())
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if registered.SessionID == "" {
		t.Fatal("Register returned an empty session ID")
	}

	if _, err := client.Heartbeat(grpcContext("heartbeat"), protocol.HeartbeatRequest{
		RunnerID:  registered.RunnerID,
		SessionID: registered.SessionID,
		Capacity:  1,
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	enqueueContextPropagationAssignment(t, runners, lease)
	polled, err := client.Poll(grpcContext("poll"), protocol.PollTaskRequest{
		RunnerID:     registered.RunnerID,
		SessionID:    registered.SessionID,
		Capacity:     1,
		Capabilities: contextPropagationCapabilities(),
	})
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if polled.Lease == nil {
		t.Fatal("Poll() returned no lease")
	}

	assertGRPCContextValue(t, runners.contextFor("register"), "register")
	assertGRPCContextValue(t, runners.contextFor("heartbeat"), "heartbeat")
	assertGRPCContextValue(t, runners.contextFor("claim"), "poll")
	assertGRPCContextValue(t, runners.contextFor("finalize"), "poll")
	assertGRPCContextValue(t, engine.contextForBuildLease(), "poll")
}

func contextPropagationLease() *engine.TaskLease {
	task := engine.Task{
		ExecutionID: "context-execution",
		NodeName:    "context-node",
		NodeIdx:     1,
	}
	return &engine.TaskLease{
		LeaseID:    "context-lease",
		LeaseToken: "context-token",
		Task:       task,
		NodeType:   "xflow.function",
	}
}

func contextPropagationRegisterRequest() protocol.RegisterRunnerRequest {
	return protocol.RegisterRunnerRequest{
		RunnerID:     "context-runner",
		Concurrency:  1,
		Capabilities: contextPropagationCapabilities(),
	}
}

func contextPropagationCapabilities() []protocol.Capability {
	return []protocol.Capability{{NodeType: "xflow.function"}}
}

func enqueueContextPropagationAssignment(t *testing.T, runners RunnerDirectory, lease *engine.TaskLease) {
	t.Helper()
	enqueued, err := runners.EnqueueAssignment(context.Background(), Assignment{
		AssignmentID: BuildAssignmentID(&lease.Task),
		Task:         lease.Task,
		Routing:      engine.TaskRouting{NodeType: lease.NodeType},
	})
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() enqueued=false, want true")
	}
}

func httpContext(value string) context.Context {
	return context.WithValue(context.Background(), requestContextKey{}, value)
}

func grpcContext(value string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), requestContextMetadataKey, value)
}

func postJSONWithContext(t *testing.T, handler http.Handler, ctx context.Context, path string, body any, out any) {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, path, &buf).WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("POST %s status = %d, want %d", path, response.Code, http.StatusOK)
	}
	if out == nil {
		return
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func assertHTTPContextValue(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	if ctx == nil {
		t.Fatal("request context was not forwarded")
	}
	if got := ctx.Value(requestContextKey{}); got != want {
		t.Fatalf("request context value = %v, want %q", got, want)
	}
}

func assertGRPCContextValue(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	if ctx == nil {
		t.Fatal("incoming context was not forwarded")
	}
	metadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		t.Fatal("forwarded context has no incoming metadata")
	}
	if got := metadata.Get(requestContextMetadataKey); len(got) != 1 || got[0] != want {
		t.Fatalf("incoming metadata = %v, want %q=%q", got, requestContextMetadataKey, want)
	}
}
