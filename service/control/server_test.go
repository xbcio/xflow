package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestHTTPRunnerRegisterPollAndResult(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	register := protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}
	var registerResp protocol.RegisterRunnerResponse
	postJSON(t, server.URL+protocol.RegisterRunnerPath, register, http.StatusOK, &registerResp)
	if registerResp.RunnerID != "runner-1" {
		t.Fatalf("runner id = %q, want runner-1", registerResp.RunnerID)
	}
	if registerResp.SessionID == "" {
		t.Fatal("session id is empty")
	}

	lease := engine.TaskLease{
		LeaseID:     engine.LeaseID("lease-1"),
		LeaseToken:  engine.LeaseToken("token-1"),
		Task:        engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		NodeType:    "xflow.function",
		NodeVersion: 1,
	}
	fake.buildLease = &lease
	enqueued, err := dir.EnqueueAssignment(context.Background(), Assignment{
		AssignmentID: BuildAssignmentID(&lease.Task),
		Task:         lease.Task,
		Routing:      engine.TaskRouting{NodeType: lease.NodeType, NodeVersion: lease.NodeVersion},
	})
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() enqueued=false, want true")
	}

	var pollResp protocol.PollTaskResponse
	postJSON(t, server.URL+protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     "runner-1",
		SessionID:    registerResp.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}, http.StatusOK, &pollResp)
	if pollResp.Lease == nil || pollResp.Lease.LeaseID != lease.LeaseID {
		t.Fatalf("polled lease = %+v, want %+v", pollResp.Lease, lease)
	}

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  "runner-1",
		SessionID: registerResp.SessionID,
		Lease:     pollResp.Lease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusOK, &resultResp)
	if !resultResp.Accepted {
		t.Fatalf("result accepted = false, response %+v", resultResp)
	}
	if fake.committedLease == nil || fake.committedLease.LeaseID != lease.LeaseID {
		t.Fatalf("committed lease = %+v, want %+v", fake.committedLease, lease)
	}
	enqueued, err = dir.EnqueueAssignment(context.Background(), Assignment{
		AssignmentID: BuildAssignmentID(&lease.Task),
		Task:         lease.Task,
		Routing:      engine.TaskRouting{NodeType: lease.NodeType, NodeVersion: lease.NodeVersion},
	})
	if err != nil {
		t.Fatalf("requeue after report EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("requeue after report EnqueueAssignment() enqueued=false, want true")
	}
}

func TestHTTPPollRejectsStaleSession(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	register := protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}
	var first protocol.RegisterRunnerResponse
	postJSON(t, server.URL+protocol.RegisterRunnerPath, register, http.StatusOK, &first)
	var second protocol.RegisterRunnerResponse
	postJSON(t, server.URL+protocol.RegisterRunnerPath, register, http.StatusOK, &second)

	resp := postJSONRaw(t, server.URL+protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     "runner-1",
		SessionID:    first.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale poll status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}

func TestHTTPRegisterRejectsInvalidNamespace(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	register := protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Namespaces:   []string{"bad:namespace"},
	}
	resp := postJSONRaw(t, server.URL+protocol.RegisterRunnerPath, register)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("register invalid namespace status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHTTPRunnerSessionRequired(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	register := protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}
	var session protocol.RegisterRunnerResponse
	postJSON(t, server.URL+protocol.RegisterRunnerPath, register, http.StatusOK, &session)

	tests := []struct {
		name string
		url  string
		body any
	}{
		{
			name: "heartbeat",
			url:  protocol.HeartbeatPath,
			body: protocol.HeartbeatRequest{RunnerID: session.RunnerID, Capacity: 1, InFlight: 0},
		},
		{
			name: "poll",
			url:  protocol.PollTaskPath,
			body: protocol.PollTaskRequest{RunnerID: session.RunnerID, Capacity: 1},
		},
		{
			name: "report",
			url:  protocol.ReportResultPath,
			body: protocol.ReportResultRequest{
				RunnerID: session.RunnerID,
				Lease: &engine.TaskLease{
					LeaseID:  "lease-1",
					Task:     engine.Task{ExecutionID: "exec-1", NodeName: "start"},
					NodeType: "xflow.function",
				},
				Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := postJSONRaw(t, server.URL+tt.url, tt.body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s status = %d, want %d", tt.name, resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestCoreReportResultCleanupSurvivesReregistration(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := mustRegisterHTTPRunner(t, dir)
	assignment := stableTestAssignment("node-a")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Task:       claim.Assignment.Task,
		NodeType:   claim.Assignment.Routing.NodeType,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	var replacement RunnerSession
	fake := &fakeControlEngine{
		commitHook: func() {
			var err error
			replacement, err = dir.Register(ctx, RegisterRunnerRequest{
				RunnerID:     session.RunnerID,
				Capacity:     1,
				Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
				Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
			})
			if err != nil {
				t.Fatalf("Register() during commit error = %v", err)
			}
		},
	}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("reportResult() error = %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("reportResult() accepted = false, response %+v", resp)
	}
	if replacement.SessionID == "" {
		t.Fatal("replacement session id is empty")
	}
	if fake.committedLease == nil || fake.committedLease.LeaseToken != lease.LeaseToken {
		t.Fatalf("committed lease = %+v, want token %q", fake.committedLease, lease.LeaseToken)
	}

	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() after report error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() after report enqueued=false, want released seen state after commit-time re-registration")
	}
}

func TestHTTPReportResultRejectsStaleLeaseToken(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()
	session := mustRegisterHTTPRunner(t, dir)

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease: &engine.TaskLease{
			LeaseID:    engine.LeaseID("lease-1"),
			LeaseToken: engine.LeaseToken("stale"),
			Task:       engine.Task{ExecutionID: "exec-1", NodeName: "start"},
			NodeType:   "xflow.function",
		},
		Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resultResp)
	if resultResp.Accepted {
		t.Fatal("stale lease result should not be accepted")
	}
}

func TestHTTPReportResultPreservesRunnerError(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	ctx := context.Background()
	session := mustRegisterHTTPRunner(t, dir)
	assignment := stableTestAssignment("node-a")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Task:       claim.Assignment.Task,
		NodeType:   claim.Assignment.Routing.NodeType,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  "runner-1",
		SessionID: session.SessionID,
		Lease:     lease,
		Result:    engine.TaskResult{Error: errors.New("handler failed")},
	}, http.StatusOK, &resultResp)
	if !resultResp.Accepted {
		t.Fatalf("result accepted = false, response %+v", resultResp)
	}
	if fake.committedResult.Error == nil || fake.committedResult.Error.Error() != "handler failed" {
		t.Fatalf("committed error = %v, want handler failed", fake.committedResult.Error)
	}
}

func TestHTTPReportResultStaleTokenReleasesMatchingLeaseCapacity(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	ctx := context.Background()
	session := mustRegisterHTTPRunner(t, dir)

	first := stableTestAssignment("node-a")
	second := stableTestAssignment("node-b")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	firstClaim := mustClaimAssignment(t, ctx, dir, session)
	staleLease := &engine.TaskLease{
		LeaseID:    "lease-stale",
		LeaseToken: "token-stale",
		Task:       firstClaim.Assignment.Task,
		NodeType:   firstClaim.Assignment.Routing.NodeType,
	}
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, staleLease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	if _, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 1)); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want stale finalized lease to consume capacity before cleanup")
	}

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     staleLease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resultResp)
	if resultResp.Accepted {
		t.Fatal("stale lease result should not be accepted")
	}

	claim, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() after stale cleanup error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false after stale cleanup, want released capacity")
	}
	if claim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("claimed assignment = %q, want %q", claim.Assignment.AssignmentID, second.AssignmentID)
	}

	// The stale-token cleanup released the assignment without clearing its seen
	// mark. That mark records the dispatch; it does not make the work finished.
	// Nobody owns the assignment now, so it must stay re-dispatchable — a
	// rejection here is what stranded a map expansion's batch permanently.
	if enqueued, err := dir.EnqueueAssignment(ctx, first); err != nil {
		t.Fatalf("EnqueueAssignment(first) error = %v", err)
	} else if !enqueued {
		t.Fatal("EnqueueAssignment(first) enqueued=false after stale cleanup; the " +
			"assignment is unreachable and its parent would wait forever")
	}
}

func TestHTTPReportResultStaleTokenReleasesCapacityForInternalTaskIdentity(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	ctx := context.Background()
	session := mustRegisterHTTPRunner(t, dir)

	first := hiddenIdentityAssignment("node-a", 7, 3)
	second := stableTestAssignment("node-b")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	firstClaim := mustClaimAssignment(t, ctx, dir, session)
	staleLease := &engine.TaskLease{
		LeaseID:    "lease-hidden",
		LeaseToken: "token-hidden",
		Task:       firstClaim.Assignment.Task,
		NodeType:   firstClaim.Assignment.Routing.NodeType,
	}
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, staleLease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     staleLease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resultResp)
	if resultResp.Accepted {
		t.Fatal("stale lease result should not be accepted")
	}

	claim, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() after stale cleanup error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false after hidden-identity stale cleanup, want released capacity")
	}
	if claim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("claimed assignment = %q, want %q", claim.Assignment.AssignmentID, second.AssignmentID)
	}
}

func TestHTTPReportResultStaleTokenKeepsReplacementLeaseAndSeen(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	ctx := context.Background()
	session := mustRegisterHTTPRunner(t, dir)

	first := stableTestAssignment("node-a")
	second := stableTestAssignment("node-b")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	firstClaim := mustClaimAssignment(t, ctx, dir, session)
	staleLease := &engine.TaskLease{
		LeaseID:    "lease-old",
		LeaseToken: "token-old",
		Task:       firstClaim.Assignment.Task,
		NodeType:   firstClaim.Assignment.Routing.NodeType,
	}
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, staleLease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	replacement := engine.TaskLease{
		LeaseID:    "lease-new",
		LeaseToken: "token-new",
		Task:       firstClaim.Assignment.Task,
		NodeType:   firstClaim.Assignment.Routing.NodeType,
	}
	dir.mu.Lock()
	dir.runners[session.RunnerID].finalizedLease[first.AssignmentID] = replacement
	dir.mu.Unlock()

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     staleLease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resultResp)
	if resultResp.Accepted {
		t.Fatal("stale lease result should not be accepted")
	}

	if _, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 1)); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want replacement lease to keep capacity reserved")
	}

	dir.mu.RLock()
	current, ok := dir.runners[session.RunnerID].finalizedLease[first.AssignmentID]
	dir.mu.RUnlock()
	if !ok {
		t.Fatal("replacement lease missing after stale cleanup")
	}
	if current.LeaseToken != replacement.LeaseToken {
		t.Fatalf("replacement lease token = %q, want %q", current.LeaseToken, replacement.LeaseToken)
	}

	if enqueued, err := dir.EnqueueAssignment(ctx, first); err != nil {
		t.Fatalf("EnqueueAssignment(first) error = %v", err)
	} else if enqueued {
		t.Fatal("EnqueueAssignment(first) enqueued=true, want seen marker retained while replacement lease is active")
	}
}

type fakeControlEngine struct {
	inspectDetail      engine.ExecutionDetail
	inspectErr         error
	buildLease         *engine.TaskLease
	buildErr           error
	recoverLease       *engine.TaskLease
	recoverErr         error
	commitHook         func()
	commitOutcome      engine.CommitOutcome
	commitErr          error
	committedLease     *engine.TaskLease
	committedResult    engine.TaskResult
	committedNamespace namespace.Namespace
	signalName         string
	signalData         map[string]any
	canceledID         types.ExecutionID
}

func (f *fakeControlEngine) Submit(context.Context, *graph.Graph, map[string]any, ...*types.Runtime) (types.ExecutionID, error) {
	return types.ExecutionID("exec-1"), nil
}

func (f *fakeControlEngine) Inspect(context.Context, types.ExecutionID, ...string) (engine.ExecutionDetail, error) {
	if f.inspectErr != nil {
		return engine.ExecutionDetail{}, f.inspectErr
	}
	return f.inspectDetail, nil
}

func (f *fakeControlEngine) DeliverSignal(_ context.Context, _ types.ExecutionID, name string, data map[string]any) error {
	f.signalName = name
	f.signalData = data
	return nil
}

func (f *fakeControlEngine) Cancel(_ context.Context, id types.ExecutionID) error {
	f.canceledID = id
	return nil
}

func (f *fakeControlEngine) Invoke(_ context.Context, _ *graph.Graph, _ string, _ map[string]any, _ ...*types.Runtime) (types.ExecutionID, error) {
	return types.ExecutionID("exec-invoke"), nil
}

func (f *fakeControlEngine) RevokeSignal(_ context.Context, _ types.ExecutionID, _ string) error {
	return nil
}

func (f *fakeControlEngine) BuildTaskLease(_ context.Context, task *engine.Task) (*engine.TaskLease, error) {
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	if f.buildLease != nil {
		lease := *f.buildLease
		return &lease, nil
	}
	return &engine.TaskLease{Task: *task, NodeType: "xflow.function"}, nil
}

func (f *fakeControlEngine) RecoverTaskLease(_ context.Context, task *engine.Task) (*engine.TaskLease, error) {
	if f.recoverErr != nil {
		return nil, f.recoverErr
	}
	if f.recoverLease != nil {
		lease := *f.recoverLease
		return &lease, nil
	}
	return nil, engine.ErrLeaseNotRecoverable
}

func (f *fakeControlEngine) CommitTaskResultWithOutcome(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) (engine.CommitOutcome, error) {
	f.committedNamespace = namespace.FromContext(ctx)
	if f.commitHook != nil {
		f.commitHook()
	}
	if f.commitErr != nil {
		if errors.Is(f.commitErr, engine.ErrInvalidLeaseToken) {
			return engine.CommitOutcomeStaleToken, f.commitErr
		}
		return engine.CommitOutcomeTransientError, f.commitErr
	}
	f.committedLease = lease
	f.committedResult = result
	if f.commitOutcome != "" {
		return f.commitOutcome, nil
	}
	return engine.CommitOutcomeAccepted, nil
}

func (f *fakeControlEngine) CommitTaskResult(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) error {
	_, err := f.CommitTaskResultWithOutcome(ctx, lease, result)
	return err
}

func mustRegisterHTTPRunner(t *testing.T, dir *MemoryRunnerDirectory) RunnerSession {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

func (f *fakeControlEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

func (f *fakeControlEngine) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	return false, nil
}

func (f *fakeControlEngine) SeedExecutionFromEntry(context.Context, engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	return engine.SeedExecutionFromEntryResponse{}, nil
}

func stableTestAssignment(nodeName string) Assignment {
	task := engine.Task{
		ExecutionID: "exec-1",
		NodeName:    nodeName,
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	return Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
}

func hiddenIdentityAssignment(nodeName string, activationID, autoDepth int) Assignment {
	task := engine.Task{
		ExecutionID:  "exec-1",
		NodeName:     nodeName,
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: activationID,
		AutoDepth:    autoDepth,
	}
	return Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
}

func postJSON(t *testing.T, url string, body any, wantStatus int, out any) {
	t.Helper()
	resp := postJSONRaw(t, url, body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

func postJSONRaw(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestCorePollRecoversLeaseAfterFinalizeCrashWindow(t *testing.T) {
	ctx := context.Background()
	underlying := NewMemoryRunnerDirectory()
	session := mustRegisterHTTPRunner(t, underlying)
	assignment := stableTestAssignment("recover-handoff")
	mustEnqueueAssignment(t, ctx, underlying, assignment)
	lease := &engine.TaskLease{
		LeaseID:    "lease-handoff",
		LeaseToken: "token-handoff",
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{"request": "original"}},
		NodeType:   assignment.Routing.NodeType,
	}
	fake := &fakeControlEngine{buildLease: lease}
	directory := &failingFinalizeRunnerDirectory{RunnerDirectory: underlying, failures: 1}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	req := protocol.PollTaskRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}

	if _, err := core.pollTask(ctx, req, TransportInfo{}); err == nil {
		t.Fatal("first poll error = nil, want simulated finalize response loss")
	}

	// The first BuildTaskLease already made the engine lease durable. A retry
	// must attach that exact lease to a fresh directory claim rather than leave
	// the assignment seen-but-unreachable until the lease TTL expires.
	fake.buildErr = engine.ErrLeaseAlreadyActive
	fake.recoverLease = lease
	response, err := core.pollTask(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("recovery pollTask() error = %v", err)
	}
	if response.Lease == nil || response.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("recovered response lease = %+v, want token %q", response.Lease, lease.LeaseToken)
	}
	if directory.failures != 0 {
		t.Fatalf("FinalizeClaim failures remaining = %d, want 0", directory.failures)
	}
}

type failingFinalizeRunnerDirectory struct {
	RunnerDirectory
	failures int
}

func (d *failingFinalizeRunnerDirectory) FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	if d.failures > 0 {
		d.failures--
		return errors.New("simulated finalize response loss")
	}
	return d.RunnerDirectory.FinalizeClaim(ctx, claimID, lease)
}
