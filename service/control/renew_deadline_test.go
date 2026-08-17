package control

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestRenewRefusedPastDeadline: a lease whose ExecutionDeadline has passed gets
// Renewed:false.
func TestRenewRefusedPastDeadline(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-deadline",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Finalize a lease in the directory with a deadline in the past.
	task := engine.Task{
		ExecutionID: "exec-deadline-1",
		NodeName:    "step1",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	pastDeadline := time.Now().Add(-5 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-dl-1",
		LeaseToken:        "token-dl-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.function",
		IssuedAt:          time.Now().Add(-10 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: pastDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	// The fake engine needs to implement CommitTaskTimeout.
	fake := &deadlineTestEngine{}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-dl-1",
		LeaseToken: "token-dl-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if resp.Renewed {
		t.Fatal("renewLease() Renewed=true, want false for expired deadline")
	}
	if resp.Error == "" {
		t.Fatal("renewLease() Error is empty, want non-empty message")
	}
}

// TestRenewCommitsTimeoutTerminal is the load-bearing half: refusing renewal
// alone leaves the sweeper to re-enqueue the task one TTL later, which is
// exactly the retry the design forbids. Assert the node reached a terminal
// state, not just that renewal was refused.
func TestRenewCommitsTimeoutTerminal(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-terminal",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-terminal-1",
		NodeName:    "step1",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	pastDeadline := time.Now().Add(-5 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-term-1",
		LeaseToken:        "token-term-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.function",
		IssuedAt:          time.Now().Add(-10 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: pastDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	fake := &deadlineTestEngine{}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-term-1",
		LeaseToken: "token-term-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if resp.Renewed {
		t.Fatal("expected Renewed=false")
	}
	// The key assertion: CommitTaskTimeout was called.
	if !fake.commitTimeoutCalled {
		t.Fatal("CommitTaskTimeout was NOT called; refusing alone leaves the sweeper to " +
			"re-enqueue the task one TTL later, which is the retry a timeout must never get")
	}
}

// TestRenewUnaffectedBeforeDeadline: a lease with time left renews normally.
func TestRenewUnaffectedBeforeDeadline(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-ok",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-ok-1",
		NodeName:    "step1",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	futureDeadline := time.Now().Add(30 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-ok-1",
		LeaseToken:        "token-ok-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.function",
		IssuedAt:          time.Now().Add(-1 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: futureDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	fake := &deadlineTestEngine{nodeRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-ok-1",
		LeaseToken: "token-ok-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatal("renewLease() Renewed=false, want true for future deadline")
	}
	if fake.commitTimeoutCalled {
		t.Fatal("CommitTaskTimeout was called for a lease that has not expired")
	}
}

// TestRenewUnaffectedWithZeroDeadline: a lease with no deadline renews forever
// (unchanged behaviour).
func TestRenewUnaffectedWithZeroDeadline(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-zero",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-zero-1",
		NodeName:    "step1",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-zero-1",
		LeaseToken: "token-zero-1",
		Task:       task,
		Attempt:    1,
		NodeType:   "xflow.function",
		IssuedAt:   time.Now().Add(-1 * time.Minute),
		TTL:        60 * time.Second,
		// ExecutionDeadline is zero -- no bound.
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	fake := &deadlineTestEngine{nodeRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-zero-1",
		LeaseToken: "token-zero-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatal("renewLease() Renewed=false, want true for zero deadline (unchanged)")
	}
	if fake.commitTimeoutCalled {
		t.Fatal("CommitTaskTimeout should not be called for zero-deadline leases")
	}
}

// TestRenewSkipsDeadlineBranchForGroupLease: the deadline branch is node-only.
// A group lease with a past ExecutionDeadline (a future scenario: today no
// group lease carries ExecutionDeadline) must NOT be handed to
// CommitTaskTimeout. The isGroupTask guard skips the branch and lets the
// group renewal path handle it. This test pins the outer guard: deleting
// !isGroupTask from the branch predicate makes this red because
// CommitTaskTimeout would be called.
func TestRenewSkipsDeadlineBranchForGroupLease(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-group-deadline",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-group-deadline-1",
		NodeName:    "grp1",
		NodeIdx:     0,
		UnitIdx:     1,
		Type:        engine.TaskTypeGroupExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.group"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Now:          time.Now(),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want claim")
	}
	// Hypothetical future: a group lease stamped with ExecutionDeadline. Today
	// BuildGroupLease never sets it; this test forces it to verify the guard.
	pastDeadline := time.Now().Add(-5 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-grp-dl-1",
		LeaseToken:        "token-grp-dl-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.group",
		IssuedAt:          time.Now().Add(-10 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: pastDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	fake := &deadlineTestEngine{groupRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-grp-dl-1",
		LeaseToken: "token-grp-dl-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	// The group lease should have been renewed via the group path (the branch
	// was skipped), NOT refused or committed via the node timeout path.
	if !resp.Renewed {
		t.Fatal("renewLease() Renewed=false for a group lease; the deadline branch " +
			"must be skipped for group tasks so the group renewal path handles it")
	}
	if fake.commitTimeoutCalled {
		t.Fatal("CommitTaskTimeout was called for a group lease; the isGroupTask guard " +
			"must skip the deadline branch for group tasks")
	}
}

// deadlineTestEngine is a fake that satisfies EngineFacade (via
// embedding fakeControlEngine) and the nodeLeaseEngine + nodeTimeoutCommitter
// + groupLeaseEngine interfaces needed by the deadline backstop tests.
type deadlineTestEngine struct {
	fakeControlEngine
	nodeRenewResult     bool
	nodeRenewErr        error
	groupRenewResult    bool
	commitTimeoutCalled bool
	commitTimeoutLease  *engine.TaskLease
	commitTimeoutErr    error
}

func (d *deadlineTestEngine) RenewTaskLease(_ context.Context, lease *engine.TaskLease, _ time.Duration) (bool, error) {
	if d.nodeRenewErr != nil {
		return false, d.nodeRenewErr
	}
	return d.nodeRenewResult, nil
}

func (d *deadlineTestEngine) RenewGroupLease(_ context.Context, lease *engine.TaskLease, _ time.Duration) (bool, error) {
	return d.groupRenewResult, nil
}

func (d *deadlineTestEngine) BuildGroupLease(_ context.Context, t *engine.Task) (*engine.TaskLease, *engine.GroupLeasePayload, error) {
	lease := &engine.TaskLease{Task: *t, NodeType: "xflow.group"}
	return lease, &engine.GroupLeasePayload{}, nil
}

func (d *deadlineTestEngine) RecoverGroupLease(_ context.Context, _ types.ExecutionID, _ int) (*engine.TaskLease, *engine.GroupLeasePayload, error) {
	return nil, nil, errors.New("not implemented")
}

func (d *deadlineTestEngine) CommitGroupResult(_ context.Context, lease *engine.TaskLease, res engine.GroupResult) (engine.CommitOutcome, error) {
	return engine.CommitOutcomeAccepted, nil
}

func (d *deadlineTestEngine) CommitTaskTimeout(_ context.Context, lease *engine.TaskLease, _ error) error {
	d.commitTimeoutCalled = true
	d.commitTimeoutLease = lease
	return d.commitTimeoutErr
}

// TestRenewCommitsTimeoutEmitsServerMetric proves the server-side backstop
// production path (renewLease) EMITS xflow_node_timeouts_total with
// source="server". This is the only origin of a source=server timeout; the
// source label is the direct measure of whether runners honour the deadline.
// It must go red if the observeNodeTimeout call site in group_control_loop.go
// is deleted.
func TestRenewCommitsTimeoutEmitsServerMetric(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-server-metric",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-server-metric-1",
		NodeName:    "sensitive-node-name", // must never appear as a metric label
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	pastDeadline := time.Now().Add(-5 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-srv-metric-1",
		LeaseToken:        "token-srv-metric-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.function",
		IssuedAt:          time.Now().Add(-10 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: pastDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	m := metrics.New()
	fake := &deadlineTestEngine{}
	core := &Core{
		engine:           fake,
		runners:          dir,
		pollWait:         time.Second,
		timeoutObserver:  metrics.NewNodeTimeoutMetrics(m),
	}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-srv-metric-1",
		LeaseToken: "token-srv-metric-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v, want nil", err)
	}
	if resp.Renewed {
		t.Fatal("renewLease() Renewed=true, want false for expired deadline")
	}
	if !fake.commitTimeoutCalled {
		t.Fatal("CommitTaskTimeout was NOT called; the backstop must commit the terminal")
	}

	// Scrape the registry exactly as the server's /metrics would expose it.
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	want := `xflow_node_timeouts_total{node_type="xflow.function",source="server"} 1`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics body missing %q:\n%s", want, body)
	}
	// source=runner must NOT have been incremented by the server path.
	if strings.Contains(body, `source="runner"`) {
		t.Fatalf("server path emitted a runner-source timeout; the source label is wrong:\n%s", body)
	}
	// No sensitive label leakage.
	if strings.Contains(body, "sensitive-node-name") || strings.Contains(body, "exec-server-metric-1") {
		t.Fatalf("metrics body leaked a node name or execution id:\n%s", body)
	}
}

// TestRenewUnaffectedBeforeDeadlineEmitsNoTimeoutMetric is the negative probe:
// a lease still within its deadline renews normally and must NOT emit a
// timeout metric. This guards against a wiring that fires on every renewal.
func TestRenewUnaffectedBeforeDeadlineEmitsNoTimeoutMetric(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-no-metric",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	task := engine.Task{
		ExecutionID: "exec-no-metric-1",
		NodeName:    "step1",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	futureDeadline := time.Now().Add(30 * time.Minute)
	lease := &engine.TaskLease{
		LeaseID:           "lease-no-metric-1",
		LeaseToken:        "token-no-metric-1",
		Task:              task,
		Attempt:           1,
		NodeType:          "xflow.function",
		IssuedAt:          time.Now().Add(-1 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: futureDeadline,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}

	m := metrics.New()
	fake := &deadlineTestEngine{nodeRenewResult: true}
	core := &Core{
		engine:          fake,
		runners:         dir,
		pollWait:        time.Second,
		timeoutObserver: metrics.NewNodeTimeoutMetrics(m),
	}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "lease-no-metric-1",
		LeaseToken: "token-no-metric-1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatal("renewLease() Renewed=false, want true for future deadline")
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "xflow_node_timeouts_total") {
		t.Fatalf("a future-deadline renewal emitted a timeout metric:\n%s", rec.Body.String())
	}
}

