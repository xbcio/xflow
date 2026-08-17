package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
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

// deadlineTestEngine is a fake that satisfies both EngineFacade (via
// embedding fakeControlEngine) and the nodeLeaseEngine + commitTaskTimeout
// interfaces needed by the deadline backstop.
type deadlineTestEngine struct {
	fakeControlEngine
	nodeRenewResult      bool
	nodeRenewErr         error
	commitTimeoutCalled  bool
	commitTimeoutLease   *engine.TaskLease
	commitTimeoutErr     error
}

func (d *deadlineTestEngine) RenewTaskLease(_ context.Context, lease *engine.TaskLease, _ time.Duration) (bool, error) {
	if d.nodeRenewErr != nil {
		return false, d.nodeRenewErr
	}
	return d.nodeRenewResult, nil
}

func (d *deadlineTestEngine) CommitTaskTimeout(_ context.Context, lease *engine.TaskLease, _ error) error {
	d.commitTimeoutCalled = true
	d.commitTimeoutLease = lease
	return d.commitTimeoutErr
}

