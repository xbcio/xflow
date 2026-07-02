package control

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestDispatcherEnqueuesRoutedAssignmentWithoutBuildingLease(t *testing.T) {
	ctx := context.Background()
	leaseEngine := &fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
		lease:   &engine.TaskLease{LeaseID: "lease-should-not-build"},
	}
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(leaseEngine, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if leaseEngine.buildCalls != 0 {
		t.Fatalf("BuildTaskLease calls = %d, want 0", leaseEngine.buildCalls)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if claim.Assignment.Task.NodeName != "node-a" {
		t.Fatalf("claimed task = %+v, want node-a", claim.Assignment.Task)
	}
	if claim.Assignment.AssignmentID != BuildAssignmentID(task) {
		t.Fatalf("assignment id = %q, want %q", claim.Assignment.AssignmentID, BuildAssignmentID(task))
	}
}

func TestDispatcherDeduplicatesAssignmentByDeterministicTaskIdentity(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(&fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("first HandleTask() error = %v", err)
	}
	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("second HandleTask() error = %v", err)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil || !ok {
		t.Fatalf("first ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if _, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("second ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("second ClaimForRunner() ok=true, want deduplicated queue")
	}
}

func TestDispatcherReturnsTransientErrorWithoutRunnerDirectory(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	dispatcher := NewDispatcher(&fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}, nil)

	err := dispatcher.HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error without runner directory")
	}
	if !IsTransient(err) {
		t.Fatalf("err = %v, want transient", err)
	}
}

func TestDispatcherTreatsExecutionInactiveAsNoOp(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{routingErr: engine.ErrExecutionInactive}
	dir := NewMemoryRunnerDirectory()

	if err := NewDispatcher(leaseEngine, dir).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v, want nil", err)
	}

	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, ok, err := dir.ClaimForRunner(context.Background(), ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want no queued assignment")
	}
}

func TestDispatcherPropagatesUnexpectedRoutingError(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	want := errors.New("boom")
	leaseEngine := &fakeDispatchEngine{routingErr: want}

	err := NewDispatcher(leaseEngine, NewMemoryRunnerDirectory()).HandleTask(context.Background(), task)
	if !errors.Is(err, want) {
		t.Fatalf("HandleTask() error = %v, want %v", err, want)
	}
}

func TestDispatcherBuildsLeaseAndAssignsToRunnerPool(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	lease := &engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     *task,
		NodeType: "xflow.function",
	}
	leaseEngine := &fakeDispatchEngine{lease: lease}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	dispatcher := NewDispatcher(leaseEngine, pool)
	if err := dispatcher.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	got, ok := pool.Poll("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})
	if !ok {
		t.Fatal("runner did not receive assigned lease")
	}
	if got.LeaseID != lease.LeaseID {
		t.Fatalf("assigned lease = %q, want %q", got.LeaseID, lease.LeaseID)
	}
	if leaseEngine.committed {
		t.Fatal("control dispatcher committed a result; runner should report results later")
	}
}

func TestDispatcherReturnsErrorWhenNoRunnerCanExecuteLease(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
		lease: &engine.TaskLease{
			LeaseID:  engine.LeaseID("lease-1"),
			Task:     *task,
			NodeType: "xflow.function",
		},
	}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.http"}})

	err := NewDispatcher(leaseEngine, pool).HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error without matching runner")
	}
	if !errors.Is(err, ErrNoMatchingRunner) {
		t.Fatalf("err = %v, want ErrNoMatchingRunner", err)
	}
	var transient interface{ Transient() bool }
	if errors.As(err, &transient) {
		t.Fatalf("err = %T, dispatcher should not return queue-layer transient wrappers", err)
	}
	if leaseEngine.committed {
		t.Fatal("control dispatcher committed a result despite no runner")
	}
}

func TestDispatcherDoesNotBuildLeaseWhenNoRunnerCanAcceptRouting(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
		lease: &engine.TaskLease{
			LeaseID:  engine.LeaseID("lease-1"),
			Task:     *task,
			NodeType: "xflow.function",
		},
	}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.http"}})

	err := NewDispatcher(leaseEngine, pool).HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected error without matching runner")
	}
	if !errors.Is(err, ErrNoMatchingRunner) {
		t.Fatalf("err = %v, want ErrNoMatchingRunner", err)
	}
	if leaseEngine.buildCalls != 0 {
		t.Fatalf("BuildTaskLease calls = %d, want 0 before a runner is available", leaseEngine.buildCalls)
	}
}

func TestDispatcherReturnsCapacityErrorWhenAllRunnersSaturated(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	lease := &engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     *task,
		NodeType: "xflow.function",
	}
	leaseEngine := &fakeDispatchEngine{lease: lease}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	d := NewDispatcher(leaseEngine, pool)
	if err := d.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("first HandleTask() = %v, want nil", err)
	}
	err := d.HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected ErrNoCapacity after pool saturates")
	}
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity", err)
	}
}

func TestDispatcherObserverRecordsTransientPlacementFailures(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{routing: engine.TaskRouting{NodeType: "xflow.function"}}
	observer := &recordingDispatcherObserver{}

	err := NewDispatcher(leaseEngine, nil, WithDispatcherObserver(observer)).HandleTask(context.Background(), task)
	if !errors.Is(err, ErrNoMatchingRunner) {
		t.Fatalf("err = %v, want ErrNoMatchingRunner", err)
	}
	if got := observer.reasons; len(got) != 1 || got[0] != "no_matching_runner" {
		t.Fatalf("observer reasons = %v, want [no_matching_runner]", got)
	}
}

func TestDispatcherReturnsBuildLeaseErrorUnchanged(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	buildErr := errors.New("state unavailable")
	leaseEngine := &fakeDispatchEngine{
		routing:  engine.TaskRouting{NodeType: "xflow.function"},
		buildErr: buildErr,
	}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	err := NewDispatcher(leaseEngine, pool).HandleTask(context.Background(), task)
	if !errors.Is(err, buildErr) {
		t.Fatalf("HandleTask() err = %v, want %v", err, buildErr)
	}
}

func TestDispatcherTreatsLeaseAlreadyActiveAsNoOp(t *testing.T) {
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0}
	leaseEngine := &fakeDispatchEngine{
		routing:  engine.TaskRouting{NodeType: "xflow.function"},
		buildErr: engine.ErrLeaseAlreadyActive,
	}
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	if err := NewDispatcher(leaseEngine, pool).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v, want nil", err)
	}

	if _, ok := pool.Poll("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}}); ok {
		t.Fatal("runner received a lease for a duplicate active dispatch")
	}
	if leaseEngine.committed {
		t.Fatal("control dispatcher committed a duplicate active lease")
	}
}

type fakeDispatchEngine struct {
	routing    engine.TaskRouting
	routingErr error
	lease      *engine.TaskLease
	buildCalls int
	buildErr   error
	committed  bool
}

type recordingDispatcherObserver struct {
	reasons []string
}

func (o *recordingDispatcherObserver) OnDispatchTransient(reason string) {
	o.reasons = append(o.reasons, reason)
}

func (e *fakeDispatchEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	if e.routingErr != nil {
		return e.routing, e.routingErr
	}
	if e.routing.NodeType != "" {
		return e.routing, nil
	}
	if e.lease != nil {
		return engine.TaskRouting{NodeType: e.lease.NodeType, NodeVersion: e.lease.NodeVersion}, nil
	}
	return engine.TaskRouting{}, nil
}

func (e *fakeDispatchEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	e.buildCalls++
	if e.buildErr != nil {
		return nil, e.buildErr
	}
	return e.lease, nil
}

func (e *fakeDispatchEngine) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	e.committed = true
	return nil
}

func (e *fakeDispatchEngine) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	return false, nil
}
