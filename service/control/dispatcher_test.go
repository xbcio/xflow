package control

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestDispatcherBuildsLeaseAndAssignsToRunnerPool(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
	// Second dispatch fills the only runner's headroom.
	err := d.HandleTask(context.Background(), task)
	if err == nil {
		t.Fatal("expected ErrNoCapacity after pool saturates")
	}
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity", err)
	}
}

func TestDispatcherObserverRecordsTransientPlacementFailures(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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

type fakeDispatchEngine struct {
	lease      *engine.TaskLease
	routing    engine.TaskRouting
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

func TestDispatcherTreatsLeaseAlreadyActiveAsNoOp(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 0}
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
