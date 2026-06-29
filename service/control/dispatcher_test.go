package control

import (
	"context"
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
	if leaseEngine.committed {
		t.Fatal("control dispatcher committed a result despite no runner")
	}
}

type fakeDispatchEngine struct {
	lease     *engine.TaskLease
	committed bool
}

func (e *fakeDispatchEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return e.lease, nil
}

func (e *fakeDispatchEngine) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	e.committed = true
	return nil
}
