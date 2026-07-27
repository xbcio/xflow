package control

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

var _ func(Router, RunnerDirectory, ...DispatcherOption) *Dispatcher = NewDispatcher

func TestDispatcherEnqueuesRoutedAssignmentWithoutBuildingLease(t *testing.T) {
	ctx := context.Background()
	dispatchEngine := &fakeDispatchEngine{
		routing: engine.TaskRouting{NodeType: "xflow.function"},
	}
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(dispatchEngine, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
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

type fakeDispatchEngine struct {
	routing    engine.TaskRouting
	routingErr error
}

func (e *fakeDispatchEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return e.routing, e.routingErr
}

func (e *fakeDispatchEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	panic("dispatcher must not call BuildTaskLease")
}

func TestDispatcherAssignmentCarriesNamespaceFromContext(t *testing.T) {
	ctx := context.Background()
	ctx = namespace.WithNamespace(ctx, "namespace-acme")
	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(&fakeDispatchEngine{routing: engine.TaskRouting{NodeType: "xflow.function"}}, dir)
	task := &engine.Task{ExecutionID: "exec-1", NodeName: "node-a", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}

	if err := dispatcher.HandleTask(ctx, task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
		Namespaces:   []namespace.Namespace{"namespace-acme"},
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
	if claim.Assignment.Namespace != "namespace-acme" {
		t.Fatalf("assignment namespace = %q, want namespace-acme", claim.Assignment.Namespace)
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
