package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type fakeLeaseEngine struct {
	lease     *engine.TaskLease
	buildErr  error
	committed bool
	result    engine.TaskResult
}

func (e *fakeLeaseEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return e.lease, e.buildErr
}

func (e *fakeLeaseEngine) CommitTaskResult(_ context.Context, lease *engine.TaskLease, result engine.TaskResult) error {
	e.committed = true
	e.result = result
	return nil
}

func (e *fakeLeaseEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

func (e *fakeLeaseEngine) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	return false, nil
}

type fakeActionHandler struct{}

func (fakeActionHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.action"}
}

func (fakeActionHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

type runtimeSuspendHandler struct{}

func (runtimeSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.wait"}
}

func (runtimeSuspendHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	panic("Execute should not be called for suspending handlers")
}

func (runtimeSuspendHandler) PrepareSuspend(context.Context, *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, nil
}

func (runtimeSuspendHandler) OnResume(context.Context, *types.Input, *types.SignalPayload) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"approved": true}}, nil
}

type singleHandlerRegistry struct {
	handler types.ActionHandler
}

func (r singleHandlerRegistry) Get(types.ExecutionID, string, string, int) (types.ActionHandler, error) {
	return r.handler, nil
}

func TestDispatcherExecutesActionHandlerAndCommitsResult(t *testing.T) {
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-runtime"),
		NodeName:    "action",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{
			Task:     *task,
			Input:    &types.Input{NodeName: "action"},
			NodeType: "test.action",
		},
	}
	dispatcher := NewEmbeddedDispatcher(eng, singleHandlerRegistry{handler: fakeActionHandler{}})

	if err := dispatcher.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if !eng.committed {
		t.Fatal("dispatcher did not commit the action result")
	}
	if eng.result.Output == nil || eng.result.Output.Data["ok"] != true {
		t.Fatalf("committed result = %+v, want ok output", eng.result)
	}
}

type failingExecutor struct {
	err error
}

func (e failingExecutor) Execute(context.Context, *engine.TaskLease) (engine.TaskResult, error) {
	return engine.TaskResult{}, e.err
}

func TestDispatcherReturnsExecutorErrorWithoutEngineFallback(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	want := errors.New("runner failed")
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
	}
	dispatcher := NewDispatcher(eng, failingExecutor{err: want})

	err := dispatcher.HandleTask(context.Background(), task)
	if !errors.Is(err, want) {
		t.Fatalf("HandleTask() error = %v, want %v", err, want)
	}
	if eng.committed {
		t.Fatal("dispatcher committed a failed executor result")
	}
}

type recordingExecutor struct {
	called bool
}

func (e *recordingExecutor) Execute(context.Context, *engine.TaskLease) (engine.TaskResult, error) {
	e.called = true
	return engine.TaskResult{}, nil
}

func TestDispatcherTreatsLeaseAlreadyActiveAsNoOp(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	eng := &fakeLeaseEngine{buildErr: engine.ErrLeaseAlreadyActive}
	executor := &recordingExecutor{}
	dispatcher := NewDispatcher(eng, executor)

	if err := dispatcher.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v, want nil", err)
	}
	if executor.called {
		t.Fatal("dispatcher executed a duplicate active lease")
	}
	if eng.committed {
		t.Fatal("dispatcher committed a duplicate active lease")
	}
}

func TestEmbeddedRunnerReturnsSuspendRequestForSuspendingHandler(t *testing.T) {
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-runtime"),
		NodeName:    "wait",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "wait"}, NodeType: "test.wait"},
	}
	dispatcher := NewEmbeddedDispatcher(eng, singleHandlerRegistry{handler: &runtimeSuspendHandler{}})

	if err := dispatcher.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if !eng.committed || eng.result.Suspend == nil {
		t.Fatalf("dispatcher committed %+v, want suspend request", eng.result)
	}
}
