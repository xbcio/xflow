package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type fakeLeaseEngine struct {
	lease         *engine.TaskLease
	buildErr      error
	committed     bool
	result        engine.TaskResult
	released      bool
	releasedLease *engine.TaskLease
	releaseErr    error
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

func (e *fakeLeaseEngine) ReleaseTaskLease(_ context.Context, lease *engine.TaskLease) (bool, error) {
	e.released = true
	e.releasedLease = lease
	if e.releaseErr != nil {
		return false, e.releaseErr
	}
	return true, nil
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

func TestDispatcherLeavesUnknownExecutorOutcomeLeased(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	want := errors.New("runner response lost")
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
	}
	dispatcher := NewDispatcher(eng, failingExecutor{err: want})

	err := dispatcher.HandleTask(context.Background(), task)
	if !errors.Is(err, want) {
		t.Fatalf("HandleTask() error = %v, want %v", err, want)
	}
	if eng.committed {
		t.Fatal("dispatcher committed an unknown executor outcome")
	}
	if eng.released {
		t.Fatal("dispatcher released a lease whose execution outcome was unknown")
	}
}

func TestDispatcherCommitsConfirmedNodeFailure(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	want := errors.New("handler rejected input")
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
	}

	if err := NewDispatcher(eng, failingExecutor{err: NodeFailure(want)}).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if !eng.committed {
		t.Fatal("dispatcher did not commit confirmed node failure")
	}
	if !errors.Is(eng.result.Error, want) {
		t.Fatalf("committed result error = %v, want %v", eng.result.Error, want)
	}
	if eng.released {
		t.Fatal("dispatcher released a lease after confirmed node execution")
	}
}

func TestDispatcherReleasesConfirmedDispatchFailure(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	want := errors.New("runner unavailable before delivery")
	lease := &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"}
	eng := &fakeLeaseEngine{lease: lease}

	if err := NewDispatcher(eng, failingExecutor{err: DispatchFailure(want)}).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if eng.committed {
		t.Fatal("dispatcher committed a failure that happened before execution")
	}
	if !eng.released || eng.releasedLease != lease {
		t.Fatalf("ReleaseTaskLease() called=%v lease=%p, want lease %p", eng.released, eng.releasedLease, lease)
	}
}

func TestDispatcherCommitsPermanentConfigurationFailureWithoutRetry(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	want := errors.New("missing runner credential")
	eng := &fakeLeaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
	}

	if err := NewDispatcher(eng, failingExecutor{err: PermanentConfigurationFailure(want)}).HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if !eng.committed {
		t.Fatal("dispatcher did not commit permanent configuration failure")
	}
	if !types.IsPermanent(eng.result.Error) || !errors.Is(eng.result.Error, want) {
		t.Fatalf("committed permanent error = %v, want permanent wrapper around %v", eng.result.Error, want)
	}
	if eng.released {
		t.Fatal("dispatcher released a permanent configuration failure")
	}
}

func TestDispatcherReturnsReleaseFailureForDispatchFailure(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	dispatchErr := errors.New("runner unavailable before delivery")
	releaseErr := errors.New("requeue failed")
	eng := &fakeLeaseEngine{
		lease:      &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
		releaseErr: releaseErr,
	}

	err := NewDispatcher(eng, failingExecutor{err: DispatchFailure(dispatchErr)}).HandleTask(context.Background(), task)
	if !errors.Is(err, dispatchErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("HandleTask() error = %v, want both dispatch and release errors", err)
	}
	if !eng.released {
		t.Fatal("dispatcher did not attempt immediate lease release")
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

type noImmediateReleaseEngine struct {
	lease     *engine.TaskLease
	committed bool
}

func (e *noImmediateReleaseEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return e.lease, nil
}

func (e *noImmediateReleaseEngine) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	e.committed = true
	return nil
}

func (e *noImmediateReleaseEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

func (e *noImmediateReleaseEngine) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	return false, nil
}

func TestDispatcherLeavesDispatchFailureFencedWhenReleaseIsUnsupported(t *testing.T) {
	task := &engine.Task{ExecutionID: types.ExecutionID("exec-runtime"), NodeName: "action"}
	dispatchErr := errors.New("runner unavailable before delivery")
	eng := &noImmediateReleaseEngine{
		lease: &engine.TaskLease{Task: *task, Input: &types.Input{NodeName: "action"}, NodeType: "test.action"},
	}

	err := NewDispatcher(eng, failingExecutor{err: DispatchFailure(dispatchErr)}).HandleTask(context.Background(), task)
	if !errors.Is(err, dispatchErr) || !errors.Is(err, ErrLeaseReleaseUnsupported) {
		t.Fatalf("HandleTask() error = %v, want dispatch and unsupported-release errors", err)
	}
	if eng.committed {
		t.Fatal("dispatcher committed a dispatch-before-execution failure without a release capability")
	}
}
