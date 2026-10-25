package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// deadlineParkHandler parks its node on a signal that never arrives, so the
// only thing that can end the execution is its deadline. Nothing holds a lease
// while it waits, which is exactly the case the sweep exists for.
type deadlineParkHandler struct{}

func (deadlineParkHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.deadline_park", Kind: types.NodeKindAction}
}

func (deadlineParkHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, errors.New("Execute must not run for a suspending node")
}

func (deadlineParkHandler) PrepareSuspend(context.Context, *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"never"}}, nil
}

func (deadlineParkHandler) OnResume(context.Context, *types.Input, *types.SignalPayload) (*types.Output, error) {
	return &types.Output{}, nil
}

// waitForDeadlineIndexEmpty polls until the store holds no deadline entry. The
// entry is acknowledged only after the cascade finished, so an empty index is
// what "ExpireExecution completed" looks like from the outside.
func waitForDeadlineIndexEmpty(t *testing.T, state *memoryState, within time.Duration) {
	t.Helper()
	limit := time.Now().Add(within)
	for {
		state.mu.Lock()
		tracked := len(state.deadlines)
		state.mu.Unlock()
		if tracked == 0 {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("deadline index still holds %d entries after %v", tracked, within)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestBackendSweepsExecutionDeadlines drives the sweeper directly, the way
// TestBackendSweepsQueueTimeouts drives its own: an overdue execution is
// expired and acknowledged, one that is not yet due is left alone, and the loop
// returns once its context is canceled.
func TestBackendSweepsExecutionDeadlines(t *testing.T) {
	ctx := context.Background()
	b := New()
	eng := engine.New(b.State(), b.Queue())
	g := nodeStateGraph(t)

	overdue := types.ExecutionID("exec-overdue")
	notDue := types.ExecutionID("exec-not-due")
	for id, deadline := range map[types.ExecutionID]time.Time{
		overdue: time.Now().UTC().Add(-time.Second),
		notDue:  time.Now().UTC().Add(time.Hour),
	} {
		if err := b.state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
		}); err != nil {
			t.Fatalf("CreateExecution(%s) error = %v", id, err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runDeadlineSweeper(runCtx, eng, 5*time.Millisecond)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("runDeadlineSweeper did not return after its context was canceled")
		}
	}
	t.Cleanup(stop)

	// The overdue entry leaves the index only after ExpireExecution acknowledged
	// it, so by then the status and reason are final.
	limit := time.Now().Add(5 * time.Second)
	for {
		b.state.mu.Lock()
		_, tracked := b.state.deadlines[overdue]
		b.state.mu.Unlock()
		if !tracked {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("overdue execution was never acknowledged by the sweeper")
		}
		time.Sleep(2 * time.Millisecond)
	}
	snap, err := b.state.GetExecution(ctx, overdue)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(overdue) = %+v, %v", snap, err)
	}
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("overdue execution = status %q error %q, want timeout / %q",
			snap.Status, snap.Error, engine.ExecutionDeadlineExceededError)
	}

	// Several ticks have passed since the overdue one was handled; the one with
	// an hour left must have been skipped by every one of them.
	time.Sleep(30 * time.Millisecond)
	snap, err = b.state.GetExecution(ctx, notDue)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(notDue) = %+v, %v", snap, err)
	}
	if snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("not-yet-due execution status = %q, want it untouched (running)", snap.Status)
	}
	requireDeadlineIDs(t, listedDeadlines(t, b.state, time.Now().UTC().Add(2*time.Hour), 8), notDue)

	stop()
}

// TestBackendExpiresASuspendedExecutionAtItsDeadline is the end-to-end case: a
// workflow with settings.timeout whose only node is parked on a signal. Nothing
// holds a lease, so only the in-process sweep can end it. It goes through
// bindHandlerWithOutbox so the goroutine wiring and the stop function are under
// test as well, and the margins are wide because the deadline is wall-clock.
func TestBackendExpiresASuspendedExecutionAtItsDeadline(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.deadline_park", deadlineParkHandler{})
	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry)
	stop := b.bindHandlerWithOutbox(eng, dispatcher.HandleTask, 5*time.Millisecond)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "deadline-suspended",
		Version:  "v1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Second},
		Nodes:    []types.NodeDef{{Name: "park", Type: "test.deadline_park"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	snap, err := b.state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if snap.Deadline.IsZero() {
		t.Fatal("submitted execution carries no Deadline, so the sweep has nothing to find")
	}

	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	if res.Status != types.ExecutionStatusTimeout {
		t.Fatalf("WaitDone() status = %q, want %q", res.Status, types.ExecutionStatusTimeout)
	}
	if res.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("WaitDone() error = %q, want %q", res.Error, engine.ExecutionDeadlineExceededError)
	}

	// WaitDone returns at the status write, before the cascade. The index entry
	// goes only after the cascade, so waiting for it makes the node assertion
	// below a statement about a finished expiry rather than a race with it.
	waitForDeadlineIndexEmpty(t, b.state, 10*time.Second)
	node, err := b.state.GetNode(ctx, id, "park")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("suspended node after the expiry = %+v, want it canceled by the cascade", node)
	}

	stop()
	stopped = true
}
