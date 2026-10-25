package local

import (
	"context"
	"errors"
	"sync"
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

// expireProbeState records which executions the engine reads. The engine
// re-checks the deadline itself and refuses a not-due execution without
// changing it, so a sweeper that listed one too early is invisible in the
// execution's status; the only trace it leaves is that the engine was asked.
// Embedding the concrete store keeps every optional capability the engine
// looks for (the deadline acknowledgement included).
type expireProbeState struct {
	*memoryState
	mu    sync.Mutex
	reads map[types.ExecutionID]int
}

func (s *expireProbeState) noteRead(id types.ExecutionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reads == nil {
		s.reads = make(map[types.ExecutionID]int)
	}
	s.reads[id]++
}

func (s *expireProbeState) readCount(id types.ExecutionID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[id]
}

func (s *expireProbeState) GetExecution(ctx context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	s.noteRead(id)
	return s.memoryState.GetExecution(ctx, id)
}

func (s *expireProbeState) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.noteRead(id)
	return s.memoryState.GetExecutionStatus(ctx, id)
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
	probe := &expireProbeState{memoryState: b.state}
	eng := engine.New(probe, b.Queue())
	g := nodeStateGraph(t)

	overdue := types.ExecutionID("exec-overdue")
	notDue := types.ExecutionID("exec-not-due")
	for id, deadline := range map[types.ExecutionID]time.Time{
		overdue: time.Now().UTC().Add(-time.Second),
		// Inside the hour a sweep that lists ahead of the clock would reach,
		// and far enough from now that no real tick finds it due.
		notDue: time.Now().UTC().Add(30 * time.Minute),
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
	// time left must have been skipped by every one of them.
	time.Sleep(30 * time.Millisecond)
	snap, err = b.state.GetExecution(ctx, notDue)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(notDue) = %+v, %v", snap, err)
	}
	if snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("not-yet-due execution status = %q, want it untouched (running)", snap.Status)
	}
	// The engine would refuse it as not due anyway, so a status check cannot
	// tell a sweeper that listed it from one that did not. Whether the engine
	// was ever asked about it can.
	if reads := probe.readCount(notDue); reads != 0 {
		t.Fatalf("the sweeper handed the not-yet-due execution to the engine (%d reads), want it never listed", reads)
	}
	if probe.readCount(overdue) == 0 {
		t.Fatal("the probe saw no engine read of the overdue execution, so it is not observing the sweeper")
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

// blockingCompleteHooks parks the OnExecutionComplete hook on a channel the test
// controls. ExpireExecution announces the timeout through that hook before it
// cascades, so a sweeper stuck in it is a sweeper in the middle of an expiry.
type blockingCompleteHooks struct {
	engine.BaseHooks
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *blockingCompleteHooks) OnExecutionComplete(context.Context, types.ExecutionID, types.ExecutionStatus) {
	h.once.Do(func() { close(h.entered) })
	<-h.release
}

// TestBackendStopWaitsForTheDeadlineSweeper pins that the stop function does
// not return while the deadline sweeper is still expiring an execution. The
// sweeper writes to the store and cascades through the engine; a stop that
// returned early would let the caller tear the backend down underneath it.
func TestBackendStopWaitsForTheDeadlineSweeper(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.deadline_park", deadlineParkHandler{})
	b := New(WithConcurrency(1), WithRegistry(reg))
	hooks := &blockingCompleteHooks{entered: make(chan struct{}), release: make(chan struct{})}
	eng := engine.New(b.State(), b.Queue(), engine.WithHooks(hooks))
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry)
	bound := b.bindHandlerWithOutbox(eng, dispatcher.HandleTask, 5*time.Millisecond)

	// The hook is released and the backend stopped on every exit, and only once:
	// stopping twice closes the queue's channel twice.
	var releaseOnce, stopOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hooks.release) }) }
	stop := func() { stopOnce.Do(bound) }
	t.Cleanup(func() {
		release()
		stop()
	})

	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "deadline-stop",
		Version:  "v1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: 50 * time.Millisecond},
		Nodes:    []types.NodeDef{{Name: "park", Type: "test.deadline_park"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-hooks.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the deadline sweeper never reached the OnExecutionComplete hook")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()
	select {
	case <-stopped:
		t.Fatal("the stop function returned while the deadline sweeper was still inside ExpireExecution")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the stop function did not return after the sweeper was released")
	}
}
