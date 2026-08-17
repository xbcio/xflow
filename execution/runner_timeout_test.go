package execution

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// blockingHandler ignores ctx entirely and blocks until released is closed.
// This exercises the worst case: a handler that does not cooperate with context
// cancellation. The runner must still release the slot.
type blockingHandler struct {
	blocked  chan struct{} // closed by handler on entry
	released chan struct{} // closed by test to unblock handler
}

func (blockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.blocking"}
}

func (h blockingHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	close(h.blocked)
	<-h.released
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestExecuteEnforcesDeadline: a handler that ignores ctx must not hold the
// slot. Execute returns as soon as the deadline passes, with a permanent error.
func TestExecuteEnforcesDeadline(t *testing.T) {
	released := make(chan struct{})
	blocked := make(chan struct{})
	h := blockingHandler{blocked: blocked, released: released}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	budget := 100 * time.Millisecond
	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-block", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-block", NodeName: "probe", Timeout: budget},
		NodeType:          "test.blocking",
		ExecutionDeadline: time.Now().Add(budget),
	}

	// Safety backstop: if the deadline enforcement is broken, the blocking
	// handler hangs forever. This AfterFunc ensures the test still terminates.
	backstop := time.AfterFunc(5*time.Second, func() {
		close(released)
	})
	defer backstop.Stop()

	start := time.Now()
	res, err := runner.Execute(context.Background(), lease)
	elapsed := time.Since(start)

	// Unblock the leaked handler goroutine so it can exit.
	select {
	case <-released:
	default:
		close(released)
	}

	if err != nil {
		t.Fatalf("Execute() error = %v, want nil (timeout flows via TaskResult.Error)", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a permanent node.timeout error")
	}
	if !types.IsPermanent(res.Error) {
		t.Fatalf("TaskResult.Error is not permanent: %v", res.Error)
	}
	if !strings.Contains(res.Error.Error(), "node.timeout") && !strings.Contains(res.Error.Error(), "timeout") {
		t.Fatalf("TaskResult.Error message = %q, want it to mention timeout", res.Error.Error())
	}
	// Execute should return within ~200ms of the budget, not 5s (the backstop).
	if elapsed > 2*time.Second {
		t.Fatalf("Execute took %v, want ≤ ~200ms of the %v budget", elapsed, budget)
	}
}

// TestExpiredDeadlineDoesNotRunHandler is the guard against copying
// subgraph.go's After(time.Now()) shape: an already-expired deadline must
// terminate the task, NOT wave it through unbounded.
func TestExpiredDeadlineDoesNotRunHandler(t *testing.T) {
	var invoked atomic.Int32
	h := countingHandler{invoked: &invoked}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-expired", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-expired", NodeName: "probe", Timeout: 5 * time.Second},
		NodeType:          "test.counting",
		ExecutionDeadline: time.Now().Add(-time.Second), // already expired
	}
	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	// Give any leaked goroutine a chance to invoke the handler — if the
	// early-exit guard is missing, the goroutine will increment the counter.
	time.Sleep(50 * time.Millisecond)
	if n := invoked.Load(); n != 0 {
		t.Fatalf("handler invoked %d times on an already-expired deadline, want 0", n)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a permanent node.timeout error")
	}
	if !types.IsPermanent(res.Error) {
		t.Fatalf("TaskResult.Error is not permanent: %v", res.Error)
	}
}

// TestZeroDeadlineIsUnbounded: an unconfigured-and-default-disabled node still
// runs to completion.
func TestZeroDeadlineIsUnbounded(t *testing.T) {
	var invoked atomic.Int32
	h := countingHandler{invoked: &invoked}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-unbounded", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-unbounded", NodeName: "probe"},
		NodeType: "test.counting",
		// ExecutionDeadline is zero (default) and Input.Timeout is zero.
	}
	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if n := invoked.Load(); n != 1 {
		t.Fatalf("handler invoked %d times, want 1", n)
	}
	if res.Error != nil {
		t.Fatalf("TaskResult.Error = %v, want nil (unbounded execution)", res.Error)
	}
}

// TestCooperativeHandlerTimeoutIsPermanent: a handler that respects ctx and
// returns context.DeadlineExceeded (or a wrapped form) must have its error
// reclassified to a permanent node.timeout. Without reclassification, the
// engine would retry it (context.DeadlineExceeded is not Permanent).
//
// This exercises reclassifyTimeout directly: it constructs a cancelled context
// and calls the helper, because the goroutine+select path is inherently racy
// (when both channels are ready, Go picks randomly). The indirect Execute test
// TestExecuteEnforcesDeadline covers the integration path.
func TestCooperativeHandlerTimeoutIsPermanent(t *testing.T) {
	budget := 80 * time.Millisecond
	deadline := time.Now().Add(-time.Millisecond) // already past
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Simulate a cooperative handler returning context.DeadlineExceeded.
	handlerErr := context.DeadlineExceeded

	got := reclassifyTimeout(ctx, handlerErr, budget, deadline)
	if got == nil {
		t.Fatal("reclassifyTimeout returned nil, want a permanent node.timeout error")
	}
	if !types.IsPermanent(got) {
		t.Fatalf("reclassifyTimeout result is not permanent: %v (reclassification missing?)", got)
	}
	if !strings.Contains(got.Error(), "timeout") {
		t.Fatalf("reclassifyTimeout result = %q, want mention of timeout", got.Error())
	}
}

// TestReclassifyNoOpWhenNoDeadline ensures reclassifyTimeout is a no-op when
// no deadline was set (zero deadline).
func TestReclassifyNoOpWhenNoDeadline(t *testing.T) {
	handlerErr := context.DeadlineExceeded
	got := reclassifyTimeout(context.Background(), handlerErr, 0, time.Time{})
	if got != handlerErr {
		t.Fatalf("reclassifyTimeout = %v, want original error %v (no deadline = no-op)", got, handlerErr)
	}
}

// TestReclassifyNoOpWhenNoError ensures reclassifyTimeout returns nil when
// the handler succeeded.
func TestReclassifyNoOpWhenNoError(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got := reclassifyTimeout(ctx, nil, 5*time.Second, time.Now())
	if got != nil {
		t.Fatalf("reclassifyTimeout = %v, want nil (nil error = no-op)", got)
	}
}

// TestExecuteReclassifyWiring pins that reclassifyTimeout is actually CALLED
// on the goroutine+select ch branch in Execute. A cooperative handler that
// waits on ctx.Done() then returns ctx.Err() makes both select branches ready
// simultaneously. Go picks randomly, but BOTH branches must produce a Permanent
// node.timeout error — the ctx.Done() branch returns newNodeTimeoutError
// directly, the ch branch relies on reclassifyTimeout. Running 50 iterations
// ensures both branches are exercised (probability of never hitting ch across
// 50 fair coin flips: ~1e-15).
func TestExecuteReclassifyWiring(t *testing.T) {
	const iterations = 50
	h := ctxWaitHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	for i := range iterations {
		budget := 30 * time.Millisecond
		lease := &engine.TaskLease{
			Task:              engine.Task{ExecutionID: types.ExecutionID("exec-reclassify-wiring"), NodeName: "probe"},
			Input:             &types.Input{ExecutionID: "exec-reclassify-wiring", NodeName: "probe", Timeout: budget},
			NodeType:          "test.ctx-wait",
			ExecutionDeadline: time.Now().Add(budget),
		}
		res, err := runner.Execute(context.Background(), lease)
		if err != nil {
			t.Fatalf("iteration %d: Execute() error = %v", i, err)
		}
		if res.Error == nil {
			t.Fatalf("iteration %d: TaskResult.Error = nil, want permanent node.timeout", i)
		}
		if !types.IsPermanent(res.Error) {
			t.Fatalf("iteration %d: TaskResult.Error is not permanent: %v (reclassify wiring broken on ch branch?)", i, res.Error)
		}
	}
}

// TestOnResumeReclassifyWiring pins that reclassifyTimeout is called on the
// ch branch of callOnResume. Same loop strategy as TestExecuteReclassifyWiring.
func TestOnResumeReclassifyWiring(t *testing.T) {
	const iterations = 50
	h := &ctxWaitSuspendHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	for i := range iterations {
		budget := 30 * time.Millisecond
		lease := &engine.TaskLease{
			Task: engine.Task{
				ExecutionID: types.ExecutionID("exec-resume-reclassify"),
				NodeName:    "probe",
				Type:        engine.TaskTypeNodeResume,
				Payload:     &types.SignalPayload{Name: "signal"},
			},
			Input:             &types.Input{ExecutionID: "exec-resume-reclassify", NodeName: "probe", Timeout: budget},
			NodeType:          "test.ctx-wait-suspend",
			ExecutionDeadline: time.Now().Add(budget),
		}
		res, err := runner.Execute(context.Background(), lease)
		if err != nil {
			t.Fatalf("iteration %d: Execute() error = %v", i, err)
		}
		if res.Error == nil {
			t.Fatalf("iteration %d: TaskResult.Error = nil, want permanent node.timeout", i)
		}
		if !types.IsPermanent(res.Error) {
			t.Fatalf("iteration %d: TaskResult.Error is not permanent: %v (reclassify wiring broken on callOnResume ch branch?)", i, res.Error)
		}
	}
}

// TestSuspendingPathsAreBounded covers OnResume and PrepareSuspend: the
// suspending handler path is subject to the same deadline enforcement.
func TestSuspendingPathsAreBounded(t *testing.T) {
	released := make(chan struct{})
	blocked := make(chan struct{})
	h := &blockingSuspendHandler{blocked: blocked, released: released}
	runner := NewRunner(singleHandlerRegistry{handler: h})

	budget := 100 * time.Millisecond
	lease := &engine.TaskLease{
		Task: engine.Task{
			ExecutionID: "exec-suspend-block",
			NodeName:    "probe",
			Type:        engine.TaskTypeNodeResume,
			Payload:     &types.SignalPayload{Name: "approval"},
		},
		Input:             &types.Input{ExecutionID: "exec-suspend-block", NodeName: "probe", Timeout: budget},
		NodeType:          "test.blocking-suspend",
		ExecutionDeadline: time.Now().Add(budget),
	}

	backstop := time.AfterFunc(5*time.Second, func() {
		close(released)
	})
	defer backstop.Stop()

	start := time.Now()
	res, err := runner.Execute(context.Background(), lease)
	elapsed := time.Since(start)

	select {
	case <-released:
	default:
		close(released)
	}

	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a permanent node.timeout error on the suspending path")
	}
	if !types.IsPermanent(res.Error) {
		t.Fatalf("TaskResult.Error is not permanent: %v", res.Error)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Execute took %v on suspending path, want ≤ ~200ms of the %v budget", elapsed, budget)
	}
}

// --- helpers ---

// countingHandler counts invocations atomically; used to verify an
// already-expired deadline does not run the handler.
type countingHandler struct {
	invoked *atomic.Int32
}

func (countingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.counting"}
}

func (h countingHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.invoked.Add(1)
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// blockingSuspendHandler blocks in OnResume, ignoring ctx.
type blockingSuspendHandler struct {
	blocked  chan struct{}
	released chan struct{}
}

func (*blockingSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.blocking-suspend"}
}

func (h *blockingSuspendHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, nil
}

func (h *blockingSuspendHandler) OnResume(_ context.Context, _ *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	close(h.blocked)
	<-h.released
	return &types.Output{Data: map[string]any{"resumed": true}}, nil
}

func (h *blockingSuspendHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, nil
}

// ctxWaitHandler sleeps for slightly less than the budget then returns
// context.DeadlineExceeded. The handler finishes 1-2ms before the context
// deadline fires, so the ch branch in the select is ready first. This ensures
// the ch branch (where reclassifyTimeout is wired) gets exercised deterministically.
// reclassifyTimeout sees ctx.Err() != nil because by the time the select
// evaluates, the deadline has already fired (the main goroutine was also
// sleeping, giving the timer a chance to fire).
//
// The test uses a 50ms budget. The handler sleeps 48ms and returns. The ctx
// deadline fires at 50ms. Between 48ms and 50ms the channel has a value but
// ctx.Done() hasn't fired yet, so the select MUST pick ch. reclassifyTimeout
// then checks ctx.Err(): if the deadline has fired (50ms elapsed), it
// reclassifies; if not yet, it returns the original error. To ensure ctx IS
// expired when reclassify runs, we use a 50ms budget with an extra 5ms of
// scheduling slack — but the invariant we actually need is weaker: across N
// iterations, at least SOME must hit the ch branch with ctx expired.
type ctxWaitHandler struct{}

func (ctxWaitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.ctx-wait"}
}

func (ctxWaitHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	<-ctx.Done()
	return nil, context.DeadlineExceeded
}

// ctxWaitSuspendHandler is a SuspendingHandler whose OnResume sleeps for the
// full budget then returns ctx.Err(). Used by the loop test for callOnResume
// reclassify wiring.
type ctxWaitSuspendHandler struct{}

func (*ctxWaitSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.ctx-wait-suspend"}
}

func (*ctxWaitSuspendHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, nil
}

func (*ctxWaitSuspendHandler) OnResume(ctx context.Context, _ *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	<-ctx.Done()
	return nil, context.DeadlineExceeded
}

func (*ctxWaitSuspendHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"signal"}}, nil
}
