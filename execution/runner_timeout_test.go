package execution

import (
	"context"
	"errors"
	"strings"
	"sync"
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
// on the ch branch in Execute's select. A cooperative handler waits on
// ctx.Done() then returns context.DeadlineExceeded. The outer select nearly
// always picks ctx.Done() (the two branches are NOT a fair coin flip — the main
// goroutine is already blocked in select when ctx fires, while the handler
// goroutine still needs scheduling to write ch). The reclassify coverage comes
// from the INNER non-blocking select after runtime.Gosched(): the yield lets
// the handler goroutine run and write to ch (~56% hit rate per iteration).
// Removing reclassify from that inner path reddens the test on the first
// iteration that hits it (measured: iteration 0 fails immediately).
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

// --- parent-cancellation classification regression tests ---
//
// These pin the fix for the regression where isRunnerTimeout/reclassifyTimeout
// tested only ctx.Err() != nil, conflating context.DeadlineExceeded (a verdict
// -> permanent node.timeout) with context.Canceled (the lease-renewal loop
// cancelling execCtx when the lease is fenced or renewal fails MaxRetries
// times). A fenced/lost lease must NOT be committed as a terminal permanent
// failure by the runner that just lost it: the server redelivers, and a
// permanent error suppresses the retry the redelivery depends on.

// cancelCoopHandler waits for ctx cancellation then returns ctx.Err(). It is
// the cooperative mirror of blockingHandler: it respects ctx, so it surfaces
// whatever cancellation cause fired (DeadlineExceeded or Canceled) as its own
// error, which reclassifyTimeout must then classify.
type cancelCoopHandler struct{}

func (cancelCoopHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cancel-coop"}
}

func (cancelCoopHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// cancelCoopSuspendHandler is the SuspendingHandler form of cancelCoopHandler:
// OnResume waits for ctx cancellation then returns ctx.Err().
type cancelCoopSuspendHandler struct{}

func (*cancelCoopSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cancel-coop-suspend"}
}

func (*cancelCoopSuspendHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, nil
}

func (*cancelCoopSuspendHandler) OnResume(ctx context.Context, _ *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*cancelCoopSuspendHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"signal"}}, nil
}

// recordingTimeoutObserver captures OnNodeExecutionTimeout calls so a test can
// assert the timeout metric emission call site did NOT fire for a non-timeout
// cancellation. It is the in-package recording counterpart of the real adapter
// in observability/metrics; the end-to-end metric assertion lives in
// observability/metrics/node_timeout_test.go.
type recordingTimeoutObserver struct {
	mu       sync.Mutex
	timeouts int
}

func (o *recordingTimeoutObserver) OnNodeExecutionTimeout(context.Context, string, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timeouts++
}

func (o *recordingTimeoutObserver) OnHandlerAbandoned(context.Context, string, float64)     {}
func (o *recordingTimeoutObserver) OnHandlerDuration(context.Context, string, time.Duration) {}

func (o *recordingTimeoutObserver) timeoutCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.timeouts
}

// TestParentCancelCooperativeNotPermanentTimeout: the lease deadline is 10
// minutes away (nowhere near firing). The PARENT ctx is cancelled — exactly
// what renewLeaseLoop does when the lease is fenced or renewal fails MaxRetries
// times. A cooperative handler returns ctx.Err() (context.Canceled). This MUST
// NOT be reclassified to a permanent node.timeout; it must be a transient
// node.cancelled so the server can redeliver.
func TestParentCancelCooperativeNotPermanentTimeout(t *testing.T) {
	obs := &recordingTimeoutObserver{}
	runner := NewRunner(singleHandlerRegistry{handler: cancelCoopHandler{}}, WithTimeoutObserver(obs))

	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-cancel-coop", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-cancel-coop", NodeName: "probe", Timeout: 10 * time.Minute},
		NodeType:          "test.cancel-coop",
		ExecutionDeadline: time.Now().Add(10 * time.Minute),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	res, err := runner.Execute(ctx, lease)
	if err != nil {
		t.Fatalf("Execute() transport error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a non-nil cancellation error")
	}
	if types.IsPermanent(res.Error) {
		t.Errorf("PROBE RED: parent cancellation reported as PERMANENT node timeout: %v", res.Error)
	}
	if !errors.Is(res.Error, context.Canceled) && !strings.Contains(res.Error.Error(), "node.cancelled") {
		t.Errorf("TaskResult.Error = %v, want a transient node.cancelled (or wrapped context.Canceled), not a permanent timeout", res.Error)
	}
	if strings.Contains(res.Error.Error(), "10m0s timeout") {
		t.Errorf("TaskResult.Error message lies about a timeout that never happened: %q", res.Error.Error())
	}
	// The timeout metric must NOT increment for a non-timeout cancellation.
	if n := obs.timeoutCount(); n != 0 {
		t.Errorf("OnNodeExecutionTimeout fired %d time(s) for a parent cancellation, want 0 (metric labels are permanent time series)", n)
	}
}

// TestParentCancelAbandonBranchNotPermanentTimeout: a handler that IGNORES ctx
// (blockingHandler) is abandoned when the parent cancels (deadline 10 minutes
// away). The abandon branch must still produce a transient node.cancelled, not a
// permanent node.timeout. The abandoned gauge still increments — a handler that
// ignores ctx leaks a goroutine regardless of why its context was done.
func TestParentCancelAbandonBranchNotPermanentTimeout(t *testing.T) {
	obs := &recordingTimeoutObserver{}
	released := make(chan struct{})
	blocked := make(chan struct{})
	h := blockingHandler{blocked: blocked, released: released}
	runner := NewRunner(singleHandlerRegistry{handler: h}, WithTimeoutObserver(obs))

	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-cancel-abandon", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-cancel-abandon", NodeName: "probe", Timeout: 10 * time.Minute},
		NodeType:          "test.blocking",
		ExecutionDeadline: time.Now().Add(10 * time.Minute),
	}

	// Safety backstop: never let the test hang.
	go func() {
		time.Sleep(5 * time.Second)
		select {
		case <-released:
		default:
			close(released)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	res, err := runner.Execute(ctx, lease)
	// Unblock the leaked handler goroutine so it can exit.
	select {
	case <-released:
	default:
		close(released)
	}
	if err != nil {
		t.Fatalf("Execute() transport error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a non-nil cancellation error")
	}
	if types.IsPermanent(res.Error) {
		t.Errorf("abandon branch reported a PERMANENT node timeout for a parent cancellation: %v", res.Error)
	}
	if !strings.Contains(res.Error.Error(), "node.cancelled") {
		t.Errorf("TaskResult.Error = %v, want a transient node.cancelled from the abandon branch", res.Error)
	}
	// The timeout metric must NOT increment for a non-timeout cancellation,
	// even on the abandon branch.
	if n := obs.timeoutCount(); n != 0 {
		t.Errorf("OnNodeExecutionTimeout fired %d time(s) on the abandon branch for a parent cancellation, want 0", n)
	}
}

// TestParentCancelOnResumeAbandonNotPermanentTimeout covers the callOnResume
// abandon branch under parent cancellation (deadline far away).
func TestParentCancelOnResumeAbandonNotPermanentTimeout(t *testing.T) {
	obs := &recordingTimeoutObserver{}
	released := make(chan struct{})
	blocked := make(chan struct{})
	h := &blockingSuspendHandler{blocked: blocked, released: released}
	runner := NewRunner(singleHandlerRegistry{handler: h}, WithTimeoutObserver(obs))

	lease := &engine.TaskLease{
		Task: engine.Task{
			ExecutionID: "exec-cancel-resume",
			NodeName:    "probe",
			Type:        engine.TaskTypeNodeResume,
			Payload:     &types.SignalPayload{Name: "signal"},
		},
		Input:             &types.Input{ExecutionID: "exec-cancel-resume", NodeName: "probe", Timeout: 10 * time.Minute},
		NodeType:          "test.blocking-suspend",
		ExecutionDeadline: time.Now().Add(10 * time.Minute),
	}

	go func() {
		time.Sleep(5 * time.Second)
		select {
		case <-released:
		default:
			close(released)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	res, err := runner.Execute(ctx, lease)
	select {
	case <-released:
	default:
		close(released)
	}
	if err != nil {
		t.Fatalf("Execute() transport error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a non-nil cancellation error")
	}
	if types.IsPermanent(res.Error) {
		t.Errorf("callOnResume abandon branch reported a PERMANENT node timeout for a parent cancellation: %v", res.Error)
	}
	if !strings.Contains(res.Error.Error(), "node.cancelled") {
		t.Errorf("TaskResult.Error = %v, want a transient node.cancelled from the callOnResume abandon branch", res.Error)
	}
	if n := obs.timeoutCount(); n != 0 {
		t.Errorf("OnNodeExecutionTimeout fired %d time(s) on callOnResume abandon for a parent cancellation, want 0", n)
	}
}

// TestGenuineDeadlineCooperativeIsPermanentTimeout guards the other side of the
// fix: a genuine deadline firing must STILL produce a permanent node.timeout.
// This is a single clean Execute-level assertion (distinct from the loop-based
// TestExecuteReclassifyWiring) so a regression that collapses the two causes
// back into one reddens it directly.
func TestGenuineDeadlineCooperativeIsPermanentTimeout(t *testing.T) {
	obs := &recordingTimeoutObserver{}
	runner := NewRunner(singleHandlerRegistry{handler: ctxWaitHandler{}}, WithTimeoutObserver(obs))

	budget := 40 * time.Millisecond
	lease := &engine.TaskLease{
		Task:              engine.Task{ExecutionID: "exec-genuine-coop", NodeName: "probe"},
		Input:             &types.Input{ExecutionID: "exec-genuine-coop", NodeName: "probe", Timeout: budget},
		NodeType:          "test.ctx-wait",
		ExecutionDeadline: time.Now().Add(budget),
	}

	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() transport error = %v, want nil", err)
	}
	if res.Error == nil {
		t.Fatal("TaskResult.Error = nil, want a permanent node.timeout")
	}
	if !types.IsPermanent(res.Error) {
		t.Fatalf("TaskResult.Error is not permanent for a genuine deadline: %v", res.Error)
	}
	if !strings.Contains(res.Error.Error(), "node.timeout") && !strings.Contains(res.Error.Error(), "timeout") {
		t.Fatalf("TaskResult.Error = %q, want it to mention timeout", res.Error.Error())
	}
	if n := obs.timeoutCount(); n == 0 {
		t.Errorf("OnNodeExecutionTimeout did not fire for a genuine deadline, want >= 1")
	}
}

// TestReclassifyCancelCauseRules exercises classifyCancelCause directly for the
// racy "both causes present" case (req 3): a Canceled context whose deadline
// instant has elapsed must still classify as a timeout, while a Canceled
// context whose deadline is in the future classifies as cancelled.
func TestReclassifyCancelCauseRules(t *testing.T) {
	budget := 5 * time.Second

	// Genuine deadline: ctx.Err() == DeadlineExceeded -> permanent node.timeout.
	dlPast := time.Now().Add(-time.Second)
	ctxDL, cancelDL := context.WithDeadline(context.Background(), dlPast)
	defer cancelDL()
	got := reclassifyTimeout(ctxDL, context.DeadlineExceeded, budget, dlPast)
	if got == nil || !types.IsPermanent(got) {
		t.Fatalf("DeadlineExceeded -> got %v, want permanent node.timeout", got)
	}

	// Parent cancel, deadline far in the future -> transient node.cancelled.
	dlFuture := time.Now().Add(10 * time.Minute)
	ctxCancel, cancelCancel := context.WithCancel(context.Background())
	cancelCancel()
	got = reclassifyTimeout(ctxCancel, context.Canceled, budget, dlFuture)
	if got == nil {
		t.Fatal("Canceled (deadline future) -> nil, want transient node.cancelled")
	}
	if types.IsPermanent(got) {
		t.Fatalf("Canceled (deadline future) -> %v, want NOT permanent", got)
	}
	if !strings.Contains(got.Error(), "node.cancelled") {
		t.Fatalf("Canceled (deadline future) -> %q, want node.cancelled", got.Error())
	}

	// Both causes present, deadline elapsed: parent cancelled first so
	// ctx.Err() == Canceled, but the deadline instant has passed. Must
	// classify as a timeout (the budget was genuinely exhausted).
	dlElapsed := time.Now().Add(-time.Second)
	ctxBoth, cancelBoth := context.WithCancel(context.Background())
	cancelBoth()
	got = reclassifyTimeout(ctxBoth, context.Canceled, budget, dlElapsed)
	if got == nil || !types.IsPermanent(got) {
		t.Fatalf("Canceled+deadline-elapsed -> %v, want permanent node.timeout (deadline won the race to the budget)", got)
	}

	// nil err is a no-op even when the context is done (success preserved,
	// no timeout metric fires). This pins the minor fix: a handler that
	// succeeded at the deadline instant must not be reclassified.
	got = reclassifyTimeout(ctxDL, nil, budget, dlPast)
	if got != nil {
		t.Fatalf("nil err with done ctx -> %v, want nil (success preserved)", got)
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

// ctxWaitHandler waits for ctx cancellation then returns
// context.DeadlineExceeded. The outer select nearly always picks ctx.Done()
// (measured 0/100 taking the outer ch branch) because the main goroutine is
// already blocked in select when ctx fires, while the handler goroutine still
// needs to be scheduled to write to ch. The reclassify coverage comes from the
// INNER non-blocking select in the ctx.Done() branch: after runtime.Gosched()
// yields, the handler goroutine writes to ch, and the inner select catches it
// (~56% hit rate measured over 200 iterations). Removing reclassify from that
// inner path surfaces a bare context.DeadlineExceeded on the first iteration
// that hits it (empirically iteration 0 or 1).
type ctxWaitHandler struct{}

func (ctxWaitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.ctx-wait"}
}

func (ctxWaitHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	<-ctx.Done()
	return nil, context.DeadlineExceeded
}

// ctxWaitSuspendHandler is a SuspendingHandler whose OnResume waits for ctx
// cancellation then returns context.DeadlineExceeded. Same mechanism as
// ctxWaitHandler: reclassify coverage comes from the inner fallback select
// after Gosched in callOnResume's ctx.Done() branch.
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
