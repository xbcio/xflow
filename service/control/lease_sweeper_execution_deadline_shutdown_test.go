package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// cancelOnExpire cancels the pass's context from inside ExpireExecution and then
// answers as the wrapped fake does, which is how a shutdown that lands during a
// call is made. The error it returns is a plain one, not context.Canceled, so
// only the context itself tells the pass the failure was a shutdown.
type cancelOnExpire struct {
	*fakeExpirer
	cancel func()
}

func (c cancelOnExpire) ExpireExecution(ctx context.Context, id types.ExecutionID) (engine.ExpireOutcome, error) {
	c.cancel()
	return c.fakeExpirer.ExpireExecution(ctx, id)
}

// TestLeaseSweeperExecutionDeadlineShutdownIsNotAFailure pins that an error which
// arrives because the sweeper's context ended is not held against the
// execution: no failure count, no backoff window and no alert, however many
// shutdowns it lands in, and the pass stops at that execution.
func TestLeaseSweeperExecutionDeadlineShutdownIsNotAFailure(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	ids := h.list("exec", 3, h.now.Add(-time.Hour))
	for _, id := range ids {
		h.expirer.errs[id] = errStoreDown
	}
	var cancel context.CancelFunc
	h.LeaseSweeper.engine = cancelOnExpire{fakeExpirer: h.expirer, cancel: func() { cancel() }}

	const passes = 120
	for i := 0; i < passes; i++ {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		h.ReapExpiredExecutionsOnce(ctx)
		cancel()
		// Past the longest backoff, so a failure recorded by mistake would not hide.
		*h.now = h.now.Add(10 * time.Minute)
	}

	if got := len(h.expirer.snapshot()); got != passes {
		t.Fatalf("ExpireExecution calls = %d, want %d: one per pass, which stops at the shutdown", got, passes)
	}
	if got := h.deadlineBackoff.Len(); got != 0 {
		t.Fatalf("backoff records = %d, want a shutdown to leave none", got)
	}
	if got := len(h.warnings(keepsFailingMsg)); got != 0 {
		t.Fatalf("failure warnings = %d, want none for a shutdown", got)
	}
}

// TestLeaseSweeperExecutionDeadlineCanceledContextAttemptsNothing pins that a
// pass whose context has already ended expires nothing and records nothing.
func TestLeaseSweeperExecutionDeadlineCanceledContextAttemptsNothing(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	h.list("exec", 5, h.now.Add(-time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if got := h.ReapExpiredExecutionsOnce(ctx); got != 0 {
		t.Fatalf("pass on a canceled context released %d, want 0", got)
	}
	if got := len(h.expirer.snapshot()); got != 0 {
		t.Fatalf("ExpireExecution calls = %d, want none on a canceled context", got)
	}
	if got := h.deadlineBackoff.Len(); got != 0 {
		t.Fatalf("backoff records = %d, want none", got)
	}
}
