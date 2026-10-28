package distributed

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
)

// defaultDeadlineSweepInterval is how often an embedded backend looks for
// executions past their settings.timeout deadline. It matches the lease timeout
// monitor's poll, which is the other background loop an embedded backend owns:
// an execution can overrun its deadline by up to one interval.
const defaultDeadlineSweepInterval = 5 * time.Second

// deadlineSweepBatch bounds how many overdue executions one sweep expires. The
// rest are still listed on the next sweep.
const deadlineSweepBatch = engine.DefaultDeadlineSweepBatch

// runDeadlineSweeper expires the executions whose settings.timeout deadline has
// passed with no task left to notice it, until ctx is canceled.
//
// A running node enforces the deadline through its own lease, but an execution
// that is parked (suspended, paused, queued), between tasks, or whose runner
// died holds no lease, so nothing on the task path fires for it. The control
// plane's leader-gated deadline pass covers a deployment that has one; an
// embedded backend (xflow.NewCluster) has none, so without this loop such an
// execution would outlive its deadline for good.
//
// It is safe to run in every embedded process against one Redis, and next to a
// control plane's pass: ExpireExecution is idempotent, the timeout status is a
// fenced compare-and-set, and the index entry is acknowledged only after the
// cascade finished. The one overlap that remains is the narrow window
// ExpireExecution documents, in which two concurrent callers both announce the
// completion hook.
//
// The loop body is engine.DeadlineSweep, which sets an execution that fails (or
// is canceling) aside for a growing window so the oldest entries cannot hold
// every slot of the page and starve the later ones.
func (b *Backend) runDeadlineSweeper(ctx context.Context, expirer engine.ExecutionExpirer, interval time.Duration) {
	if interval <= 0 {
		interval = defaultDeadlineSweepInterval
	}
	var logger engine.DeadlineSweepLogger
	if b.logger != nil {
		logger = b.logger
	}
	sweepCfg := engine.DeadlineSweepConfig{
		Lister:  b.state,
		Expirer: expirer,
		Batch:   deadlineSweepBatch,
		Logger:  logger,
	}
	if b.deadlineExpiredObserver != nil {
		observer := b.deadlineExpiredObserver
		sweepCfg.OnExpired = func(execCtx context.Context, _ engine.DeadlineExpiredExecution) {
			// The observer is host-supplied: a panic here would otherwise leave
			// this goroutine and take every later deadline with it.
			defer func() { _ = recover() }()
			observer.OnExecutionDeadlineExpired(execCtx, engine.DeadlineExpiredSourceEmbedded)
		}
	}
	sweep := engine.NewDeadlineSweep(sweepCfg)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := sweep.Sweep(ctx); err != nil && b.logger != nil {
				b.logger.Error("execution_deadline_sweep_failed", "err", err)
			}
		}
	}
}
