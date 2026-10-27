package engine

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// isNodeTimeoutError reports whether err is the permanent error a node fails
// with when its lease deadline elapses (types.NodeTimeoutErrorCode). The code is
// the only thing that tells a timeout from any other node failure once the
// result has crossed the Runner Protocol.
func isNodeTimeoutError(err error) bool {
	var ce *types.ClassifiedError
	return errors.As(err, &ce) && ce.Code == types.NodeTimeoutErrorCode
}

// executionDeadlineClockSlack is how far before the execution deadline a node
// timeout may arrive and still be the execution's timeout, for a lease that was
// cut off at that deadline.
//
// The two sides of the comparison run on different clocks. The runner cuts a
// node off with a context timer (monotonic on one machine, the runner's own
// wall clock across machines) set to the lease's ExecutionDeadline, while the
// guard compares the server's wall clock with the stored deadline. Any drift
// between them (an NTP slew, a step, a runner whose clock runs ahead) makes the
// cut-off land a few milliseconds before the server's wall clock reaches the
// deadline, the guard answers not due, and on_error then routes around what was
// the execution's own timeout: stop ends the execution as failed for good, and
// continue keeps it running past its budget.
//
// It is a bound, not a configuration: it covers drift, not a node that really
// finished early, so it must stay far below anything a user could call a
// separate budget (never above 100ms). It is neither configurable nor on the
// wire, so a runner cannot widen it, and the most a runner that lies about its
// clock gains is one slack of earlier expiry on a lease already cut off at the
// execution deadline.
const executionDeadlineClockSlack = 50 * time.Millisecond

// leaseClampedToExecutionDeadline reports whether a lease's effective deadline
// is the execution deadline itself, as opposed to a shorter budget of the node's
// own. Both values are the server's: a lease reaching a commit has been resolved
// from the server's directory, and the execution deadline is the stored one, so
// a runner cannot claim to have been cut off at the execution deadline.
//
// A lease deadline later than the execution's cannot exist (it is clamped), and
// the boundary counts as clamped: a node budget that ends exactly at the
// execution's is the execution's timeout by the same tie-break deadlineReached
// applies. A zero lease deadline carries no evidence either way and is not
// clamped, so only a lease that visibly ends at the execution deadline gets the
// slack.
func leaseClampedToExecutionDeadline(leaseDeadline, execDeadline time.Time) bool {
	return !leaseDeadline.IsZero() && !execDeadline.IsZero() && !leaseDeadline.Before(execDeadline)
}

// expireIfExecutionDeadlineReached turns a node timeout into the execution's
// own timeout when the execution deadline (settings.timeout) has been reached
// at now.
//
// This entry compares the clocks exactly. It is for the callers whose trigger
// and comparison share one clock (the renewal backstop and the sub-workflow
// wait end, both read off the server's wall clock); a timeout that a runner
// reports goes through expireIfLeaseTimeoutReached instead.
func (e *Engine) expireIfExecutionDeadlineReached(ctx context.Context, id types.ExecutionID, g *graph.Graph, now time.Time) (handled bool, err error) {
	return e.expireIfLeaseTimeoutReached(ctx, id, g, now, time.Time{})
}

// expireIfLeaseTimeoutReached turns a node timeout into the execution's own
// timeout when the execution deadline (settings.timeout) has been reached at
// now.
//
// A lease's ExecutionDeadline is the smaller of the node's budget and the
// execution's (see clampToExecutionDeadline), so a node timeout alone cannot
// say which budget ran out. The execution's deadline is the authority: it is
// compared at the snapshot's value, not the folded lease value, and the
// boundary instant counts as reached (deadlineReached), so a node and an
// execution budget that run out together are the execution's timeout. Only
// then is on_error out of the picture: a timeout that has consumed the whole
// execution budget is not a node failure to route around, and continuing past
// it would keep an execution running beyond its budget.
//
// leaseDeadline is the effective deadline of the lease that timed out, after
// clamping. When it is the execution deadline (leaseClampedToExecutionDeadline)
// the node was cut off exactly there by a timer on another clock, so the
// deadline counts as reached executionDeadlineClockSlack early. When the lease
// ended earlier, on the node's own budget, the node really did time out before
// the execution did and the comparison stays exact: widening it would turn a
// node failure on_error is meant to handle into the execution's timeout. Zero
// means the caller has no lease to judge by, which is also exact.
//
// handled reports that the execution deadline is reached and ExpireExecution
// has settled it, so the caller must not apply on_error. That includes every
// non-error outcome of ExpireExecution except NotDue: an execution already
// concluded, canceling or gone leaves this commit with nothing to do, the same
// answer CommitTaskTimeout gives for an inactive execution. NotDue means the
// stored deadline is not reached after all, so the commit proceeds as before.
// A store failure is returned as is; the caller retries and ExpireExecution
// resumes from wherever the failed call stopped.
//
// A graph without settings.timeout answers from g alone: no store read, and the
// commit paths stay byte-identical for it.
func (e *Engine) expireIfLeaseTimeoutReached(ctx context.Context, id types.ExecutionID, g *graph.Graph, now, leaseDeadline time.Time) (handled bool, err error) {
	if g == nil || g.Timeout() <= 0 {
		return false, nil
	}
	deadline, err := e.executionDeadline(ctx, id, g)
	if err != nil {
		return false, err
	}
	var slack time.Duration
	if leaseClampedToExecutionDeadline(leaseDeadline, deadline) {
		slack = executionDeadlineClockSlack
	}
	if !deadlineReached(now.Add(slack), deadline) {
		return false, nil
	}
	outcome, err := e.expireExecution(ctx, id, slack)
	if err != nil {
		return false, err
	}
	return outcome != ExpireOutcomeNotDue, nil
}
