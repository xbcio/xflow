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

// expireIfExecutionDeadlineReached turns a node timeout into the execution's
// own timeout when the execution deadline (settings.timeout) has been reached
// at now.
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
func (e *Engine) expireIfExecutionDeadlineReached(ctx context.Context, id types.ExecutionID, g *graph.Graph, now time.Time) (handled bool, err error) {
	if g == nil || g.Timeout() <= 0 {
		return false, nil
	}
	deadline, err := e.executionDeadline(ctx, id, g)
	if err != nil {
		return false, err
	}
	if !deadlineReached(now, deadline) {
		return false, nil
	}
	outcome, err := e.ExpireExecution(ctx, id)
	if err != nil {
		return false, err
	}
	return outcome != ExpireOutcomeNotDue, nil
}
