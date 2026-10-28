package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

func (e *Engine) notifyNodeSuspended(ctx context.Context, t *Task) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeSuspended(hookCtx, t.ExecutionID, t.NodeName)
	})
}

func (e *Engine) notifyNodeComplete(ctx context.Context, id types.ExecutionID, nodeName string, status types.NodeStatus) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeComplete(hookCtx, id, nodeName, status)
	})
}

// notifyExecutionComplete is the engine's one observation that an execution
// reached a terminal status. Every finalization path funnels through it — the
// atomic commit's ExecutionDone, the cyclic and group commits, the legacy
// completion check, and Cancel — which is what makes it the right place for the
// sub-workflow parent notification: a child's conclusion has exactly one
// trigger, however the child concluded.
func (e *Engine) notifyExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	e.notifySubWorkflowParent(ctx, id)
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnExecutionComplete(hookCtx, id, status)
	})
}

// notifySubWorkflowParent tells the parent of a concluded child execution that
// its wait can be resolved now.
//
// It is deliberately best effort, and deliberately the ONLY trigger: a delivery
// that is lost or refused costs latency, not correctness, because the parent
// node's wait lease expires and the sweeper redelivers its dispatch task, which
// then finds the child already concluded (concludeTerminalChild). That split is
// why there is no durable completion intent and no outbox write order to get
// right: correctness lives in the reclaim backstop, and this path only exists
// to make the common case fast.
//
// Concluding is also where the child's conclusion is recorded in the parent's
// slot (resolveSubWorkflowConclusion), while the child's records still exist to
// be read. That makes this the primary writer of the conclusion; the reclaim
// path writes it too, so a notification lost before the record is still covered
// as long as the child's records outlive the wait's lease.
//
// An execution with no ParentNode is not a sub-workflow child — a submission,
// and a rerun, both have none — and is skipped without further work.
func (e *Engine) notifySubWorkflowParent(ctx context.Context, childID types.ExecutionID) {
	snap, err := e.state.GetExecution(ctx, childID)
	if err != nil {
		e.logSubWorkflowNotifyFailure(ctx, childID, err)
		return
	}
	if snap == nil || snap.ParentID == "" || snap.ParentNode == "" {
		return
	}
	if _, err := e.completeSubWorkflowNode(ctx, snap.ParentID, snap.ParentNode, childID); err != nil {
		e.logSubWorkflowNotifyFailure(ctx, childID, err)
	}
}

// logSubWorkflowNotifyFailure reports a notification that could not be
// delivered. This is a log-and-continue path by contract: the child's own
// finalization has already applied, and failing it retroactively would present
// a lost notification as a failed execution. The reclaim backstop resolves the
// parent node either way.
func (e *Engine) logSubWorkflowNotifyFailure(ctx context.Context, childID types.ExecutionID, err error) {
	if e.logger == nil {
		return
	}
	e.logger.Error("sub-workflow completion notification failed",
		"child_execution_id", string(childID), "err", err)
}

func (e *Engine) notifyNodeRetry(ctx context.Context, id types.ExecutionID, nodeName string, attempt int, delay time.Duration) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeRetry(hookCtx, id, nodeName, attempt, delay)
	})
}
