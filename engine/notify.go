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

func (e *Engine) notifyExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnExecutionComplete(hookCtx, id, status)
	})
}

func (e *Engine) notifyNodeRetry(ctx context.Context, id types.ExecutionID, nodeName string, attempt int, delay time.Duration) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeRetry(hookCtx, id, nodeName, attempt, delay)
	})
}
