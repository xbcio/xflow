package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// BaseHooks provides no-op implementations of all Hooks methods.
// Embed it in your hook struct to only override methods you care about.
type BaseHooks struct{}

func (BaseHooks) OnNodeStart(_ context.Context, _ types.ExecutionID, _ string) {}
func (BaseHooks) OnNodeComplete(_ context.Context, _ types.ExecutionID, _ string, _ types.NodeStatus) {
}
func (BaseHooks) OnNodeSuspended(_ context.Context, _ types.ExecutionID, _ string) {}
func (BaseHooks) OnExecutionComplete(_ context.Context, _ types.ExecutionID, _ types.ExecutionStatus) {
}
func (BaseHooks) OnSignalDelivered(_ context.Context, _ types.ExecutionID, _ string, _ map[string]any) {
}
func (BaseHooks) OnSignalRevoked(_ context.Context, _ types.ExecutionID, _ string) {}
func (BaseHooks) OnNodeTimeout(_ context.Context, _ types.ExecutionID, _ string)   {}
func (BaseHooks) OnNodeRetry(_ context.Context, _ types.ExecutionID, _ string, _ int, _ time.Duration) {
}

// SafeHook wraps a hook call with panic recovery and a 5s timeout.
// Exported for use by adapter packages (e.g. timeout monitor).
func SafeHook(ctx context.Context, logger Logger, fn func(ctx context.Context)) {
	safeHook(ctx, logger, fn)
}

// safeHook wraps a hook call with panic recovery and a 5s timeout.
func safeHook(ctx context.Context, logger Logger, fn func(ctx context.Context)) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			if logger != nil {
				logger.Error("hook panic recovered", "panic", r)
			}
		}
	}()
	fn(ctx)
}
