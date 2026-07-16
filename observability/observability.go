package observability

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// HookChain fans engine lifecycle hooks out to multiple receivers.
type HookChain []engine.Hooks

func (c HookChain) OnNodeStart(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeStart(ctx, id, name) })
}
func (c HookChain) OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeComplete(ctx, id, name, status) })
}
func (c HookChain) OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeSuspended(ctx, id, name) })
}
func (c HookChain) OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnExecutionComplete(ctx, id, status) })
}
func (c HookChain) OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalDelivered(ctx, id, signalName, data) })
}
func (c HookChain) OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalRevoked(ctx, id, signalName) })
}
func (c HookChain) OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeTimeout(ctx, id, nodeName) })
}
func (c HookChain) OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, attempt int, delay time.Duration) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeRetry(ctx, id, name, attempt, delay) })
}

func (c HookChain) each(ctx context.Context, fn func(engine.Hooks, context.Context)) {
	for _, h := range c {
		if h == nil {
			continue
		}
		engine.SafeHook(ctx, nil, func(ctx context.Context) { fn(h, ctx) })
	}
}
