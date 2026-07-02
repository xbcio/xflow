package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

type executionTTLCtxKey struct{}
type workflowDefCtxKey struct{}
type traceIDCtxKey struct{}
type spanIDCtxKey struct{}

// WithExecutionTTL attaches a retention TTL hint for a single execution
// submission. StateStore implementations may use it to choose key or record
// retention without coupling callers to a concrete storage backend.
func WithExecutionTTL(ctx context.Context, d time.Duration) context.Context {
	if d <= 0 {
		return ctx
	}
	return context.WithValue(ctx, executionTTLCtxKey{}, d)
}

// ExecutionTTLFromContext extracts the execution retention TTL hint.
func ExecutionTTLFromContext(ctx context.Context) (time.Duration, bool) {
	d, ok := ctx.Value(executionTTLCtxKey{}).(time.Duration)
	return d, ok && d > 0
}

// WithWorkflowDef attaches the original workflow definition for a single
// execution submission. Concrete backends may use it to persist audit metadata
// without coupling engine callers to a storage-specific representation.
func WithWorkflowDef(ctx context.Context, def *types.WorkflowDef) context.Context {
	if def == nil {
		return ctx
	}
	return context.WithValue(ctx, workflowDefCtxKey{}, def)
}

// WorkflowDefFromContext extracts the original workflow definition attached to
// the submission context.
func WorkflowDefFromContext(ctx context.Context) (*types.WorkflowDef, bool) {
	def, ok := ctx.Value(workflowDefCtxKey{}).(*types.WorkflowDef)
	return def, ok && def != nil
}

// WithTraceID attaches trace metadata for a single execution submission. The
// engine persists it on the execution snapshot so later task leases can pass it
// to node handlers even when they run in another goroutine or process.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDCtxKey{}, traceID)
}

// TraceIDFromContext extracts the trace ID attached to the submission context.
func TraceIDFromContext(ctx context.Context) (string, bool) {
	traceID, ok := ctx.Value(traceIDCtxKey{}).(string)
	return traceID, ok && traceID != ""
}

// WithSpanID attaches span metadata for a single execution submission. The
// engine persists it on the execution snapshot so later task leases can pass it
// to node handlers even when they run in another goroutine or process.
func WithSpanID(ctx context.Context, spanID string) context.Context {
	if spanID == "" {
		return ctx
	}
	return context.WithValue(ctx, spanIDCtxKey{}, spanID)
}

// SpanIDFromContext extracts the span ID attached to the submission context.
func SpanIDFromContext(ctx context.Context) (string, bool) {
	spanID, ok := ctx.Value(spanIDCtxKey{}).(string)
	return spanID, ok && spanID != ""
}
