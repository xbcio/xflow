package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

type executionTTLCtxKey struct{}
type workflowDefCtxKey struct{}

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
