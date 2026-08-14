package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

type executionTTLCtxKey struct{}
type workflowDefCtxKey struct{}
type executionIDCtxKey struct{}
type traceIDCtxKey struct{}
type spanIDCtxKey struct{}
type traceCarrierCtxKey struct{}
type executionScopeCtxKey struct{}
type executionTransientCtxKey struct{}

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

// WithExecutionID attaches a pre-allocated execution id to the submission
// context so the apiserver authz wrapper can stamp the admission audit row with
// the same id the engine will persist. When set, Submit/Invoke reuse it instead
// of minting a fresh id; this closes the audit↔execution correlation gap that
// left reconcile Probe reading an empty ExecutionID (R3.1). Empty id is ignored.
func WithExecutionID(ctx context.Context, id types.ExecutionID) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, executionIDCtxKey{}, id)
}

// ExecutionIDFromContext extracts a pre-allocated execution id attached to the
// submission context. Returns "" when none was attached.
func ExecutionIDFromContext(ctx context.Context) (types.ExecutionID, bool) {
	id, ok := ctx.Value(executionIDCtxKey{}).(types.ExecutionID)
	return id, ok && id != ""
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

// WithTraceCarrier attaches the W3C traceparent/tracestate carrier captured at
// submission so the engine can persist it on the ExecutionSnapshot and a later
// asynchronous dispatch can extract a real remote parent (via the W3C
// propagator) for the dispatch span. Unlike TraceID/SpanID, the full carrier
// preserves tracestate and the sampled flag and is parsed — not reconstructed
// — by the propagator, so it does not fake a parent (RELEASE-GATES §4).
func WithTraceCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return context.WithValue(ctx, traceCarrierCtxKey{}, carrier)
}

// TraceCarrierFromContext extracts the W3C carrier attached to the submission
// context. Returns nil when none was attached.
func TraceCarrierFromContext(ctx context.Context) map[string]string {
	carrier, _ := ctx.Value(traceCarrierCtxKey{}).(map[string]string)
	return carrier
}

// WithExecutionScope attaches expression roots that belong to the whole
// execution rather than to one node -- today, a map body's $item/$index/$items.
// The engine persists them on the ExecutionSnapshot, and buildInput merges them
// into every node's Data, so a body member sees them wherever it sits in the
// body's graph.
//
// This exists as a submission-context value rather than a Submit parameter for
// the same reason WithExecutionID does: Submit's signature is on the public SDK
// surface and every caller would have to grow a parameter it has nothing to put
// in. Callers that attach nothing are unaffected -- an absent scope leaves the
// snapshot's field nil, which is what every non-body submission has always had.
func WithExecutionScope(ctx context.Context, scope map[string]any) context.Context {
	if len(scope) == 0 {
		return ctx
	}
	return context.WithValue(ctx, executionScopeCtxKey{}, scope)
}

// ExecutionScopeFromContext extracts the execution-wide expression roots
// attached to the submission context. Returns nil when none were attached.
func ExecutionScopeFromContext(ctx context.Context) map[string]any {
	scope, _ := ctx.Value(executionScopeCtxKey{}).(map[string]any)
	return scope
}

// TransientHint carries per-execution transient mode settings through the
// submission context. It is set by the engine when the compiled graph declares
// WorkflowOptions.Transient=true, and read by the StateStore to apply
// per-execution transient behavior.
type TransientHint struct {
	TTL           time.Duration
	CompletionTTL time.Duration
}

// WithExecutionTransient attaches a per-execution transient hint to the
// submission context. The StateStore reads it at CreateExecution time to
// apply per-execution transient behavior (skip SQL audit, apply TTL).
func WithExecutionTransient(ctx context.Context, hint TransientHint) context.Context {
	return context.WithValue(ctx, executionTransientCtxKey{}, hint)
}

// ExecutionTransientFromContext extracts the per-execution transient hint.
// Returns the hint and true when the execution should be transient.
func ExecutionTransientFromContext(ctx context.Context) (TransientHint, bool) {
	hint, ok := ctx.Value(executionTransientCtxKey{}).(TransientHint)
	return hint, ok
}

