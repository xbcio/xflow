package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestExecutionTTLContextRoundTrip(t *testing.T) {
	ctx := WithExecutionTTL(context.Background(), 30*time.Minute)

	ttl, ok := ExecutionTTLFromContext(ctx)
	if !ok {
		t.Fatal("expected execution TTL metadata")
	}
	if ttl != 30*time.Minute {
		t.Fatalf("ttl = %v, want %v", ttl, 30*time.Minute)
	}
}

func TestExecutionTTLContextIgnoresNonPositiveDuration(t *testing.T) {
	ctx := WithExecutionTTL(context.Background(), 0)

	if ttl, ok := ExecutionTTLFromContext(ctx); ok {
		t.Fatalf("unexpected execution TTL metadata: %v", ttl)
	}
}

func TestWorkflowDefContextRoundTrip(t *testing.T) {
	def := &types.WorkflowDef{Name: "vulnerability-approval"}

	ctx := WithWorkflowDef(context.Background(), def)

	got, ok := WorkflowDefFromContext(ctx)
	if !ok {
		t.Fatal("expected workflow definition metadata")
	}
	if got != def {
		t.Fatalf("WorkflowDefFromContext() = %p, want %p", got, def)
	}
}

func TestTraceContextRoundTrip(t *testing.T) {
	ctx := WithTraceID(context.Background(), "trace-123")
	ctx = WithSpanID(ctx, "span-456")

	traceID, ok := TraceIDFromContext(ctx)
	if !ok {
		t.Fatal("expected trace ID metadata")
	}
	if traceID != "trace-123" {
		t.Fatalf("TraceIDFromContext() = %q, want trace-123", traceID)
	}

	spanID, ok := SpanIDFromContext(ctx)
	if !ok {
		t.Fatal("expected span ID metadata")
	}
	if spanID != "span-456" {
		t.Fatalf("SpanIDFromContext() = %q, want span-456", spanID)
	}
}

func TestTraceContextIgnoresEmptyValues(t *testing.T) {
	ctx := WithTraceID(context.Background(), "")
	ctx = WithSpanID(ctx, "")

	if traceID, ok := TraceIDFromContext(ctx); ok {
		t.Fatalf("unexpected trace ID metadata: %q", traceID)
	}
	if spanID, ok := SpanIDFromContext(ctx); ok {
		t.Fatalf("unexpected span ID metadata: %q", spanID)
	}
}
