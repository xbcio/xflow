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
