package xflow

import (
	"context"
	"errors"
	"testing"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
)

// TestInvokeIdempotencyKeyReturnsTheSameExecution: the SDK's keyed invocation
// starts one execution, and the retry of the same request is answered with it
// whatever the caller's input map order was. The engine's own contract is in
// engine/ and backend/internal/statestoretest; what is pinned here is that the
// SDK option reaches it with the workflow-scoped key it promises.
func TestInvokeIdempotencyKeyReturnsTheSameExecution(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("idem-invoke")
	start := wf.Node("start", node.Start())
	done := wf.Node("done", node.Function("return input"))
	wf.Connect(start, done)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	input := map[string]any{"order": "o-1", "qty": 2}
	first, err := eng.Invoke(ctx, workflowID, Start(), input, WithIdempotencyKey("order-1"))
	if err != nil {
		t.Fatalf("first keyed Invoke() error = %v", err)
	}
	// The same request rebuilt in another map order is the same request.
	second, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"qty": 2, "order": "o-1"}, WithIdempotencyKey("order-1"))
	if err != nil {
		t.Fatalf("retried keyed Invoke() error = %v", err)
	}
	if second != first {
		t.Fatalf("retry returned %q, want the original %q", second, first)
	}

	// A different request under the same key is a conflict, not a duplicate.
	if _, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"order": "o-2"}, WithIdempotencyKey("order-1")); !errors.Is(err, enginecore.ErrIdempotencyConflict) {
		t.Fatalf("reused key with different input error = %v, want ErrIdempotencyConflict", err)
	}

	// An invocation without a key is unaffected: a fresh execution every time.
	unkeyed, err := eng.Invoke(ctx, workflowID, Start(), input)
	if err != nil {
		t.Fatalf("unkeyed Invoke() error = %v", err)
	}
	if unkeyed == first {
		t.Fatal("an unkeyed Invoke reused the keyed execution")
	}
}

// TestFireAndForgetRejectsIdempotencyKey: FAF persists no execution, so a key
// cannot be honored. It is refused rather than ignored, and the refusal happens
// before anything runs.
func TestFireAndForgetRejectsIdempotencyKey(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	handler := &fafNoopHandler{nodeType: "test.faf.idempotency"}
	faf := Workflow("faf-idempotency").FAF()
	faf.LocalNode("only", handler)
	fafID, err := eng.AddWorkflow(context.Background(), faf)
	if err != nil {
		t.Fatalf("AddWorkflow(FAF) error = %v", err)
	}
	if err := eng.FireAndForget(context.Background(), fafID, nil, WithIdempotencyKey("k")); !errors.Is(err, ErrFAFIdempotencyUnsupported) {
		t.Fatalf("FireAndForget with a key error = %v, want ErrFAFIdempotencyUnsupported", err)
	}
	if handler.executed.Load() {
		t.Fatal("a refused keyed FireAndForget executed the handler")
	}
}
