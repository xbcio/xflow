package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

// TestRerunIdempotencyKeyReturnsTheSameRerun: the rerun option is scoped to the
// execution being rerun, so a retried rerun finds the execution the first attempt
// created and reports it as a duplicate.
func TestRerunIdempotencyKeyReturnsTheSameRerun(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("idem-rerun")
	start := wf.Node("start", node.Start())
	done := wf.Node("done", node.Function("return input"))
	wf.Connect(start, done)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	parent, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if err := eng.Cancel(ctx, parent); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	first, err := eng.Rerun(ctx, parent, "start", RerunIdempotencyKey("rerun-1"))
	if err != nil {
		t.Fatalf("keyed Rerun() error = %v", err)
	}
	if first.Duplicate || first.ExecutionID == "" || first.ExecutionID == parent {
		t.Fatalf("first keyed rerun = %+v, want a new execution", first)
	}
	again, err := eng.Rerun(ctx, parent, "start", RerunIdempotencyKey("rerun-1"))
	if err != nil {
		t.Fatalf("retried keyed Rerun() error = %v", err)
	}
	if !again.Duplicate || again.ExecutionID != first.ExecutionID {
		t.Fatalf("retried rerun = %+v, want a duplicate of %q", again, first.ExecutionID)
	}
}
