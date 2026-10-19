package xflow

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
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
	// The two-node workflow can finish before this cancel lands, and the rerun
	// only needs the parent to be terminal. Accept "inactive" after confirming
	// that is what happened: tolerating it blindly would also hide a Cancel
	// broken for an unrelated reason, and the rerun below would still fail on a
	// running parent.
	if err := eng.Cancel(ctx, parent); err != nil {
		if !errors.Is(err, engine.ErrExecutionInactive) {
			t.Fatalf("Cancel() error = %v", err)
		}
		res, waitErr := eng.Wait(ctx, parent)
		if waitErr != nil {
			t.Fatalf("Wait() after inactive Cancel error = %v", waitErr)
		}
		if !types.IsTerminalExecutionStatus(res.Status) {
			t.Fatalf("parent status = %s after inactive Cancel, want a terminal status", res.Status)
		}
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
