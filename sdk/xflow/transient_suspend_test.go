package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestTransientSuspendWaitReportsPublicError(t *testing.T) {
	eng, err := NewLocal(WithExecutionMode(ExecutionModeTransient))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_suspend_wait_error")
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait("approval").OnError(types.OnErrorContinue))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	id, err := eng.Invoke(context.Background(), workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
	if result.Error != ErrTransientSuspendUnsupported.Error() {
		t.Fatalf("result error = %q, want %q", result.Error, ErrTransientSuspendUnsupported.Error())
	}
}
