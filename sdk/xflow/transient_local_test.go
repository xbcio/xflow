package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestNewLocalTransientFailsSuspendWorkflow(t *testing.T) {
	eng, err := NewLocal(WithExecutionMode(ExecutionModeTransient))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_local_suspend")
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait("approval"))
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
}

func TestNewLocalTransientExpiresCompletedExecutionState(t *testing.T) {
	action := node.Define("test.transient.local.action", func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{"seen": input.Data["value"]}}, nil
	})
	eng, err := NewLocal(
		WithExecutionMode(ExecutionModeTransient),
		WithTransientCompletionTTL(25*time.Millisecond),
		WithNodes(action),
	)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_local")
	start := wf.Node("start", node.Start())
	run := wf.Node("run", action.New(nil))
	wf.Connect(start, run)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"value": "ok"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", result.Status)
	}
	if got := result.Output["run"].(map[string]any)["seen"]; got != "ok" {
		t.Fatalf("run.seen = %v, want ok", got)
	}

	waitForTransientCondition(t, time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		return snap == nil
	})
}

func waitForTransientCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before timeout")
		}
	}
}
