package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestNewLocalTransientRunsSimpleWorkflow(t *testing.T) {
	action := node.Define("test.transient.local.action", func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{"seen": input.Data["value"]}}, nil
	})
	eng, err := NewLocal(WithExecutionMode(ExecutionModeTransient), WithNodes(action))
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
}
