package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestEngineWaitRedactsPrivateOutputWithLocalWaiter ensures the local backend's
// fast completion notification cannot bypass the core public-output projection.
func TestEngineWaitRedactsPrivateOutputWithLocalWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()
	if eng.waiter == nil {
		t.Fatal("NewLocal() did not install a completion waiter")
	}

	wf := Workflow("private-output-wait")
	start := wf.Node("start", node.Start())
	private := wf.LocalNode("private", privateOutputWaitHandler{}).PrivateOutput()
	public := wf.LocalNode("public", publicOutputWaitHandler{})
	wf.Connect(start, private)
	wf.Connect(private, public)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	executionID, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	result, err := eng.Wait(ctx, executionID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("Wait() status = %q, want %q (error %q)", result.Status, types.ExecutionStatusSuccess, result.Error)
	}
	if _, ok := result.Output["private"]; ok {
		t.Fatalf("Wait() leaked private node output: %#v", result.Output["private"])
	}
	publicOutput, ok := result.Output["public"].(map[string]any)
	if !ok {
		t.Fatalf("Wait() output[public] = %#v, want public node output", result.Output["public"])
	}
	if got := publicOutput["visible"]; got != true {
		t.Fatalf("Wait() output[public][visible] = %#v, want true", got)
	}
	if got := publicOutput["received_private"]; got != true {
		t.Fatalf("public node received_private = %#v, want true", got)
	}
}

type privateOutputWaitHandler struct{}

func (privateOutputWaitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.private_output_wait.private"}
}

func (privateOutputWaitHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"secret": "do-not-project"}}, nil
}

type publicOutputWaitHandler struct{}

func (publicOutputWaitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.private_output_wait.public"}
}

func (publicOutputWaitHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	receivedPrivate := input != nil && input.Data["secret"] == "do-not-project"
	return &types.Output{Data: map[string]any{
		"visible":          true,
		"received_private": receivedPrivate,
	}}, nil
}
