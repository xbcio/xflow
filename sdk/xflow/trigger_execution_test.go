package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

func TestTriggerEmitRunsDownstreamNodes(t *testing.T) {
	var capturedRuntime types.TriggerRuntime
	tr := trigger.Define("test.trigger.e2e", func(_ context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		capturedRuntime = in.Runtime
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	seen := make(chan any, 1)
	action := node.Define("test.capture.trigger", func(_ context.Context, input *types.Input) (*types.Output, error) {
		seen <- input.Data["trigger"]
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})

	eng, err := NewLocal(WithNodes(tr, action))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("trigger_e2e").Version("v3")
	start := wf.Node("trigger", tr.New(nil))
	capture := wf.Node("capture", action.New(nil))
	wf.Connect(start, capture)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatal(err)
	}
	if capturedRuntime == nil {
		t.Fatal("trigger runtime was not captured")
	}

	event := &types.TriggerEvent{ID: "evt-1", Kind: "test"}
	execID, err := capturedRuntime.Emit(context.Background(), workflowID, "trigger", event)
	if err != nil {
		t.Fatal(err)
	}
	// A trigger execution is an SDK Invoke on the addressed workflow, so its
	// audit snapshot must name that workflow and the version it ran — the
	// identity the editor's run history resolves definitions by.
	detail, err := eng.Inspect(context.Background(), execID)
	if err != nil {
		t.Fatalf("Inspect(trigger execution): %v", err)
	}
	if detail.WorkflowID != workflowID {
		t.Fatalf("trigger execution WorkflowID = %q, want %q", detail.WorkflowID, workflowID)
	}
	if detail.WorkflowVersion != "v3" {
		t.Fatalf("trigger execution WorkflowVersion = %q, want %q", detail.WorkflowVersion, "v3")
	}

	select {
	case got := <-seen:
		if got != event {
			t.Fatalf("downstream trigger = %#v, want original event", got)
		}
	case <-time.After(time.Second):
		t.Fatal("downstream node did not receive trigger event")
	}
}
