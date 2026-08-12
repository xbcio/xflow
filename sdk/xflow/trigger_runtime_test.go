package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

func TestAddWorkflowActivatesTriggerHandler(t *testing.T) {
	activated := make(chan struct{}, 1)
	tr := trigger.Define("test.trigger", func(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		activated <- struct{}{}
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	eng, err := NewLocal(WithNodes(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("wf")
	wf.Node("trigger", tr.New(nil))
	_, err = eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-activated:
	case <-time.After(time.Second):
		t.Fatal("trigger was not activated")
	}
}
