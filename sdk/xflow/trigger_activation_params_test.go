package xflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// The in-process activation path reads a trigger's parameters straight off the
// raw WorkflowDef, never off the compiled Graph, so it used to be the one path
// where a template reached handler.Activate verbatim no matter what. That makes
// the two deployment modes disagree about what the same workflow means: the
// distributed path subscribes to the rendered topic, the in-process path to the
// literal template text.

// TestInlineActivationRejectsARootThatDoesNotExistYet pins the fail-closed half.
// A trigger parameter referencing $input cannot be rendered at activation time
// -- no execution exists -- and shipping the template text to the handler is a
// silent wrong answer, so activation must fail instead.
func TestInlineActivationRejectsARootThatDoesNotExistYet(t *testing.T) {
	activated := make(chan struct{}, 1)
	tr := trigger.Define("test.trigger.activation.roots", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		activated <- struct{}{}
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	eng, err := NewLocal(WithNodes(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("wf-activation-roots")
	wf.Node("trigger", tr.New(map[string]any{"topic": "${{ $input.topic }}"}))

	_, err = eng.AddWorkflow(context.Background(), wf)
	if err == nil {
		t.Fatal("AddWorkflow() = nil error, want the activation to fail -- " +
			"otherwise the handler subscribes to the literal template text")
	}
	if !strings.Contains(err.Error(), "$input") {
		t.Errorf("error %q does not name the offending root", err)
	}
	select {
	case <-activated:
		t.Error("handler was activated despite the unrenderable parameter")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestInlineActivationPassesLiteralsThrough is the positive control: without it
// a layer that rejected everything would satisfy the test above.
func TestInlineActivationPassesLiteralsThrough(t *testing.T) {
	got := make(chan map[string]any, 1)
	tr := trigger.Define("test.trigger.activation.literal", func(_ context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		got <- in.Params
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	eng, err := NewLocal(WithNodes(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("wf-activation-literal")
	wf.Node("trigger", tr.New(map[string]any{"topic": "sas-traffic", "partitions": 4}))
	if _, err := eng.AddWorkflow(context.Background(), wf); err != nil {
		t.Fatal(err)
	}

	select {
	case params := <-got:
		if params["topic"] != "sas-traffic" || params["partitions"] != 4 {
			t.Errorf("params = %#v, want the literals unchanged", params)
		}
	case <-time.After(time.Second):
		t.Fatal("trigger was not activated")
	}
}
