package xflow

import (
	"context"
	"errors"
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

// TestTriggerRuntimeForwardsExecutionStatus: a trigger reaches the execution
// store through the runtime it was activated with, and the runtime's job is to
// pass the question down to its primitives, which is where the local and
// distributed providers differ.
func TestTriggerRuntimeForwardsExecutionStatus(t *testing.T) {
	ctx := context.Background()
	primitives := &statusPrimitives{status: types.ExecutionStatusRunning, found: true}
	rt := newTriggerRuntime(nil, primitives)

	status, found, err := rt.GetExecutionStatus(ctx, "exec-1")
	if err != nil || !found || status != types.ExecutionStatusRunning {
		t.Fatalf("GetExecutionStatus() = %q, %v, %v; want the primitives' answer %q",
			status, found, err, types.ExecutionStatusRunning)
	}
	if primitives.asked != "exec-1" {
		t.Fatalf("the primitives were asked about %q, want exec-1", primitives.asked)
	}
}

// TestTriggerRuntimeWithoutTheCapabilityDegrades: backend.TriggerPrimitives
// does not require the execution status reader, so a provider that has none is a
// normal case. The runtime must report the capability as unavailable — not a
// status, and not not-found — because that is the signal the trigger degrades
// on, and it is the runtime's only chance to give it.
func TestTriggerRuntimeWithoutTheCapabilityDegrades(t *testing.T) {
	rt := newTriggerRuntime(nil, &plainPrimitives{})

	status, found, err := rt.GetExecutionStatus(context.Background(), "exec-1")
	if !errors.Is(err, types.ErrExecutionStatusUnavailable) {
		t.Fatalf("GetExecutionStatus() = %q, %v, %v; want types.ErrExecutionStatusUnavailable", status, found, err)
	}
}

// statusPrimitives is a primitives implementation that has the optional
// execution status capability; plainPrimitives is one that does not. A provider
// may be either, which is exactly the difference the runtime must handle.
type statusPrimitives struct {
	plainPrimitives
	status types.ExecutionStatus
	found  bool
	asked  types.ExecutionID
}

func (p *statusPrimitives) GetExecutionStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	p.asked = id
	return p.status, p.found, nil
}

type plainPrimitives struct{}

func (p *plainPrimitives) Dedup(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (p *plainPrimitives) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return nil, true, nil
}

func (p *plainPrimitives) State(context.Context, string) types.TriggerState { return nil }
