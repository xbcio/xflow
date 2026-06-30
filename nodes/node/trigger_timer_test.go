package node

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestTimerTriggerEmitsAtInterval(t *testing.T) {
	rt := newFakeTriggerRuntime()
	tr := TimerTrigger().Every(10 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(context.Background())
	if !rt.waitEmit(time.Second) {
		t.Fatal("timer did not emit")
	}
}

type fakeTriggerRuntime struct {
	emits chan *types.TriggerEvent
}

func newFakeTriggerRuntime() *fakeTriggerRuntime {
	return &fakeTriggerRuntime{emits: make(chan *types.TriggerEvent, 10)}
}

func (r *fakeTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.emits <- event
	return "exec-1", nil
}

func (r *fakeTriggerRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (r *fakeTriggerRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return fakeTriggerLock{}, true, nil
}

func (r *fakeTriggerRuntime) State(context.Context, string) types.TriggerState { return nil }

func (r *fakeTriggerRuntime) waitEmit(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.emits:
		return true
	case <-timer.C:
		return false
	}
}

type fakeTriggerLock struct{}

func (fakeTriggerLock) Release(context.Context) error { return nil }
