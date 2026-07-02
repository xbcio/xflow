package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func TestTimerTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	rt := newFakeTriggerRuntime()
	rt.dedupFunc = func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	}
	tr := TimerTrigger().Every(5 * time.Millisecond)
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

	if !rt.waitDedup(time.Second) {
		t.Fatal("timer did not attempt dedup")
	}
	if got := rt.emitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestTimerTriggerContinuesAfterEmitError(t *testing.T) {
	rt := newFakeTriggerRuntime()
	var calls atomic.Int64
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	}
	tr := TimerTrigger().Every(5 * time.Millisecond)
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

	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.emitCount())
	}
}

func TestTimerTriggerEventUsesDeterministicIntervalBucket(t *testing.T) {
	interval := 10 * time.Second
	firstTick := time.Unix(1_700_000_000, 100_000_000).UTC()
	secondTick := firstTick.Add(700 * time.Millisecond)
	thirdTick := firstTick.Add(interval)

	firstEvent := newTimerTriggerEvent("wf-1", "timer", interval, firstTick)
	secondEvent := newTimerTriggerEvent("wf-1", "timer", interval, secondTick)
	thirdEvent := newTimerTriggerEvent("wf-1", "timer", interval, thirdTick)

	if firstEvent.ID != secondEvent.ID {
		t.Fatalf("same bucket IDs = %q/%q, want equal", firstEvent.ID, secondEvent.ID)
	}
	if firstEvent.ID == thirdEvent.ID {
		t.Fatalf("different bucket IDs = %q/%q, want different", firstEvent.ID, thirdEvent.ID)
	}

	wantScheduled := firstTick.Truncate(interval).Format(time.RFC3339Nano)
	if got := firstEvent.Data["scheduled_time"]; got != wantScheduled {
		t.Fatalf("scheduled_time = %#v, want %q", got, wantScheduled)
	}
	if !firstEvent.Time.Equal(firstTick) {
		t.Fatalf("first event time = %s, want %s", firstEvent.Time, firstTick)
	}
	if !secondEvent.Time.Equal(secondTick) {
		t.Fatalf("second event time = %s, want %s", secondEvent.Time, secondTick)
	}
}

type fakeTriggerRuntime struct {
	mu          sync.Mutex
	callbackMu  sync.Mutex
	emits       []*types.TriggerEvent
	emitSignal  chan struct{}
	dedupSignal chan struct{}
	emitFunc    func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error)
	dedupFunc   func(context.Context, string, time.Duration) (bool, error)
}

func newFakeTriggerRuntime() *fakeTriggerRuntime {
	return &fakeTriggerRuntime{
		emitSignal:  make(chan struct{}, 32),
		dedupSignal: make(chan struct{}, 32),
	}
}

func (r *fakeTriggerRuntime) Emit(ctx context.Context, workflowID types.WorkflowID, nodeName string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.recordEmit(event)
	if r.emitFunc != nil {
		r.callbackMu.Lock()
		defer r.callbackMu.Unlock()
		return r.emitFunc(ctx, workflowID, nodeName, event)
	}
	return "exec-1", nil
}

func (r *fakeTriggerRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	select {
	case r.dedupSignal <- struct{}{}:
	default:
	}
	if r.dedupFunc != nil {
		r.callbackMu.Lock()
		defer r.callbackMu.Unlock()
		return r.dedupFunc(ctx, key, ttl)
	}
	return true, nil
}

func (r *fakeTriggerRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return fakeTriggerLock{}, true, nil
}

func (r *fakeTriggerRuntime) State(context.Context, string) types.TriggerState { return nil }

func (r *fakeTriggerRuntime) waitEmit(timeout time.Duration) bool {
	return r.waitForEmitCount(1, timeout)
}

func (r *fakeTriggerRuntime) waitDedup(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.dedupSignal:
		return true
	case <-timer.C:
		return false
	}
}

func (r *fakeTriggerRuntime) waitForEmitCount(want int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if r.emitCount() >= want {
			return true
		}
		select {
		case <-r.emitSignal:
		case <-deadline.C:
			return r.emitCount() >= want
		}
	}
}

func (r *fakeTriggerRuntime) emitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.emits)
}

func (r *fakeTriggerRuntime) recordEmit(event *types.TriggerEvent) {
	r.mu.Lock()
	r.emits = append(r.emits, event)
	r.mu.Unlock()
	select {
	case r.emitSignal <- struct{}{}:
	default:
	}
}

type fakeTriggerLock struct{}

func (fakeTriggerLock) Release(context.Context) error { return nil }
