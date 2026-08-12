// Package triggertest holds fakes for the trigger-facing interfaces in types
// (TriggerRuntime, TriggerLock) that every trigger subpackage's tests need. It
// is a normal package (not an external _test package) so node/trigger/timer,
// cron, webhook, redishub and kafka can all import the same fake instead of
// each carrying a drifting copy. Same pattern as store/storetest and
// backend/internal/statestoretest.
package triggertest

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/types"
)

// FakeRuntime is a types.TriggerRuntime that records emitted events and lets a
// test install Emit/Dedup callbacks. Safe for concurrent use: a trigger's
// goroutine delivers while the test goroutine inspects.
type FakeRuntime struct {
	mu          sync.Mutex
	callbackMu  sync.Mutex
	emits       []*types.TriggerEvent
	emitSignal  chan struct{}
	dedupSignal chan struct{}
	emitFunc    func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error)
	dedupFunc   func(context.Context, string, time.Duration) (bool, error)
}

func NewFakeRuntime() *FakeRuntime {
	return &FakeRuntime{
		emitSignal:  make(chan struct{}, 32),
		dedupSignal: make(chan struct{}, 32),
	}
}

var _ types.TriggerRuntime = (*FakeRuntime)(nil)

func (r *FakeRuntime) Emit(ctx context.Context, workflowID types.WorkflowID, nodeName string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.RecordEmit(event)
	// callbackMu is taken before READING emitFunc, not just before calling it.
	// The earlier form checked `r.emitFunc != nil` outside the lock, which races
	// with SetEmitFunc installing one from the test goroutine.
	r.callbackMu.Lock()
	fn := r.emitFunc
	r.callbackMu.Unlock()
	if fn != nil {
		return fn(ctx, workflowID, nodeName, event)
	}
	return "exec-1", nil
}

func (r *FakeRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	select {
	case r.dedupSignal <- struct{}{}:
	default:
	}
	r.callbackMu.Lock()
	fn := r.dedupFunc
	r.callbackMu.Unlock()
	if fn != nil {
		return fn(ctx, key, ttl)
	}
	return true, nil
}

func (r *FakeRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return FakeLock{}, true, nil
}

func (r *FakeRuntime) State(context.Context, string) types.TriggerState { return nil }

func (r *FakeRuntime) WaitEmit(timeout time.Duration) bool {
	return r.WaitForEmitCount(1, timeout)
}

func (r *FakeRuntime) WaitDedup(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.dedupSignal:
		return true
	case <-timer.C:
		return false
	}
}

func (r *FakeRuntime) WaitForEmitCount(want int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if r.EmitCount() >= want {
			return true
		}
		select {
		case <-r.emitSignal:
		case <-deadline.C:
			return r.EmitCount() >= want
		}
	}
}

func (r *FakeRuntime) EmitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.emits)
}

// Events returns a copy of the emitted events. The copy matters: callers inspect
// event payloads while the trigger goroutine may still be appending.
func (r *FakeRuntime) Events() []*types.TriggerEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*types.TriggerEvent(nil), r.emits...)
}

// SetEmitFunc installs an emit callback. It takes callbackMu, which Emit also
// holds while reading emitFunc, so installing one concurrently with a delivery
// is not a data race.
func (r *FakeRuntime) SetEmitFunc(fn func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error)) {
	r.callbackMu.Lock()
	defer r.callbackMu.Unlock()
	r.emitFunc = fn
}

// SetDedupFunc installs a dedup callback. Same locking rationale as SetEmitFunc.
func (r *FakeRuntime) SetDedupFunc(fn func(context.Context, string, time.Duration) (bool, error)) {
	r.callbackMu.Lock()
	defer r.callbackMu.Unlock()
	r.dedupFunc = fn
}

// RecordEmit appends event to the recorded list and signals a waiter. Exported
// because a test's own runtime type may embed FakeRuntime and override Emit
// while still wanting the recording behaviour.
func (r *FakeRuntime) RecordEmit(event *types.TriggerEvent) {
	r.mu.Lock()
	r.emits = append(r.emits, event)
	r.mu.Unlock()
	select {
	case r.emitSignal <- struct{}{}:
	default:
	}
}

// FakeLock is a types.TriggerLock whose Release always succeeds.
type FakeLock struct{}

func (FakeLock) Release(context.Context) error { return nil }

var _ types.TriggerLock = FakeLock{}
