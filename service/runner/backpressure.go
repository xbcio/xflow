package runner

import (
	"context"
	"sync"
	"sync/atomic"
)

// EmitBackpressure tracks in-flight (unconfirmed) emit operations and provides
// a semaphore-like interface to pause new emits when the window is full.
// This prevents unbounded memory growth when the control plane is slow to
// acknowledge group emit batches.
type EmitBackpressure struct {
	maxInflight int
	inflight    atomic.Int64
	paused      atomic.Int64 // counter of times we hit the limit
	mu          sync.Mutex
	waiters     []chan struct{} // blocked callers waiting for capacity
}

// NewEmitBackpressure creates a backpressure controller with the given
// maximum number of in-flight (unconfirmed) emit operations.
func NewEmitBackpressure(maxInflight int) *EmitBackpressure {
	if maxInflight <= 0 {
		maxInflight = 1
	}
	return &EmitBackpressure{
		maxInflight: maxInflight,
	}
}

// Acquire blocks until a slot is available in the in-flight window.
// Returns a release function that MUST be called when the emit is confirmed.
// Returns ctx.Err() if the context is canceled while waiting.
func (bp *EmitBackpressure) Acquire(ctx context.Context) (release func(), err error) {
	// Fast path: try to claim a slot without blocking.
	for {
		current := bp.inflight.Load()
		if current < int64(bp.maxInflight) {
			if bp.inflight.CompareAndSwap(current, current+1) {
				return bp.releaseFunc(), nil
			}
			// CAS failed — another goroutine raced us; retry.
			continue
		}
		break
	}

	// Slow path: window is full — register as a waiter.
	bp.paused.Add(1)

	ch := make(chan struct{}, 1)
	bp.mu.Lock()
	bp.waiters = append(bp.waiters, ch)
	bp.mu.Unlock()

	select {
	case <-ctx.Done():
		// Remove ourselves from waiters to avoid leaking the channel.
		bp.mu.Lock()
		for i, w := range bp.waiters {
			if w == ch {
				bp.waiters = append(bp.waiters[:i], bp.waiters[i+1:]...)
				break
			}
		}
		bp.mu.Unlock()
		return nil, ctx.Err()
	case <-ch:
		// A release woke us and transferred ownership of a slot.
		return bp.releaseFunc(), nil
	}
}

// Inflight returns the current number of in-flight operations.
func (bp *EmitBackpressure) Inflight() int64 {
	return bp.inflight.Load()
}

// PauseCount returns the total number of times callers had to wait.
func (bp *EmitBackpressure) PauseCount() int64 {
	return bp.paused.Load()
}

// releaseFunc returns a one-shot release function that decrements the in-flight
// counter and wakes one waiter if any are blocked.
func (bp *EmitBackpressure) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			bp.mu.Lock()
			if len(bp.waiters) > 0 {
				// Transfer our slot directly to the next waiter rather than
				// decrementing and having the waiter re-increment — avoids a
				// race where another fast-path caller steals the slot.
				w := bp.waiters[0]
				bp.waiters = bp.waiters[1:]
				bp.mu.Unlock()
				w <- struct{}{}
				return
			}
			bp.mu.Unlock()
			bp.inflight.Add(-1)
		})
	}
}
