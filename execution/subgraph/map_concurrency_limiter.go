package subgraph

import (
	"context"
	"sync"
)

// MapConcurrencyLimiter keeps the two independent sources of map pressure
// bounded at runner scope:
//
//   - active batches bound how many batches may compete and retain decoded
//     state while waiting to finish;
//   - active items bound how many nested item engines may execute at once.
//
// Keeping these as separate permits is intentional. Reserving a batch's whole
// item width until completion improves commit locality, but leaves CPUs idle
// while that batch is between items. Per-item admission alone keeps CPUs busy,
// but lets an unbounded number of batches make partial progress and delays every
// ordered Kafka commit. The batch gate bounds that diffusion while item permits
// allow admitted batches to overlap.
//
// Both permit queues are FIFO. A canceled waiter is removed without leaking a
// permit, including when cancellation races with a grant.
type MapConcurrencyLimiter struct {
	batches *permitLimiter
	items   *permitLimiter
}

// NewMapConcurrencyLimiter creates a runner-scoped map limiter. Both capacities
// must be positive. They are deliberately independent resource budgets rather
// than aliases for Kafka's emit/reorder in-flight limit.
func NewMapConcurrencyLimiter(activeBatches, activeItems int) *MapConcurrencyLimiter {
	if activeBatches <= 0 {
		panic("subgraph: active map batch capacity must be positive")
	}
	if activeItems <= 0 {
		panic("subgraph: active map item capacity must be positive")
	}
	return &MapConcurrencyLimiter{
		batches: newPermitLimiter(activeBatches),
		items:   newPermitLimiter(activeItems),
	}
}

func (l *MapConcurrencyLimiter) acquireBatch(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	return l.batches.acquire(ctx)
}

func (l *MapConcurrencyLimiter) acquireItem(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	return l.items.acquire(ctx)
}

func (l *MapConcurrencyLimiter) effectiveItemWidth(requested int) int {
	if requested < 1 {
		requested = 1
	}
	if l == nil || requested <= l.items.capacity {
		return requested
	}
	return l.items.capacity
}

type permitLimiter struct {
	mu        sync.Mutex
	capacity  int
	available int
	waiters   []*permitWaiter
}

type permitWaiter struct {
	ready   chan struct{}
	granted bool
}

func newPermitLimiter(capacity int) *permitLimiter {
	return &permitLimiter{capacity: capacity, available: capacity}
}

func (l *permitLimiter) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.Lock()
	if len(l.waiters) == 0 && l.available > 0 {
		l.available--
		l.mu.Unlock()
		return l.releaseFunc(), nil
	}
	waiter := &permitWaiter{ready: make(chan struct{})}
	l.waiters = append(l.waiters, waiter)
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return l.releaseFunc(), nil
	case <-ctx.Done():
		l.mu.Lock()
		if waiter.granted {
			// grantWaitersLocked won the race with cancellation. The caller will
			// not receive a release function, so return the permit here.
			l.available++
		} else {
			l.removeWaiterLocked(waiter)
		}
		l.grantWaitersLocked()
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (l *permitLimiter) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.available++
			l.grantWaitersLocked()
			l.mu.Unlock()
		})
	}
}

func (l *permitLimiter) grantWaitersLocked() {
	for l.available > 0 && len(l.waiters) > 0 {
		waiter := l.waiters[0]
		l.waiters = l.waiters[1:]
		l.available--
		waiter.granted = true
		close(waiter.ready)
	}
}

func (l *permitLimiter) removeWaiterLocked(target *permitWaiter) {
	for i, waiter := range l.waiters {
		if waiter != target {
			continue
		}
		copy(l.waiters[i:], l.waiters[i+1:])
		l.waiters[len(l.waiters)-1] = nil
		l.waiters = l.waiters[:len(l.waiters)-1]
		return
	}
}
