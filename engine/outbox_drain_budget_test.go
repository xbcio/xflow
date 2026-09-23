package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// slowFlushState gives every flush of the page a real cost, which is what a
// store whose round trip is tens of milliseconds looks like from inside drain.
// LeaseOutbox is where FlushOutbox spends that time on a store with nothing
// ready, so it is the honest place to model it.
type slowFlushState struct {
	*pageRecordingState
	ids      []types.ExecutionID
	perFlush time.Duration
	// flushed is atomic because the concurrent drain under test finishes many
	// executions at once; a plain int here is a race the detector reports in the
	// FAKE, which hides whether the production path is clean.
	flushed atomic.Int64
}

func (s *slowFlushState) ListOutboxExecutions(_ context.Context, limit int) ([]types.ExecutionID, error) {
	out := s.ids
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return append([]types.ExecutionID(nil), out...), nil
}

// LeaseOutbox is where FlushOutbox spends its time on a store with nothing
// ready, so it is where the per-flush cost of a slow link is modelled.
//
// RenewOutbox is implemented only so the store satisfies OutboxLeaser: without
// it the engine's capability probe falls back to ListOutbox, which skips this
// method entirely and makes every test here vacuous. That is exactly what
// happened when these tests were first written.
func (s *slowFlushState) LeaseOutbox(_ context.Context, _ types.ExecutionID, _ time.Time, _ int) ([]OutboxEntry, error) {
	if s.perFlush > 0 {
		time.Sleep(s.perFlush)
	}
	s.flushed.Add(1)
	return nil, nil
}

func (s *slowFlushState) RenewOutbox(_ context.Context, _ types.ExecutionID, _ []OutboxEntry, _ time.Time) ([]OutboxEntry, error) {
	return nil, nil
}

// TestOutboxDispatcherFlushesPageConcurrently is the throughput regression test.
//
// A serial drain delivers 1/(cost of one flush) executions per second, which on
// a store 80ms away was ~3/second against a pipeline producing ~16 — the backlog
// grew without bound, and the sink deliveries it could not reach in time left
// their collection outputs to sit out their full TTL. Concurrency is the only
// knob that moves that number, so the DEFAULT dispatcher must drain a page in
// parallel rather than one execution at a time.
func TestOutboxDispatcherFlushesPageConcurrently(t *testing.T) {
	const (
		page     = 16
		perFlush = 10 * time.Millisecond
	)
	serialCost := page * perFlush // what a one-at-a-time drain must pay

	state := newSlowFlushState(t, page, perFlush)
	eng := New(state, &toggleOutboxQueue{})
	d := NewOutboxDispatcher(eng, time.Second) // no options: the shipped default

	start := time.Now()
	d.drain(context.Background())
	elapsed := time.Since(start)

	if got := state.flushedCount(); got != page {
		t.Fatalf("flushed %d of %d, want the whole page", got, page)
	}
	// Two-thirds is a wide margin over the ideal page/concurrency, so this fails
	// only if the drain really is serial.
	if elapsed > serialCost*2/3 {
		t.Fatalf("draining %d executions costing %v each took %v, want well under the %v "+
			"a serial drain must pay: the dispatcher is not flushing the page concurrently, "+
			"so its throughput is capped by the store's round trip and the backlog outruns it",
			page, perFlush, elapsed, serialCost)
	}
}

// TestOutboxDispatcherFlushConcurrencyOneIsSerial is the control: it proves the
// speed-up above came from concurrency and not from the fake getting faster.
func TestOutboxDispatcherFlushConcurrencyOneIsSerial(t *testing.T) {
	const (
		page     = 16
		perFlush = 10 * time.Millisecond
	)
	state := newSlowFlushState(t, page, perFlush)
	eng := New(state, &toggleOutboxQueue{})
	// A budget far above the page's cost, so the budget is not what ends it.
	d := NewOutboxDispatcher(eng, time.Second,
		WithOutboxFlushConcurrency(1),
		WithOutboxDrainBudget(time.Minute),
	)

	start := time.Now()
	d.drain(context.Background())
	elapsed := time.Since(start)

	serialCost := page * perFlush
	if got := state.flushedCount(); got != page {
		t.Fatalf("flushed %d of %d, want the whole page", got, page)
	}
	if elapsed < serialCost/2 {
		t.Fatalf("a serial drain of %d executions costing %v each finished in %v, which is "+
			"faster than the work it claims to have done: the control is not measuring "+
			"what it thinks it is", page, perFlush, elapsed)
	}
}

// TestOutboxDispatcherStopsAtItsBudget pins that one drain cannot run past its
// budget.
//
// The failure this guards is not slowness, it is unboundedness: a drain costs
// (page size / concurrency) x (cost of one flush), and the second factor belongs
// to the store link. Before the budget existed, one pass over a 2048-entry page
// against a store ~80ms away took ~635 seconds, during which outbox metrics were
// frozen and shutdown would have waited it out.
func TestOutboxDispatcherStopsAtItsBudget(t *testing.T) {
	const (
		page     = 400
		perFlush = 5 * time.Millisecond
		budget   = 20 * time.Millisecond
	)
	state := newSlowFlushState(t, page, perFlush)
	eng := New(state, &toggleOutboxQueue{})
	// Two workers, so the page cannot finish inside the budget.
	d := NewOutboxDispatcher(eng, time.Second,
		WithOutboxFlushConcurrency(2),
		WithOutboxDrainBudget(budget),
	)

	start := time.Now()
	d.drain(context.Background())
	elapsed := time.Since(start)

	if state.flushedCount() >= page {
		t.Fatalf("drain flushed the whole page of %d despite a %v budget: the budget did "+
			"not bound the pass", page, budget)
	}
	if state.flushedCount() == 0 {
		t.Fatalf("drain flushed nothing with a %v budget: the budget must let some work "+
			"through, or one pathological pass would starve every later one", budget)
	}
	// A flush already in flight when the budget comes due is allowed to finish.
	if limit := budget + perFlush + 50*time.Millisecond; elapsed > limit {
		t.Fatalf("drain took %v with a %v budget, want at most %v", elapsed, budget, limit)
	}
}

// TestOutboxDispatcherDrainsWholePageWithoutABudget is the other side of the
// coin: with the cap removed the pass must still walk the entire page, so the
// budget cannot have become an accidental work limit.
func TestOutboxDispatcherDrainsWholePageWithoutABudget(t *testing.T) {
	const page = 12
	state := newSlowFlushState(t, page, 0)
	observer := &drainObserver{}
	eng := New(state, &toggleOutboxQueue{}, WithOutboxObserver(observer))

	NewOutboxDispatcher(eng, time.Second, WithOutboxDrainBudget(0)).drain(context.Background())

	if got := state.flushedCount(); got != page {
		t.Fatalf("flushed %d of %d with no budget, want the whole page", got, page)
	}
	drains := observer.drainObservations()
	if len(drains) != 1 {
		t.Fatalf("drain observations = %d, want 1", len(drains))
	}
	if drains[0].discovered != page {
		t.Fatalf("discovered = %d, want the whole page of %d", drains[0].discovered, page)
	}
}

// TestOutboxDispatcherDefaultsAreBounded guards the shipped defaults: producing a
// dispatcher without options must still leave both the cap and the concurrency in
// place, because the pathological pass and the throughput cap are both reachable
// without anyone opting into anything.
func TestOutboxDispatcherDefaultsAreBounded(t *testing.T) {
	d := NewOutboxDispatcher(New(newFakeState(), &toggleOutboxQueue{}), time.Second)
	if d.budget != DefaultOutboxDrainBudget {
		t.Fatalf("default budget = %v, want %v", d.budget, DefaultOutboxDrainBudget)
	}
	if d.flushConcurrency != DefaultOutboxFlushConcurrency {
		t.Fatalf("default flush concurrency = %d, want %d", d.flushConcurrency, DefaultOutboxFlushConcurrency)
	}
	if d.flushConcurrency <= 1 {
		t.Fatalf("default flush concurrency = %d, want a concurrent default: a serial drain "+
			"is capped by the store's round trip", d.flushConcurrency)
	}
}

func (s *slowFlushState) flushedCount() int { return int(s.flushed.Load()) }

func newSlowFlushState(t *testing.T, page int, perFlush time.Duration) *slowFlushState {
	t.Helper()
	ids := make([]types.ExecutionID, page)
	for i := range ids {
		ids[i] = types.ExecutionID("exec-" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + string(rune('a'+i/676)))
	}
	return &slowFlushState{
		pageRecordingState: newPageRecordingState(),
		ids:                ids,
		perFlush:           perFlush,
	}
}
