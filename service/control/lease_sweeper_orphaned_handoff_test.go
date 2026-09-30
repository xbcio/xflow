package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeOrphanedHandoffReclaimer stands in for the Core the control plane wires
// into the sweeper, so the pass can be tested without an engine or a directory.
type fakeOrphanedHandoffReclaimer struct {
	mu        sync.Mutex
	calls     int
	limits    []int
	released  int
	inspected int
	err       error
}

func (f *fakeOrphanedHandoffReclaimer) ReapOrphanedHandoffs(_ context.Context, limit int) (ReapResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.limits = append(f.limits, limit)
	return ReapResult{Inspected: f.inspected, Released: f.released}, f.err
}

func (f *fakeOrphanedHandoffReclaimer) snapshot() (int, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, append([]int(nil), f.limits...)
}

func TestLeaseSweeperReapsOrphanedHandoffsAtBoundedLeaderGatedCadence(t *testing.T) {
	reclaimer := &fakeOrphanedHandoffReclaimer{released: 2}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:                   elector,
		OrphanedHandoffReaper:     reclaimer,
		OrphanedHandoffReapPeriod: time.Minute,
		OrphanedHandoffReapBatch:  11,
	})
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }

	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 2 {
		t.Fatalf("first ReapOrphanedHandoffsOnce() = %d, want 2", got)
	}
	if calls, limits := reclaimer.snapshot(); calls != 1 || limits[0] != 11 {
		t.Fatalf("reap calls=%d limits=%v, want 1/[11]", calls, limits)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 0 {
		t.Fatalf("early ReapOrphanedHandoffsOnce() = %d, want 0", got)
	}
	if calls, _ := reclaimer.snapshot(); calls != 1 {
		t.Fatalf("early reap calls = %d, want 1", calls)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 2 {
		t.Fatalf("scheduled ReapOrphanedHandoffsOnce() = %d, want 2", got)
	}
	if calls, _ := reclaimer.snapshot(); calls != 2 {
		t.Fatalf("scheduled reap calls = %d, want 2", calls)
	}

	elector.leader.Store(false)
	now = now.Add(time.Minute)
	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 0 {
		t.Fatalf("non-leader ReapOrphanedHandoffsOnce() = %d, want 0", got)
	}
	if calls, _ := reclaimer.snapshot(); calls != 2 {
		t.Fatalf("non-leader reap calls = %d, want 2", calls)
	}
}

// TestLeaseSweeperOrphanedHandoffFailureIsContained pins the reason this is
// wired into the sweeper at all: a failing settlement is best-effort
// maintenance and must never turn into a lease-execution failure or stop the
// loop.
func TestLeaseSweeperOrphanedHandoffFailureIsContained(t *testing.T) {
	reclaimer := &fakeOrphanedHandoffReclaimer{err: errors.New("engine unavailable")}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		OrphanedHandoffReaper: reclaimer,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }

	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 0 {
		t.Fatalf("ReapOrphanedHandoffsOnce() = %d, want 0 on error", got)
	}
	if calls, _ := reclaimer.snapshot(); calls != 1 {
		t.Fatalf("reap calls = %d, want 1", calls)
	}
}

// TestLeaseSweeperSkipsOrphanedHandoffWithoutReclaimer keeps a host that wires
// no Core out of this path. The pass has no directory capability to fall back
// on: settling the debt needs the engine, so the only source is the field.
func TestLeaseSweeperSkipsOrphanedHandoffWithoutReclaimer(t *testing.T) {
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: &fakeExpiredLeaseReleaser{},
	})
	if got := sw.ReapOrphanedHandoffsOnce(context.Background()); got != 0 {
		t.Fatalf("ReapOrphanedHandoffsOnce() = %d, want 0 without a reclaimer", got)
	}
}

// TestLeaseSweeperOrphanedHandoffDefaultsAreLoadBearing covers the two fallbacks
// for the production construction, controlplane.go's LeaseSweeperConfig, which
// sets neither. Both zero values fail silently rather than loudly: a zero batch
// reaches the reclaimer as limit<=0, a documented no-op, so a mistyped config
// would disable the settlement entirely while everything else stayed green; a
// zero period makes the rate limiter's `<` comparison unsatisfiable and puts a
// ledger scan on every sweep.
func TestLeaseSweeperOrphanedHandoffDefaultsAreLoadBearing(t *testing.T) {
	reclaimer := &fakeOrphanedHandoffReclaimer{released: 1}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		OrphanedHandoffReaper: reclaimer,
	})

	if sw.orphanedHandoffPeriod != DefaultOrphanedHandoffReapPeriod {
		t.Fatalf("orphaned handoff period = %v, want %v", sw.orphanedHandoffPeriod, DefaultOrphanedHandoffReapPeriod)
	}
	if sw.orphanedHandoffBatch != defaultOrphanedHandoffReapBatch {
		t.Fatalf("orphaned handoff batch = %d, want %d", sw.orphanedHandoffBatch, defaultOrphanedHandoffReapBatch)
	}

	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	sw.ReapOrphanedHandoffsOnce(context.Background())
	sw.ReapOrphanedHandoffsOnce(context.Background())
	calls, limits := reclaimer.snapshot()
	if calls != 1 {
		t.Fatalf("reap calls = %d, want 1: the default period must suppress the second", calls)
	}
	if limits[0] != defaultOrphanedHandoffReapBatch {
		t.Fatalf("reap limit = %d, want %d: a limit of 0 is a silent no-op", limits[0], defaultOrphanedHandoffReapBatch)
	}
}

// TestLeaseSweeperRunDrivesTheOrphanedHandoffReap checks the wiring rather than
// the rate: Run must reach the reclaimer without waiting for an operator, and
// it must be the startup pass and the loop pass that do it.
func TestLeaseSweeperRunDrivesTheOrphanedHandoffReap(t *testing.T) {
	reclaimer := &fakeOrphanedHandoffReclaimer{released: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		OrphanedHandoffReaper: reclaimer,
		Period:                time.Millisecond,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
	// Zero period is overridden by the default, so the loop's own cadence gate
	// would suppress a second pass; the rate is not what this test is about.
	sw.orphanedHandoffPeriod = 0
	sw.sleepFunc = func(context.Context, time.Duration) error {
		calls, _ := reclaimer.snapshot()
		if calls > 1 {
			cancel()
		}
		return ctx.Err()
	}

	sw.Run(ctx)

	if calls, _ := reclaimer.snapshot(); calls < 2 {
		t.Fatalf("Run() reached the orphaned-handoff reclaimer %d times, want the startup pass and at least one loop pass", calls)
	}
}
