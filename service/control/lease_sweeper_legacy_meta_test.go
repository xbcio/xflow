package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeLegacyReaperDirectory satisfies the two directory capabilities the
// sweeper discovers by type assertion, so it can stand in for a
// RedisRunnerDirectory without a Redis.
type fakeLegacyReaperDirectory struct {
	mu        sync.Mutex
	calls     int
	limits    []int
	reaped    int
	inspected int
	err       error
	reapFunc  func() int
}

func (f *fakeLegacyReaperDirectory) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

func (f *fakeLegacyReaperDirectory) ReapOrphanedLegacyAssignmentLeaseMeta(_ context.Context, limit int) (ReapResult, error) {
	f.mu.Lock()
	f.calls++
	f.limits = append(f.limits, limit)
	reaped, err := f.reaped, f.err
	inspected := f.inspected
	fn := f.reapFunc
	f.mu.Unlock()
	if err != nil {
		return ReapResult{}, err
	}
	if fn != nil {
		return ReapResult{Inspected: inspected, Released: fn()}, nil
	}
	return ReapResult{Inspected: inspected, Released: reaped}, nil
}

func (f *fakeLegacyReaperDirectory) snapshot() (int, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, append([]int(nil), f.limits...)
}

func TestLeaseSweeperReapsLegacyLeaseMetaAtBoundedLeaderGatedCadence(t *testing.T) {
	directory := &fakeLegacyReaperDirectory{reaped: 3}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:                   elector,
		RunnerDirectory:           directory,
		LegacyLeaseMetaReapPeriod: time.Minute,
		LegacyLeaseMetaReapBatch:  17,
	})
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }

	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 3 {
		t.Fatalf("first ReapLegacyLeaseMetaOnce() = %d, want 3", got)
	}
	if calls, limits := directory.snapshot(); calls != 1 || limits[0] != 17 {
		t.Fatalf("reap calls=%d limits=%v, want 1/[17]", calls, limits)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 0 {
		t.Fatalf("early ReapLegacyLeaseMetaOnce() = %d, want 0", got)
	}
	if calls, _ := directory.snapshot(); calls != 1 {
		t.Fatalf("early reap calls = %d, want 1", calls)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 3 {
		t.Fatalf("scheduled ReapLegacyLeaseMetaOnce() = %d, want 3", got)
	}
	if calls, _ := directory.snapshot(); calls != 2 {
		t.Fatalf("scheduled reap calls = %d, want 2", calls)
	}

	elector.leader.Store(false)
	now = now.Add(time.Minute)
	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 0 {
		t.Fatalf("non-leader ReapLegacyLeaseMetaOnce() = %d, want 0", got)
	}
	if calls, _ := directory.snapshot(); calls != 2 {
		t.Fatalf("non-leader reap calls = %d, want 2", calls)
	}
}

// TestLeaseSweeperLegacyLeaseMetaReapFailureIsContained pins the reason this is
// wired into the sweeper at all: a failing drain is best-effort maintenance and
// must never turn into a lease-execution failure or stop the loop.
func TestLeaseSweeperLegacyLeaseMetaReapFailureIsContained(t *testing.T) {
	directory := &fakeLegacyReaperDirectory{err: errors.New("redis unavailable")}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC) }

	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 0 {
		t.Fatalf("ReapLegacyLeaseMetaOnce() = %d, want 0 on error", got)
	}
	if calls, _ := directory.snapshot(); calls != 1 {
		t.Fatalf("reap calls = %d, want 1", calls)
	}
}

// TestLeaseSweeperSkipsLegacyLeaseMetaReapWithoutCapability keeps the in-memory
// directory and every other RunnerDirectory implementation out of this path.
func TestLeaseSweeperSkipsLegacyLeaseMetaReapWithoutCapability(t *testing.T) {
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: &fakeExpiredLeaseReleaser{},
	})
	if got := sw.ReapLegacyLeaseMetaOnce(context.Background()); got != 0 {
		t.Fatalf("ReapLegacyLeaseMetaOnce() = %d, want 0 without the capability", got)
	}
}

type fakeExpiredLeaseReleaser struct{}

func (f *fakeExpiredLeaseReleaser) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

// TestLeaseSweeperLegacyLeaseMetaReapDefaultsAreLoadBearing covers the two
// fallbacks for the only production construction, controlplane.go's
// LeaseSweeperConfig{Elector, Logger, RunnerDirectory}, which sets neither. Both
// zero values fail silently rather than loudly: a zero batch reaches the
// reaper as limit<=0, which is a documented no-op, so a mistyped config would
// disable the drain entirely while everything else stayed green; a zero period
// makes the rate limiter's `<` comparison unsatisfiable and puts a reaper call
// on every sweep.
func TestLeaseSweeperLegacyLeaseMetaReapDefaultsAreLoadBearing(t *testing.T) {
	directory := &fakeLegacyReaperDirectory{reaped: 1}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
	})

	if sw.reapPeriod != DefaultLegacyLeaseMetaReapPeriod {
		t.Fatalf("reap period = %v, want %v", sw.reapPeriod, DefaultLegacyLeaseMetaReapPeriod)
	}
	if sw.reapBatch != defaultLegacyLeaseMetaReapBatch {
		t.Fatalf("reap batch = %d, want %d", sw.reapBatch, defaultLegacyLeaseMetaReapBatch)
	}

	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	sw.ReapLegacyLeaseMetaOnce(context.Background())
	sw.ReapLegacyLeaseMetaOnce(context.Background())
	calls, limits := directory.snapshot()
	if calls != 1 {
		t.Fatalf("reap calls = %d, want 1: the default period must suppress the second", calls)
	}
	if limits[0] != defaultLegacyLeaseMetaReapBatch {
		t.Fatalf("reap limit = %d, want %d: a limit of 0 is a silent no-op", limits[0], defaultLegacyLeaseMetaReapBatch)
	}
}

// TestLeaseSweeperRunDrivesTheLegacyReap checks the wiring rather than the
// rate: Run must reach the reaper without waiting for an operator.
func TestLeaseSweeperRunDrivesTheLegacyReap(t *testing.T) {
	directory := &fakeLegacyReaperDirectory{reaped: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
		Period:          time.Millisecond,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC) }
	sw.sleepFunc = func(context.Context, time.Duration) error {
		calls, _ := directory.snapshot()
		if calls > 0 {
			cancel()
		}
		return ctx.Err()
	}

	sw.Run(ctx)

	if calls, _ := directory.snapshot(); calls == 0 {
		t.Fatal("Run() never reached the legacy lease-metadata reaper")
	}
}
