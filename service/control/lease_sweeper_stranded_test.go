package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStrandedReaperDirectory satisfies the directory capabilities the sweeper
// discovers by type assertion, so it can stand in for a RedisRunnerDirectory
// without a Redis.
type fakeStrandedReaperDirectory struct {
	mu        sync.Mutex
	calls     int
	limits    []int
	released  int
	inspected int
	err       error
}

func (f *fakeStrandedReaperDirectory) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

func (f *fakeStrandedReaperDirectory) ReapStrandedLeases(_ context.Context, limit int) (ReapResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.limits = append(f.limits, limit)
	return ReapResult{Inspected: f.inspected, Released: f.released}, f.err
}

func (f *fakeStrandedReaperDirectory) snapshot() (int, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, append([]int(nil), f.limits...)
}

func TestLeaseSweeperReapsStrandedLeasesAtBoundedLeaderGatedCadence(t *testing.T) {
	directory := &fakeStrandedReaperDirectory{released: 3}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:                 elector,
		RunnerDirectory:         directory,
		StrandedLeaseReapPeriod: time.Minute,
		StrandedLeaseReapBatch:  17,
	})
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }

	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 3 {
		t.Fatalf("first ReapStrandedLeasesOnce() = %d, want 3", got)
	}
	if calls, limits := directory.snapshot(); calls != 1 || limits[0] != 17 {
		t.Fatalf("reap calls=%d limits=%v, want 1/[17]", calls, limits)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 0 {
		t.Fatalf("early ReapStrandedLeasesOnce() = %d, want 0", got)
	}
	if calls, _ := directory.snapshot(); calls != 1 {
		t.Fatalf("early reap calls = %d, want 1", calls)
	}

	now = now.Add(30 * time.Second)
	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 3 {
		t.Fatalf("scheduled ReapStrandedLeasesOnce() = %d, want 3", got)
	}
	if calls, _ := directory.snapshot(); calls != 2 {
		t.Fatalf("scheduled reap calls = %d, want 2", calls)
	}

	elector.leader.Store(false)
	now = now.Add(time.Minute)
	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 0 {
		t.Fatalf("non-leader ReapStrandedLeasesOnce() = %d, want 0", got)
	}
	if calls, _ := directory.snapshot(); calls != 2 {
		t.Fatalf("non-leader reap calls = %d, want 2", calls)
	}
}

// TestLeaseSweeperStrandedReapFailureIsContained pins the reason this is wired
// into the sweeper at all: a failing reclaim is best-effort maintenance and must
// never turn into a lease-execution failure or stop the loop.
func TestLeaseSweeperStrandedReapFailureIsContained(t *testing.T) {
	directory := &fakeStrandedReaperDirectory{err: errors.New("redis unavailable")}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }

	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 0 {
		t.Fatalf("ReapStrandedLeasesOnce() = %d, want 0 on error", got)
	}
	if calls, _ := directory.snapshot(); calls != 1 {
		t.Fatalf("reap calls = %d, want 1", calls)
	}
}

// TestLeaseSweeperSkipsStrandedReapWithoutCapability keeps the in-memory
// directory and every other RunnerDirectory implementation out of this path.
func TestLeaseSweeperSkipsStrandedReapWithoutCapability(t *testing.T) {
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: &fakeExpiredLeaseReleaser{},
	})
	if got := sw.ReapStrandedLeasesOnce(context.Background()); got != 0 {
		t.Fatalf("ReapStrandedLeasesOnce() = %d, want 0 without the capability", got)
	}
}

// TestLeaseSweeperStrandedReapDefaultsAreLoadBearing covers the two fallbacks
// for the only production construction, controlplane.go's LeaseSweeperConfig,
// which sets neither. Both zero values fail silently rather than loudly: a zero
// batch reaches the reaper as limit<=0, a documented no-op, so a mistyped config
// would disable the reclaim entirely while everything else stayed green; a zero
// period makes the rate limiter's `<` comparison unsatisfiable and puts a
// reaper call on every sweep.
func TestLeaseSweeperStrandedReapDefaultsAreLoadBearing(t *testing.T) {
	directory := &fakeStrandedReaperDirectory{released: 1}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
	})

	if sw.strandedPeriod != DefaultStrandedLeaseReapPeriod {
		t.Fatalf("stranded period = %v, want %v", sw.strandedPeriod, DefaultStrandedLeaseReapPeriod)
	}
	if sw.strandedBatch != defaultStrandedLeaseReapBatch {
		t.Fatalf("stranded batch = %d, want %d", sw.strandedBatch, defaultStrandedLeaseReapBatch)
	}

	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	sw.ReapStrandedLeasesOnce(context.Background())
	sw.ReapStrandedLeasesOnce(context.Background())
	calls, limits := directory.snapshot()
	if calls != 1 {
		t.Fatalf("reap calls = %d, want 1: the default period must suppress the second", calls)
	}
	if limits[0] != defaultStrandedLeaseReapBatch {
		t.Fatalf("reap limit = %d, want %d: a limit of 0 is a silent no-op", limits[0], defaultStrandedLeaseReapBatch)
	}
}

// TestLeaseSweeperRunDrivesTheStrandedReap checks the wiring rather than the
// rate: Run must reach the reaper without waiting for an operator.
func TestLeaseSweeperRunDrivesTheStrandedReap(t *testing.T) {
	directory := &fakeStrandedReaperDirectory{released: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
		Period:          time.Millisecond,
	})
	sw.clock = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
	sw.sleepFunc = func(context.Context, time.Duration) error {
		calls, _ := directory.snapshot()
		if calls > 0 {
			cancel()
		}
		return ctx.Err()
	}

	sw.Run(ctx)

	if calls, _ := directory.snapshot(); calls == 0 {
		t.Fatal("Run() never reached the stranded-lease reaper")
	}
}
