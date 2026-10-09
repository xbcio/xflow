package control

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"
)

// fakeDepthDirectory satisfies the sweeper's directory capabilities and reports
// a fixed set of queue depths.
type fakeDepthDirectory struct {
	mu     sync.Mutex
	calls  int
	depths map[string]int64
	err    error
}

func (f *fakeDepthDirectory) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

func (f *fakeDepthDirectory) AssignmentQueueDepths(context.Context) (map[string]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.depths, nil
}

func (f *fakeDepthDirectory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// recordingDepthObserver captures what the sweeper reported, keyed by lane.
type recordingDepthObserver struct {
	*fakeObserver
	mu     sync.Mutex
	depths map[string]int64
}

func (o *recordingDepthObserver) OnAssignmentQueueDepth(_ context.Context, lane string, depth int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.depths == nil {
		o.depths = make(map[string]int64)
	}
	o.depths[lane] = depth
}

func (o *recordingDepthObserver) snapshot() map[string]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	depths := make(map[string]int64, len(o.depths))
	maps.Copy(depths, o.depths)
	return depths
}

// TestLeaseSweeperReportsAssignmentQueueDepthsLeaderGated pins the reporting
// contract: every lane the directory names reaches the observer verbatim, the
// legacy queue arrives under QueueLaneLegacy (the empty lane name the directory
// uses internally is never a label value), and a non-leader reads nothing —
// the queues are cluster-wide, so a second reporter would fight the gauge.
func TestLeaseSweeperReportsAssignmentQueueDepthsLeaderGated(t *testing.T) {
	directory := &fakeDepthDirectory{depths: map[string]int64{
		QueueLaneLegacy:    4,
		memoryLaneTestType: 2,
	}}
	observer := &recordingDepthObserver{fakeObserver: &fakeObserver{}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:         elector,
		RunnerDirectory: directory,
		Observer:        observer,
	})

	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.snapshot(); len(got) != 2 || got[QueueLaneLegacy] != 4 || got[memoryLaneTestType] != 2 {
		t.Fatalf("reported depths = %v, want legacy=4 and %s=2", got, memoryLaneTestType)
	}
	if calls := directory.callCount(); calls != 1 {
		t.Fatalf("directory reads = %d, want 1", calls)
	}

	elector.leader.Store(false)
	sw.ReportQueueDepthsOnce(context.Background())
	if calls := directory.callCount(); calls != 1 {
		t.Fatalf("non-leader reads = %d, want the walk to be skipped", calls)
	}
	if got := observer.snapshot(); len(got) != 2 || got[QueueLaneLegacy] != 4 {
		t.Fatalf("non-leader report changed the observed depths: %v", got)
	}
}

// TestLeaseSweeperQueueDepthFailureIsContained pins the failure shape: once a
// successful read has reported depths, a failing read must leave the last
// reported values in place (never zeroing them) and must not stick — the next
// cadence succeeds and reports the new values.
func TestLeaseSweeperQueueDepthFailureIsContained(t *testing.T) {
	directory := &fakeDepthDirectory{depths: map[string]int64{QueueLaneLegacy: 4}}
	observer := &recordingDepthObserver{fakeObserver: &fakeObserver{}}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
		Observer:        observer,
	})

	// A successful read first, so the failing one below has a last reported
	// value it could wrongly overwrite.
	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.snapshot(); got[QueueLaneLegacy] != 4 {
		t.Fatalf("successful read reported %v, want legacy=4", got)
	}

	directory.mu.Lock()
	directory.err = errors.New("redis is down")
	directory.mu.Unlock()
	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.snapshot(); len(got) != 1 || got[QueueLaneLegacy] != 4 {
		t.Fatalf("failed read reported %v, want the last values left in place (legacy=4)", got)
	}

	directory.mu.Lock()
	directory.err = nil
	directory.depths = map[string]int64{QueueLaneLegacy: 7, memoryLaneTestType: 2}
	directory.mu.Unlock()
	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.snapshot(); got[QueueLaneLegacy] != 7 || got[memoryLaneTestType] != 2 {
		t.Fatalf("recovered read reported %v, want legacy=7 and %s=2", got, memoryLaneTestType)
	}
}

// TestLeaseSweeperQueueDepthSkipsUnsupportedDirectoryAndAbsentObserver pins the
// two cheap no-ops: a directory without the capability reports nothing without
// failing, and no observer means no directory read at all.
func TestLeaseSweeperQueueDepthSkipsUnsupportedDirectoryAndAbsentObserver(t *testing.T) {
	observer := &recordingDepthObserver{fakeObserver: &fakeObserver{}}
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: &fakeExpiredLeaseReleaser{},
		Observer:        observer,
	})
	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.snapshot(); len(got) != 0 {
		t.Fatalf("unsupported directory reported %v, want nothing", got)
	}

	directory := &fakeDepthDirectory{depths: map[string]int64{QueueLaneLegacy: 4}}
	sw = NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		RunnerDirectory: directory,
	})
	sw.ReportQueueDepthsOnce(context.Background())
	if calls := directory.callCount(); calls != 0 {
		t.Fatalf("directory reads without an observer = %d, want 0", calls)
	}
}

// TestLeaseSweeperRunDrivesTheQueueDepthReport checks the wiring rather than
// the rate: Run must read the depths without waiting for an operator.
func TestLeaseSweeperRunDrivesTheQueueDepthReport(t *testing.T) {
	directory := &fakeDepthDirectory{depths: map[string]int64{QueueLaneLegacy: 4}}
	observer := &recordingDepthObserver{fakeObserver: &fakeObserver{}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:         elector,
		RunnerDirectory: directory,
		Observer:        observer,
		Period:          time.Millisecond,
	})
	sw.sleepFunc = func(context.Context, time.Duration) error {
		if directory.callCount() > 0 {
			cancel()
		}
		return ctx.Err()
	}

	sw.Run(ctx)

	if calls := directory.callCount(); calls == 0 {
		t.Fatal("Run() never read the assignment queue depths")
	}
	if got := observer.snapshot(); got[QueueLaneLegacy] != 4 {
		t.Fatalf("Run() reported %v, want legacy=4", got)
	}
}
