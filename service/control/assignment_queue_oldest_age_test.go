package control

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"sync"
	"testing"
	"time"
)

func queuedAtStamp(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, id AssignmentID) (int64, bool) {
	t.Helper()
	raw, err := directory.rdb.HGet(ctx, directory.keys.assignmentQueuedAt, string(id)).Result()
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("queued-at stamp %q is not an integer: %v", raw, err)
	}
	return v, true
}

// TestQueuedAtStampLifecycle pins the write side: enqueue stamps with the
// server clock, a claim clears the stamp (the entry left the queue), a requeue
// restamps with the requeue time, and a drop clears it.
func TestQueuedAtStampLifecycle(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb,
		WithRedisRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithRedisRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	session := registerLaneRunner(t, ctx, directory, "runner-queued-at", 1, memoryLaneTestType)
	assignment := redisDirectoryTestAssignment("exec-queued-at/stamp/1")
	assignment.Routing.NodeType = memoryLaneTestType

	mr.SetTime(time.UnixMilli(1_000_000))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	if got, ok := queuedAtStamp(t, ctx, directory, assignment.AssignmentID); !ok || got != 1_000_000 {
		t.Fatalf("enqueue stamp = %d (present=%v), want 1000000", got, ok)
	}

	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if _, ok := queuedAtStamp(t, ctx, directory, assignment.AssignmentID); ok {
		t.Fatal("claim left the queued-at stamp behind")
	}

	mr.SetTime(time.UnixMilli(2_000_000))
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}
	if err := directory.SettleClaimHandoff(ctx, claim.ClaimID, HandoffDispositionRequeue); err != nil {
		t.Fatalf("SettleClaimHandoff(requeue) error = %v", err)
	}
	if got, ok := queuedAtStamp(t, ctx, directory, assignment.AssignmentID); !ok || got != 2_000_000 {
		t.Fatalf("requeue stamp = %d (present=%v), want the requeue time 2000000", got, ok)
	}

	if err := directory.ClearAssignment(ctx, assignment.AssignmentID); err != nil {
		t.Fatalf("ClearAssignment() error = %v", err)
	}
	if _, ok := queuedAtStamp(t, ctx, directory, assignment.AssignmentID); ok {
		t.Fatal("ClearAssignment left the queued-at stamp behind")
	}
}

// TestAssignmentQueueOldestAgesSamplesTheHead pins the read side: the age is
// measured from the earliest stamp in the head sample on the server clock,
// unstamped entries (an older writer's) are skipped, a queue with no stamped
// entry is absent rather than zero, and lanes are named like the depth report.
func TestAssignmentQueueOldestAgesSamplesTheHead(t *testing.T) {
	ctx := context.Background()
	idleLane := "xflow.sas.ulp-result"
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb,
		WithRedisRunnerDirectoryLanes([]string{memoryLaneTestType, idleLane}),
		WithRedisRunnerDirectoryLaneWriteMode(LaneWriteLaneOnly),
	)

	// An older writer's entry at the legacy head: queued, but never stamped.
	if err := rdb.RPush(ctx, directory.keys.queue, "exec-old-writer/unstamped").Err(); err != nil {
		t.Fatal(err)
	}
	mr.SetTime(time.UnixMilli(10_000))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-age/legacy/a"))
	mr.SetTime(time.UnixMilli(40_000))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-age/legacy/b"))
	onLane := redisDirectoryTestAssignment("exec-age/lane/a")
	onLane.Routing.NodeType = memoryLaneTestType
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, onLane)

	mr.SetTime(time.UnixMilli(70_000))
	ages, err := directory.AssignmentQueueOldestAges(ctx)
	if err != nil {
		t.Fatalf("AssignmentQueueOldestAges() error = %v", err)
	}
	want := map[string]time.Duration{QueueLaneLegacy: 60 * time.Second, memoryLaneTestType: 30 * time.Second}
	if !maps.Equal(ages, want) {
		t.Fatalf("ages = %v, want %v (idle lane absent)", ages, want)
	}

	// A queue holding only unstamped entries is not measured at all.
	if err := rdb.Del(ctx, directory.keys.assignmentQueuedAt).Err(); err != nil {
		t.Fatal(err)
	}
	ages, err = directory.AssignmentQueueOldestAges(ctx)
	if err != nil {
		t.Fatalf("AssignmentQueueOldestAges() error = %v", err)
	}
	if len(ages) != 0 {
		t.Fatalf("ages without stamps = %v, want nothing reported", ages)
	}
}

func TestAssignmentQueueOldestAgesSurfacesReadFailure(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	mr.Close()
	if ages, err := directory.AssignmentQueueOldestAges(ctx); err == nil {
		t.Fatalf("AssignmentQueueOldestAges() = %v with no error, want the failure surfaced", ages)
	}
}

// TestMemoryAssignmentQueueOldestAgesMirrorsRedis pins the double against the
// same rules: stamped on enqueue and requeue by the directory clock, cleared
// on claim, absent when empty.
func TestMemoryAssignmentQueueOldestAgesMirrorsRedis(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_000, 0)
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteLaneOnly),
		WithMemoryRunnerDirectoryClock(func() time.Time { return now }),
	)
	session := memoryLaneSession(t, ctx, dir, "runner-memory-age", memoryLaneTestType)
	assignment := redisDirectoryTestAssignment("exec-memory-age/lane/a")
	assignment.Routing.NodeType = memoryLaneTestType
	if _, err := dir.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	now = now.Add(45 * time.Second)
	ages, _ := dir.AssignmentQueueOldestAges(ctx)
	if want := map[string]time.Duration{memoryLaneTestType: 45 * time.Second}; !maps.Equal(ages, want) {
		t.Fatalf("ages = %v, want %v", ages, want)
	}

	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v", ok, err)
	}
	if ages, _ := dir.AssignmentQueueOldestAges(ctx); len(ages) != 0 {
		t.Fatalf("ages after claim = %v, want nothing", ages)
	}
	if _, ok := dir.queuedAt[assignment.AssignmentID]; ok {
		t.Fatal("claim left the queued-at stamp behind")
	}

	now = now.Add(10 * time.Second)
	if err := dir.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue); err != nil {
		t.Fatalf("ReleaseClaim(requeue) error = %v", err)
	}
	now = now.Add(5 * time.Second)
	ages, _ = dir.AssignmentQueueOldestAges(ctx)
	if want := map[string]time.Duration{memoryLaneTestType: 5 * time.Second}; !maps.Equal(ages, want) {
		t.Fatalf("ages after requeue = %v, want %v (measured from the requeue)", ages, want)
	}
}

type fakeAgeDirectory struct {
	fakeDepthDirectory
	ageMu   sync.Mutex
	ageErr  error
	ages    map[string]time.Duration
	ageRead int
}

func (f *fakeAgeDirectory) AssignmentQueueOldestAges(context.Context) (map[string]time.Duration, error) {
	f.ageMu.Lock()
	defer f.ageMu.Unlock()
	f.ageRead++
	if f.ageErr != nil {
		return nil, f.ageErr
	}
	return f.ages, nil
}

type recordingAgeObserver struct {
	*recordingDepthObserver
	ageMu sync.Mutex
	ages  map[string]time.Duration
}

func (o *recordingAgeObserver) OnAssignmentQueueOldestAge(_ context.Context, lane string, age time.Duration) {
	o.ageMu.Lock()
	defer o.ageMu.Unlock()
	if o.ages == nil {
		o.ages = make(map[string]time.Duration)
	}
	o.ages[lane] = age
}

func (o *recordingAgeObserver) ageSnapshot() map[string]time.Duration {
	o.ageMu.Lock()
	defer o.ageMu.Unlock()
	return maps.Clone(o.ages)
}

// TestLeaseSweeperReportsOldestAgesBesideDepths pins the sweep discipline for
// the age series: same leader gate, and a failed age read neither zeroes the
// last ages nor suppresses the depths read in the same cadence.
func TestLeaseSweeperReportsOldestAgesBesideDepths(t *testing.T) {
	directory := &fakeAgeDirectory{
		fakeDepthDirectory: fakeDepthDirectory{depths: map[string]int64{QueueLaneLegacy: 3}},
		ages:               map[string]time.Duration{QueueLaneLegacy: 90 * time.Second},
	}
	observer := &recordingAgeObserver{recordingDepthObserver: &recordingDepthObserver{fakeObserver: &fakeObserver{}}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(&fakeLeaseLister{}, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:         elector,
		RunnerDirectory: directory,
		Observer:        observer,
	})

	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.ageSnapshot(); got[QueueLaneLegacy] != 90*time.Second {
		t.Fatalf("reported ages = %v, want legacy=90s", got)
	}

	directory.ageMu.Lock()
	directory.ageErr = errors.New("redis is down")
	directory.ageMu.Unlock()
	directory.mu.Lock()
	directory.depths = map[string]int64{QueueLaneLegacy: 5}
	directory.mu.Unlock()
	sw.ReportQueueDepthsOnce(context.Background())
	if got := observer.ageSnapshot(); len(got) != 1 || got[QueueLaneLegacy] != 90*time.Second {
		t.Fatalf("failed age read changed the ages: %v", got)
	}
	if got := observer.snapshot(); got[QueueLaneLegacy] != 5 {
		t.Fatalf("depth after failed age read = %v, want legacy=5", got)
	}

	elector.leader.Store(false)
	sw.ReportQueueDepthsOnce(context.Background())
	if directory.ageRead != 2 {
		t.Fatalf("age reads = %d, want 2 (non-leader skipped)", directory.ageRead)
	}
}
