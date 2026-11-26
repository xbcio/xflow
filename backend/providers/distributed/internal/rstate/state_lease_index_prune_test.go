package rstate

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// forEachLeaseIndexBackend runs fn against miniredis and, when
// XFLOW_TEST_REDIS_ADDR is set, against that real Redis. The prune path pairs a
// ZRANGEBYSCORE LIMIT with pipelined single-key EXISTS, so it is worth pinning
// against a real server while keeping the package runnable with no dependency.
func forEachLeaseIndexBackend(t *testing.T, fn func(t *testing.T, s *Store, rdb *redis.Client)) {
	t.Helper()
	t.Run("miniredis", func(t *testing.T) {
		state, _, rdb := newTestRedisState(t)
		fn(t, state, rdb)
	})
	t.Run("redis", func(t *testing.T) {
		addr := realRedisAddr(t)
		rdb := freshRealRedis(t, addr)
		fn(t, New(rdb, nil, time.Minute), rdb)
	})
}

// seedLeaseIndexOrphans writes count bare index members with the given score.
// No per-execution lease ZSET is created, so every one of them is an orphan.
func seedLeaseIndexOrphans(t *testing.T, rdb *redis.Client, count int, score float64) []types.ExecutionID {
	t.Helper()
	ctx := context.Background()
	ids := make([]types.ExecutionID, count)
	pipe := rdb.Pipeline()
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("orphan-%05d", i))
		pipe.ZAdd(ctx, leaseExpiryExecIndexKey(namespace.Default), redis.Z{Score: score, Member: string(ids[i])})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed %d orphan index members: %v", count, err)
	}
	return ids
}

func requireLeaseIndexCard(t *testing.T, rdb *redis.Client, want int64) {
	t.Helper()
	got, err := rdb.ZCard(context.Background(), leaseExpiryExecIndexKey(namespace.Default)).Result()
	if err != nil {
		t.Fatalf("ZCard lease index: %v", err)
	}
	if got != want {
		t.Fatalf("lease index cardinality = %d, want %d", got, want)
	}
}

// TestListExpiredLeasesPrunesOrphanLeaseIndex pins the bulk orphan prune: a
// single sweep must clear thousands of orphan members that the bounded reclaim
// read could never reach (it only ever examines leaseIndexBatchLimit members),
// while still returning and keeping the real expired leases.
func TestListExpiredLeasesPrunesOrphanLeaseIndex(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		const live = 3
		// Real expired leases score older than the orphans, so they sit at the
		// head of the index and are reachable even by the unfixed sweep.
		base := time.Now().Add(-time.Hour).UTC()
		liveIDs := make(map[types.ExecutionID]struct{}, live)
		for i := 0; i < live; i++ {
			id := types.ExecutionID(fmt.Sprintf("live-%d", i))
			liveIDs[id] = struct{}{}
			mustUpsertRunning(t, state, id, "n", fmt.Sprintf("tok-live-%d", i),
				base.Add(time.Duration(i)*time.Second), time.Second)
		}
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		if len(got) != live {
			t.Fatalf("returned %d expired leases, want the %d real ones", len(got), live)
		}
		for _, lease := range got {
			if _, ok := liveIDs[lease.ExecutionID]; !ok {
				t.Fatalf("returned orphan lease %q", lease.ExecutionID)
			}
		}
		requireLeaseIndexCard(t, rdb, live)
		for id := range liveIDs {
			if _, ok := leaseIndexScore(t, rdb, id); !ok {
				t.Fatalf("live member %q was pruned", id)
			}
		}
	})
}

// TestLeaseIndexPruneReachesOrphansBehindSaturatedHead pins the Offset=kept
// rule: a head full of real expired leases (more than one chunk, more than the
// reclaim budget) must not stop the prune from reaching the orphans after them.
func TestLeaseIndexPruneReachesOrphansBehindSaturatedHead(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		const live = 300
		base := time.Now().Add(-time.Hour).UTC()
		liveIDs := make(map[types.ExecutionID]struct{}, live)
		for i := 0; i < live; i++ {
			id := types.ExecutionID(fmt.Sprintf("live-%03d", i))
			liveIDs[id] = struct{}{}
			mustUpsertRunning(t, state, id, "n", fmt.Sprintf("tok-live-%03d", i),
				base.Add(time.Duration(i)*time.Second), time.Second)
		}
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		if len(got) != leaseIndexBatchLimit {
			t.Fatalf("returned %d expired leases, want the %d-member reclaim budget",
				len(got), leaseIndexBatchLimit)
		}
		for _, lease := range got {
			if _, ok := liveIDs[lease.ExecutionID]; !ok {
				t.Fatalf("returned orphan lease %q", lease.ExecutionID)
			}
		}
		requireLeaseIndexCard(t, rdb, live)
	})
}

// TestLeaseIndexPruneLeavesFutureRegistrations pins that the prune only looks
// at members the reclaim read would consider: a live execution whose index
// score is still in the future is neither removed nor rescored.
func TestLeaseIndexPruneLeavesFutureRegistrations(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		mustUpsertRunning(t, state, "future-1", "n", "tok-future", time.Now().UTC(), time.Hour)
		// Registration scores min(now, deadline), i.e. now; re-arm to the exact
		// minimum so the member carries the future deadline as intended.
		state.rearmLeaseExpiryIndex(ctx, namespace.Default, "future-1")
		want, err := rdb.ZScore(ctx, leaseExpiryZSetKey(namespace.Default, "future-1"),
			leaseExpiryMember("future-1", "n")).Result()
		if err != nil {
			t.Fatal(err)
		}
		if want <= float64(time.Now().UnixMilli()) {
			t.Fatalf("precondition: lease score = %v, want a future deadline", want)
		}

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil || len(got) != 0 {
			t.Fatalf("ListExpiredLeases() = %+v err=%v, want none", got, err)
		}
		if score, ok := leaseIndexScore(t, rdb, "future-1"); !ok || score != want {
			t.Fatalf("future registration = %v ok=%v, want untouched at %v", score, ok, want)
		}
	})
}

// TestLeaseIndexPruneIsIdempotent pins that a second sweep after a fully
// pruned index neither errors nor changes the result.
func TestLeaseIndexPruneIsIdempotent(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		base := time.Now().Add(-time.Hour).UTC()
		mustUpsertRunning(t, state, "live", "n", "tok-live", base, time.Second)
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		first, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil || len(first) != 1 {
			t.Fatalf("first ListExpiredLeases() = %+v err=%v, want the live lease", first, err)
		}
		requireLeaseIndexCard(t, rdb, 1)
		second, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("second ListExpiredLeases() error = %v", err)
		}
		if len(second) != len(first) || second[0].ExecutionID != first[0].ExecutionID {
			t.Fatalf("second = %+v, want the first result %+v", second, first)
		}
		requireLeaseIndexCard(t, rdb, 1)
	})
}

// addOnLeaseIndexExistsPipeline fires fn once, after the first pipeline of
// single-key EXISTS of the prune has returned. For the prune that instant is
// exactly "read the execution as an orphan, ZREM not sent yet" — where a
// producer would slip its per-execution ZSET write in.
type addOnLeaseIndexExistsPipeline struct {
	once sync.Once
	key  string
	fn   func()
}

func (h *addOnLeaseIndexExistsPipeline) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *addOnLeaseIndexExistsPipeline) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *addOnLeaseIndexExistsPipeline) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if len(cmds) > 0 && cmds[0].Name() == "exists" &&
			len(cmds[0].Args()) > 1 && cmds[0].Args()[1] == h.key {
			h.once.Do(h.fn)
		}
		return err
	}
}

// blockingLeaseWalkScanHook parks the first SCAN of the safety-net walk and
// reports it. Everything the sweep runs before the walk has completed by then,
// so a test can inspect the index at the exact instant the walk would block the
// rest of the sweep.
type blockingLeaseWalkScanHook struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (h *blockingLeaseWalkScanHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *blockingLeaseWalkScanHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "scan" {
			h.once.Do(func() { close(h.started) })
			<-h.release
		}
		return next(ctx, cmd)
	}
}

func (h *blockingLeaseWalkScanHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestListExpiredLeasesPrunesOrphansBeforeWalk pins the prune/walk order: one
// sweep must clear the orphan backlog before it enters the safety-net walk. A
// walk without a round-done marker backfills inside the call and nothing behind
// it runs until the whole keyspace has been SCANned; with the prune ordered
// after it, a long backfill starves the prune precisely while the orphan
// backlog grows (the production symptom). The walk is parked at its first SCAN
// here, so the assertion below sees the sweep at that unbounded point.
func TestListExpiredLeasesPrunesOrphansBeforeWalk(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour).UTC()
	seedLeaseIndexOrphans(t, rdb, 200, float64(base.UnixMilli()))

	hook := &blockingLeaseWalkScanHook{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rdb.AddHook(hook)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hook.release) }) }
	defer release()

	done := make(chan error, 1)
	go func() {
		_, err := state.ListExpiredLeases(ctx, time.Now())
		done <- err
	}()

	select {
	case <-hook.started:
	case err := <-done:
		t.Fatalf("ListExpiredLeases returned before the walk's first SCAN: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the safety-net walk never reached its first SCAN")
	}

	// The walk is parked; the prune must already have run.
	requireLeaseIndexCard(t, rdb, 0)

	release()
	if err := <-done; err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	requireLeaseIndexCard(t, rdb, 0)
}

// leasePruneFaultHook drives individual prune commands for the timeout and
// diagnostics tests. It matches on the command name (zrangebyscore, the EXISTS
// pipeline, zrem), never on internal state, so it stays valid across prune
// implementation changes.
type leasePruneFaultHook struct {
	mu       sync.Mutex
	zrange   int
	exists   int
	zrem     int
	onZRange func(ctx context.Context, n int) error
	onExists func(ctx context.Context, n int) error
	onZRem   func(ctx context.Context, n int) error
}

func (h *leasePruneFaultHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *leasePruneFaultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		var (
			n  int
			fn func(context.Context, int) error
		)
		switch cmd.Name() {
		case "zrangebyscore":
			h.zrange++
			n, fn = h.zrange, h.onZRange
		case "zrem":
			h.zrem++
			n, fn = h.zrem, h.onZRem
		}
		h.mu.Unlock()
		if fn != nil {
			if err := fn(ctx, n); err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}

func (h *leasePruneFaultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) == 0 || cmds[0].Name() != "exists" {
			return next(ctx, cmds)
		}
		h.mu.Lock()
		h.exists++
		n := h.exists
		fn := h.onExists
		h.mu.Unlock()
		if fn != nil {
			if err := fn(ctx, n); err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}
}

// TestLeaseIndexPruneKeepsCompletedRoundsWhenALaterRoundTimesOut pins the
// per-round timeout budget. The first two rounds each spend real time inside
// their ZREM (under the per-round budget, but more than one shared budget would
// allow for both), and the third round's ZRANGEBYSCORE fails with
// context.DeadlineExceeded. The rounds that finished before the failure must
// have already removed their orphans. One call-wide timeout, as the prune used
// to have, cannot get past the second round here.
func TestLeaseIndexPruneKeepsCompletedRoundsWhenALaterRoundTimesOut(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	// leaseIndexPruneChunkMax: one round examines at most this many members.
	const chunk, seeded = leaseIndexPruneChunkMax, 5000
	base := time.Now().Add(-time.Hour).UTC()
	seedLeaseIndexOrphans(t, rdb, seeded, float64(base.UnixMilli()))

	rdb.AddHook(&leasePruneFaultHook{
		onZRem: func(context.Context, int) error {
			time.Sleep(2750 * time.Millisecond)
			return nil
		},
		onZRange: func(_ context.Context, n int) error {
			if n >= 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	})

	state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))

	// Rounds 1 and 2 completed and are durably gone; round 3 failed before
	// removing anything.
	requireLeaseIndexCard(t, rdb, seeded-2*chunk)
	if _, ok := leaseIndexScore(t, rdb, types.ExecutionID(fmt.Sprintf("orphan-%05d", 2*chunk))); !ok {
		t.Fatal("an orphan from the failed round 3 was removed")
	}
}

// leasePruneLogCapture records every logger call so a test can assert on the
// prune's structured line without depending on a logging backend.
type leasePruneLogCapture struct {
	mu      sync.Mutex
	entries []leasePruneLogEntry
}

type leasePruneLogEntry struct {
	level string
	msg   string
	args  []any
}

func (l *leasePruneLogCapture) record(level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, leasePruneLogEntry{level: level, msg: msg, args: append([]any(nil), args...)})
}

func (l *leasePruneLogCapture) snapshot() []leasePruneLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]leasePruneLogEntry(nil), l.entries...)
}

func (l *leasePruneLogCapture) Debug(string, ...any)  {}
func (l *leasePruneLogCapture) Debugf(string, ...any) {}
func (l *leasePruneLogCapture) Info(msg string, args ...any) {
	l.record("info", msg, args)
}
func (l *leasePruneLogCapture) Infof(format string, args ...any) {
	l.record("info", fmt.Sprintf(format, args...), nil)
}
func (l *leasePruneLogCapture) Warn(msg string, args ...any) {
	l.record("warn", msg, args)
}
func (l *leasePruneLogCapture) Warnf(format string, args ...any) {
	l.record("warn", fmt.Sprintf(format, args...), nil)
}
func (l *leasePruneLogCapture) Error(msg string, args ...any) {
	l.record("error", msg, args)
}
func (l *leasePruneLogCapture) Errorf(format string, args ...any) {
	l.record("error", fmt.Sprintf(format, args...), nil)
}
func (l *leasePruneLogCapture) Panic(string, ...any)  {}
func (l *leasePruneLogCapture) Panicf(string, ...any) {}

func leasePruneLogField(args []any, key string) (any, bool) {
	for i := 0; i+1 < len(args); i += 2 {
		if k, ok := args[i].(string); ok && k == key {
			return args[i+1], true
		}
	}
	return nil, false
}

// TestLeaseIndexPruneLogsEveryFailureAndDeletion pins the prune's diagnostics:
// a call that removed members logs one structured line, every failed call logs
// one (never through noteLeaseIndexFailure's rate limit, so two consecutive
// failures produce two lines), the failure line names the stage and carries the
// elapsed field, and a call with nothing to examine logs nothing.
func TestLeaseIndexPruneLogsEveryFailureAndDeletion(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	log := &leasePruneLogCapture{}
	state.logger = log
	base := time.Now().Add(-time.Hour).UTC()

	var failing atomic.Bool
	rdb.AddHook(&leasePruneFaultHook{onExists: func(context.Context, int) error {
		if failing.Load() {
			return context.DeadlineExceeded
		}
		return nil
	}})

	// A successful call that removed orphans reports them.
	seedLeaseIndexOrphans(t, rdb, 4, float64(base.UnixMilli()))
	state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))
	entries := log.snapshot()
	if len(entries) != 1 {
		t.Fatalf("deletion call logged %d lines, want 1: %+v", len(entries), entries)
	}
	checkPruneLog(t, entries[0], map[string]any{
		"namespace": string(namespace.Default),
		"removed":   4,
	})

	// Two consecutive failures both log, with the failing stage and elapsed.
	failing.Store(true)
	seedLeaseIndexOrphans(t, rdb, 1, float64(base.UnixMilli()))
	for i := 0; i < 2; i++ {
		state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))
	}
	entries = log.snapshot()
	if len(entries) != 3 {
		t.Fatalf("two failed calls produced %d log lines in total, want 3: %+v", len(entries), entries)
	}
	for _, entry := range entries[1:] {
		checkPruneLog(t, entry, map[string]any{
			"namespace": string(namespace.Default),
			"stage":     "exists",
			"removed":   0,
		})
		if errValue, ok := leasePruneLogField(entry.args, "err"); !ok || errValue == nil {
			t.Fatalf("failure line has no err field: %+v", entry)
		}
	}

	// Nothing to examine: no line, even though a stale failure would still be
	// inside noteLeaseIndexFailure's rate-limit window.
	if err := rdb.ZRem(ctx, leaseExpiryExecIndexKey(namespace.Default), "orphan-00000").Err(); err != nil {
		t.Fatal(err)
	}
	state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))
	if got := len(log.snapshot()); got != 3 {
		t.Fatalf("empty call logged a line: total %d, want 3", got)
	}
}

// checkPruneLog asserts the entry is the structured prune line carrying every
// required field, with the listed fields matching want.
func checkPruneLog(t *testing.T, entry leasePruneLogEntry, want map[string]any) {
	t.Helper()
	for _, key := range []string{"namespace", "rounds", "checked", "orphans", "removed", "rearmed", "elapsed", "chunk"} {
		if _, ok := leasePruneLogField(entry.args, key); !ok {
			t.Fatalf("prune log line has no %q field: %+v", key, entry)
		}
	}
	for key, wantValue := range want {
		gotValue, ok := leasePruneLogField(entry.args, key)
		if !ok || fmt.Sprint(gotValue) != fmt.Sprint(wantValue) {
			t.Fatalf("prune log %q = %v ok=%v, want %v: %+v", key, gotValue, ok, wantValue, entry)
		}
	}
}

// TestLeaseIndexPruneChunkAdaptsToTimeouts pins the per-Store adaptive chunk: a
// round that fails on context.DeadlineExceeded halves it (floored at
// leaseIndexPruneChunkMin), a completed round doubles it (capped at
// leaseIndexPruneChunkMax), and a failure that is not a deadline leaves it
// alone.
func TestLeaseIndexPruneChunkAdaptsToTimeouts(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour).UTC()
	maxMs := fmt.Sprintf("%d", time.Now().UnixMilli())

	const (
		failNone = iota
		failDeadline
		failOther
	)
	var mode atomic.Int32
	rdb.AddHook(&leasePruneFaultHook{onExists: func(context.Context, int) error {
		switch mode.Load() {
		case failDeadline:
			return context.DeadlineExceeded
		case failOther:
			return fmt.Errorf("injected exists failure")
		}
		return nil
	}})
	seedLeaseIndexOrphans(t, rdb, 1, float64(base.UnixMilli()))

	state.leaseIndexPruneChunk.Store(512)
	mode.Store(failOther)
	state.pruneOrphanLeaseIndex(ctx, namespace.Default, maxMs)
	if got := state.leasePruneChunk(); got != 512 {
		t.Fatalf("chunk after a non-deadline failure = %d, want unchanged 512", got)
	}

	mode.Store(failDeadline)
	for _, want := range []int{256, 128, 128} {
		state.pruneOrphanLeaseIndex(ctx, namespace.Default, maxMs)
		if got := state.leasePruneChunk(); got != want {
			t.Fatalf("chunk after a timed-out round = %d, want %d", got, want)
		}
	}

	mode.Store(failNone)
	for _, want := range []int{256, 512, 1024, 2048, 2048} {
		state.pruneOrphanLeaseIndex(ctx, namespace.Default, maxMs)
		if got := state.leasePruneChunk(); got != want {
			t.Fatalf("chunk after a completed round = %d, want %d", got, want)
		}
	}
	requireLeaseIndexCard(t, rdb, 0)
}

// TestLeaseIndexPruneVerifyRestoresConcurrentRegistration pins the prune's
// verify step: a lease ZSET that appears after the orphan read but before the
// ZREM must be re-registered in the index, not lost with the orphan.
func TestLeaseIndexPruneVerifyRestoresConcurrentRegistration(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		zkey := leaseExpiryZSetKey(namespace.Default, "e1")
		if err := rdb.ZAdd(ctx, leaseExpiryExecIndexKey(namespace.Default),
			redis.Z{Score: 1, Member: "e1"}).Err(); err != nil {
			t.Fatal(err)
		}
		rdb.AddHook(&addOnLeaseIndexExistsPipeline{key: zkey, fn: func() {
			_ = rdb.ZAdd(context.Background(), zkey, redis.Z{Score: 4242, Member: "e1|n"}).Err()
		}})

		state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))

		if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != 4242 {
			t.Fatalf("index member = %v ok=%v, want restored at the exact minimum 4242", score, ok)
		}
	})
}
