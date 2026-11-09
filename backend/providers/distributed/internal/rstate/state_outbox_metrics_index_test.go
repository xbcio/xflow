package rstate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

type scanCountHook struct {
	mu     sync.Mutex
	n      int
	dbsize int
	// pages > 0 makes every SCAN take that many round trips: cursors 0..pages-1
	// return an empty page, and the last one runs the real SCAN. miniredis
	// ignores COUNT, so this is how tests observe page-by-page walks.
	pages int
}

func (h *scanCountHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *scanCountHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.note(cmd)
		if sc, ok := cmd.(*redis.ScanCmd); ok && h.pages > 0 && strings.EqualFold(cmd.Name(), "scan") {
			var cursor uint64
			fmt.Sscan(fmt.Sprint(cmd.Args()[1]), &cursor)
			if cursor < uint64(h.pages) {
				sc.SetVal(nil, cursor+1)
				return nil
			}
			cmd.Args()[1] = uint64(0)
		}
		return next(ctx, cmd)
	}
}

func (h *scanCountHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.note(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *scanCountHook) note(cmd redis.Cmder) {
	if strings.EqualFold(cmd.Name(), "scan") {
		h.mu.Lock()
		h.n++
		h.mu.Unlock()
	}
	if strings.EqualFold(cmd.Name(), "dbsize") {
		h.mu.Lock()
		h.dbsize++
		h.mu.Unlock()
	}
}

func (h *scanCountHook) takeDBSize() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.dbsize
	h.dbsize = 0
	return n
}

func (h *scanCountHook) take() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.n
	h.n = 0
	return n
}

func metricsEntry(id types.ExecutionID, n int) engine.OutboxEntry {
	return engine.OutboxEntry{
		ID:        fmt.Sprintf("root/%s/n%d/0", id, n),
		Task:      engine.Task{ExecutionID: id, NodeName: fmt.Sprintf("n%d", n), NodeIdx: n, Type: engine.TaskTypeNodeResume},
		CreatedAt: time.Now().UTC().Add(-time.Duration(n+1) * time.Minute),
	}
}

func createMetricsExecution(t *testing.T, s *Store, id types.ExecutionID, entries ...engine.OutboxEntry) {
	t.Helper()
	if err := s.CreateExecutionWithOutbox(context.Background(),
		&engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, entries); err != nil {
		t.Fatalf("CreateExecutionWithOutbox(%s) error = %v", id, err)
	}
}

// metricsFixture builds ready, leased, future-scored and dead executions and
// returns the snapshot the old keyspace scan would report for it.
func metricsFixture(t *testing.T, s *Store) engine.OutboxMetricsSnapshot {
	t.Helper()
	ctx := context.Background()
	createMetricsExecution(t, s, "m-ready", metricsEntry("m-ready", 0), metricsEntry("m-ready", 1))
	createMetricsExecution(t, s, "m-leased", metricsEntry("m-leased", 0))
	if got, err := s.LeaseOutbox(ctx, "m-leased", time.Now(), 1); err != nil || len(got) != 1 {
		t.Fatalf("LeaseOutbox leased = %v err=%v", got, err)
	}
	createMetricsExecution(t, s, "m-future", metricsEntry("m-future", 0))
	future := metricsEntry("m-future", 0)
	if _, err := s.rdb.ZAdd(ctx, outboxReadyKey(namespace.FromContext(ctx), "m-future"),
		redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: future.ID}).Result(); err != nil {
		t.Fatal(err)
	}
	createMetricsExecution(t, s, "m-dead", metricsEntry("m-dead", 0))
	if f, err := s.RecordOutboxFailure(ctx, "m-dead", metricsEntry("m-dead", 0), 1); err != nil || !f.DeadLettered {
		t.Fatalf("RecordOutboxFailure = %+v err=%v", f, err)
	}
	// 3 pending (2 ready + leased, none due-but-leased) and 1 more future.
	return engine.OutboxMetricsSnapshot{Pending: 4, Ready: 2, DeadLettered: 1}
}

func TestOutboxMetricsIndexMatchesKeyspaceScan(t *testing.T) {
	s, _, _ := newTestRedisState(t)
	ctx := context.Background()
	want := metricsFixture(t, s)

	s.ConfigureOutboxReadyIndex(false) // old ready scan path
	old, err := s.OutboxMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.ConfigureOutboxReadyIndex(true)
	got, err := s.OutboxMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pending != want.Pending || got.Ready != want.Ready || got.DeadLettered != want.DeadLettered {
		t.Fatalf("indexed metrics = %+v, want %+v", got, want)
	}
	if got != old {
		t.Fatalf("indexed metrics = %+v, old scan path = %+v", got, old)
	}
	if got.OldestPendingAt.IsZero() {
		t.Fatalf("OldestPendingAt is zero, want the oldest created entry")
	}
}

func TestRecordOutboxFailureRegistersDeadIndex(t *testing.T) {
	s, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	nsp := namespace.FromContext(ctx)
	createMetricsExecution(t, s, "d1", metricsEntry("d1", 0))
	if _, err := s.RecordOutboxFailure(ctx, "d1", metricsEntry("d1", 0), 3); err != nil {
		t.Fatal(err)
	}
	if rdb.SIsMember(ctx, outboxDeadIndexKey(nsp), "d1").Val() {
		t.Fatalf("retained failure must not register the dead index")
	}
	if _, err := s.RecordOutboxFailure(ctx, "d1", metricsEntry("d1", 0), 1); err != nil {
		t.Fatal(err)
	}
	if !rdb.SIsMember(ctx, outboxDeadIndexKey(nsp), "d1").Val() {
		t.Fatalf("dead-lettered execution missing from dead index")
	}
	if strings.Contains(outboxDeadIndexKey(nsp), ":exec:{") {
		t.Fatalf("dead index key must not match execScanPattern")
	}
}

func TestOutboxMetricsPrunesEmptyDeadIndexWithVerify(t *testing.T) {
	s, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	nsp := namespace.FromContext(ctx)
	idx := outboxDeadIndexKey(nsp)
	createMetricsExecution(t, s, "p1", metricsEntry("p1", 0))
	rdb.SAdd(ctx, idx, "p1", "ghost")
	if _, err := s.OutboxMetrics(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rdb.SCard(ctx, idx).Val(); n != 0 {
		t.Fatalf("dead index size = %d after prune, want 0", n)
	}
	// Verify step: a dead letter present at verify time restores the member.
	rdb.ZAdd(ctx, outboxDeadKey(nsp, "p1"), redis.Z{Score: 1, Member: "e"})
	s.pruneOutboxDeadIndex(ctx, nsp, "p1")
	if !rdb.SIsMember(ctx, idx, "p1").Val() {
		t.Fatalf("prune must re-register a member whose dead ZSET is non-empty")
	}
}

func TestOutboxMetricsWalkRecoversLostRegistration(t *testing.T) {
	s, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	nsp := namespace.FromContext(ctx)
	want := metricsFixture(t, s)
	rdb.Del(ctx, outboxReadyIndexKey(nsp), outboxDeadIndexKey(nsp))

	// Backfill: the first call walks the whole keyspace and registers.
	got, err := s.OutboxMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pending != want.Pending || got.DeadLettered != want.DeadLettered {
		t.Fatalf("after backfill = %+v, want %+v", got, want)
	}

	// Throttled phase: lose the registrations again; the walk is due after a
	// round has elapsed.
	rdb.Del(ctx, outboxReadyIndexKey(nsp), outboxDeadIndexKey(nsp))
	got, err = s.OutboxMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pending != 0 || got.DeadLettered != 0 {
		t.Fatalf("throttled walk ran too early: %+v", got)
	}
	s.outboxMetricsWalkMu.Lock()
	s.outboxMetricsWalks[nsp].last = time.Now().Add(-outboxMetricsWalkRound)
	s.outboxMetricsWalkMu.Unlock()
	got, err = s.OutboxMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pending != want.Pending || got.DeadLettered != want.DeadLettered {
		t.Fatalf("after throttled walk = %+v, want %+v", got, want)
	}
}

func TestOutboxMetricsIssuesNoScanAfterBackfill(t *testing.T) {
	s, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	metricsFixture(t, s)
	for i := 0; i < 3000; i++ {
		rdb.Set(ctx, fmt.Sprintf("unrelated:%d", i), i, 0)
	}
	hook := &scanCountHook{}
	rdb.AddHook(hook)

	if _, err := s.OutboxMetrics(ctx); err != nil {
		t.Fatal(err)
	}
	if hook.take() == 0 {
		t.Fatalf("backfill call issued no SCAN; the hook is not observing")
	}
	if _, err := s.OutboxMetrics(ctx); err != nil {
		t.Fatal(err)
	}
	if n := hook.take(); n != 0 {
		t.Fatalf("OutboxMetrics after backfill issued %d SCAN commands, want 0", n)
	}
}
