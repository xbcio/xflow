package rstate

// Direct tests of reclaimOrphanExecutionLua, bypassing the Go pre-filter
// (evaluateLiveOrphan's executionProbe.ageAnchorMs and its cheap age check).
// The script is the atomic decision: whatever the Go side believes, the
// judgement that lands is the one the script makes. These cases pin the two
// guards that live ONLY in the script — the newer-clock age anchor and the
// graph-presence refusal — so deleting either cannot be masked by a Go-side
// check that would have refused the same shape first.
//
// KEYS/ARGV layout is the one documented above the script:
//
//	KEYS[1] status, [2] graph, [3] updated_at_ms, [4] lease-expiry ZSET,
//	[5] suspended_nodes SET, [6] error, [7] activity_at_ms
//	ARGV[1] now ms, [2] grace ms, [3] terminal reason, [4] status TTL seconds,
//	[5] force, [6] dry run, [7] include live

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// reclaimLuaFixture is one execution's keys against a real Lua engine
// (miniredis runs gopher-lua, which is also what the package's other Lua tests
// exercise).
type reclaimLuaFixture struct {
	ctx  context.Context
	rdb  *redis.Client
	id   types.ExecutionID
	keys []string
}

func newReclaimLuaFixture(t *testing.T) *reclaimLuaFixture {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	id := types.ExecutionID("lua-reclaim-orphan")
	return &reclaimLuaFixture{
		ctx: context.Background(),
		rdb: rdb,
		id:  id,
		keys: []string{
			execKey(namespace.Default, id, "status"),
			execKey(namespace.Default, id, "graph"),
			execKey(namespace.Default, id, "updated_at_ms"),
			leaseExpiryZSetKey(namespace.Default, id),
			suspendedNodesKey(namespace.Default, id),
			execKey(namespace.Default, id, "error"),
			activityAtKey(namespace.Default, id),
		},
	}
}

func (f *reclaimLuaFixture) set(t *testing.T, key string, value any) {
	t.Helper()
	if err := f.rdb.Set(f.ctx, key, value, 0).Err(); err != nil {
		t.Fatalf("SET %q error = %v", key, err)
	}
}

func (f *reclaimLuaFixture) status(t *testing.T) string {
	t.Helper()
	raw, err := f.rdb.Get(f.ctx, f.keys[0]).Result()
	if err != nil {
		t.Fatalf("read status error = %v", err)
	}
	return raw
}

// run applies the script exactly as the Go callers do, from the documented
// argument vector.
func (f *reclaimLuaFixture) run(t *testing.T, now time.Time, grace time.Duration, force, dry, live int64) []any {
	t.Helper()
	reply, err := reclaimOrphanExecutionLua.Run(f.ctx, f.rdb, f.keys,
		now.UnixMilli(), grace.Milliseconds(), engine.OrphanReclaimReason,
		3600, force, dry, live).Slice()
	if err != nil {
		t.Fatalf("reclaimOrphanExecutionLua error = %v", err)
	}
	return reply
}

// TestReclaimOrphanLuaPicksTheNewerAgeClock is the activity-anchor case with no
// Go pre-filter in front of it: the status transition is very old, the activity
// clock is fresh, and the execution has no graph. Sweep mode must refuse
// (outcome 0) because the newer clock is inside the grace; once activity also
// ages past the grace the same script converges.
//
// Mutation (a): replacing the newer-of anchor with updated_at_ms alone makes
// the first call converge, and this test fails.
func TestReclaimOrphanLuaPicksTheNewerAgeClock(t *testing.T) {
	f := newReclaimLuaFixture(t)
	now := time.Now().UTC()
	f.set(t, f.keys[0], string(types.ExecutionStatusRunning))
	f.set(t, f.keys[2], now.Add(-10*time.Minute).UnixMilli())
	f.set(t, f.keys[6], now.UnixMilli())

	reply := f.run(t, now, time.Minute, 0, 0, 0)
	if got := luaReplyInt(reply, 0); got != 0 {
		t.Fatalf("outcome with fresh activity = %d, want 0 (age anchor must be the newer clock); reply=%v", got, reply)
	}
	if got := f.status(t); got != string(types.ExecutionStatusRunning) {
		t.Fatalf("status after refusal = %q, want running", got)
	}

	f.set(t, f.keys[6], now.Add(-10*time.Minute).UnixMilli())
	reply = f.run(t, now, time.Minute, 0, 0, 0)
	if got := luaReplyInt(reply, 0); got != 1 {
		t.Fatalf("outcome with both clocks old = %d, want 1; reply=%v", got, reply)
	}
	if got := f.status(t); got != string(types.ExecutionStatusCanceled) {
		t.Fatalf("status after convergence = %q, want canceled", got)
	}
}

// TestReclaimOrphanLuaRefusesGraphPresent pins the script's own graph guard,
// again without the Go probe: an old status, no lease, no suspension, and a
// graph that exists. The default caller (include_live=0) must return 0 and
// leave the status untouched; include_live=1 is the operator override that
// lifts the guard and converges.
//
// Mutation (b): deleting the EXISTS-graph guard makes the first call converge,
// and this test fails.
func TestReclaimOrphanLuaRefusesGraphPresent(t *testing.T) {
	f := newReclaimLuaFixture(t)
	now := time.Now().UTC()
	f.set(t, f.keys[0], string(types.ExecutionStatusRunning))
	f.set(t, f.keys[1], "graph-present")
	f.set(t, f.keys[2], now.Add(-10*time.Minute).UnixMilli())

	reply := f.run(t, now, time.Minute, 0, 0, 0)
	if got := luaReplyInt(reply, 0); got != 0 {
		t.Fatalf("outcome with a graph present = %d, want 0 (default must refuse); reply=%v", got, reply)
	}
	if got := f.status(t); got != string(types.ExecutionStatusRunning) {
		t.Fatalf("status after refusal = %q, want running", got)
	}

	reply = f.run(t, now, time.Minute, 0, 0, 1)
	if got := luaReplyInt(reply, 0); got != 1 {
		t.Fatalf("outcome with include_live = %d, want 1; reply=%v", got, reply)
	}
	if got := f.status(t); got != string(types.ExecutionStatusCanceled) {
		t.Fatalf("status after include_live = %q, want canceled", got)
	}
}
