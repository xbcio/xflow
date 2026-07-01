package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func newTestRedisState(t *testing.T) (*redisState, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	state := newRedisState(rdb, nil, time.Minute)
	return state, mr, rdb
}

func TestUpsertNodeIndexesLeaseAndUnsindexesOnTerminal(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Millisecond)
	snap := &engine.NodeSnapshot{
		ExecutionID:   "e1",
		Name:          "n",
		Status:        types.NodeStatusRunning,
		LeaseID:       "lease-1",
		LeaseToken:    "tok-1",
		LeaseIssuedAt: issued,
		LeaseTTL:      500 * time.Millisecond,
	}
	if err := state.UpsertNode(ctx, snap); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	// The lease is 1500ms past deadline; it should be visible in the index.
	got, err := rdb.ZScore(ctx, leaseExpiryZSetKey, leaseExpiryMember("e1", "n")).Result()
	if err != nil {
		t.Fatalf("ZScore() error = %v", err)
	}
	want := float64(issued.Add(500 * time.Millisecond).UnixMilli())
	if got != want {
		t.Fatalf("index score = %v, want %v", got, want)
	}

	// Roundtrip snapshot fields through GetNode.
	ns, err := state.GetNode(ctx, "e1", "n")
	if err != nil {
		t.Fatal(err)
	}
	if ns == nil || !ns.LeaseIssuedAt.Equal(issued) || ns.LeaseTTL != 500*time.Millisecond {
		t.Fatalf("roundtrip snap = %+v, want issued=%v ttl=500ms", ns, issued)
	}

	// A terminal upsert drops the index entry.
	term := *snap
	term.Status = types.NodeStatusSuccess
	if err := state.UpsertNode(ctx, &term); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.ZScore(ctx, leaseExpiryZSetKey, leaseExpiryMember("e1", "n")).Result(); err != redis.Nil {
		t.Fatalf("terminal upsert did not drop index entry: %v", err)
	}
}

func TestListExpiredLeasesReturnsOnlyPastDeadline(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Minute).UTC()
	future := time.Now().Add(time.Minute).UTC()

	mustUpsertRunning(t, state, "e1", "past", "tok-past", past, time.Second)
	mustUpsertRunning(t, state, "e1", "future", "tok-fut", future, time.Second)

	expired, err := state.ListExpiredLeases(ctx, time.Now())
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 || expired[0].NodeName != "past" {
		t.Fatalf("expired = %+v, want [past]", expired)
	}
	if expired[0].LeaseToken != "tok-past" {
		t.Fatalf("recovered token = %q, want tok-past", expired[0].LeaseToken)
	}
}

func TestRevokeLeaseAtomicallyRollsBackWhenTokenMatches(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-time.Minute).UTC()
	mustUpsertRunning(t, state, "e1", "n", "tok-1", issued, time.Second)

	ok, err := state.RevokeLease(ctx, "e1", "n", "tok-1")
	if err != nil || !ok {
		t.Fatalf("RevokeLease(matching) ok=%v err=%v", ok, err)
	}
	// Node should be back to pending, index cleared, token blanked.
	status, err := rdb.Get(ctx, nodeStatusKey("e1", "n")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if status != string(types.NodeStatusPending) {
		t.Fatalf("status after revoke = %q, want pending", status)
	}
	if _, err := rdb.ZScore(ctx, leaseExpiryZSetKey, leaseExpiryMember("e1", "n")).Result(); err != redis.Nil {
		t.Fatalf("index entry not cleared: %v", err)
	}
	tok, _ := rdb.HGet(ctx, nodeMetaKey("e1", "n"), "lease_token").Result()
	if tok != "" {
		t.Fatalf("lease_token = %q, want empty", tok)
	}
}

func TestRevokeLeaseLosesRaceToConcurrentCommit(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-time.Minute).UTC()
	mustUpsertRunning(t, state, "e1", "n", "tok-1", issued, time.Second)

	// Simulate a runner commit having already flipped status + cleared token.
	if err := rdb.Set(ctx, nodeStatusKey("e1", "n"), string(types.NodeStatusCommitting), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, nodeMetaKey("e1", "n"), "lease_token", "").Err(); err != nil {
		t.Fatal(err)
	}

	ok, err := state.RevokeLease(ctx, "e1", "n", "tok-1")
	if err != nil {
		t.Fatalf("RevokeLease() error = %v", err)
	}
	if ok {
		t.Fatal("RevokeLease succeeded despite the runner having committed")
	}
}

func TestListExpiredLeasesPrunesStaleIndexEntries(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()

	// Insert a bare ZSET member without a corresponding node status — simulates
	// a crashed / TTL'd node where the status expired but the index lingered.
	past := time.Now().Add(-time.Minute)
	if err := rdb.ZAdd(ctx, leaseExpiryZSetKey, redis.Z{
		Score: float64(past.UnixMilli()), Member: leaseExpiryMember("e1", "gone"),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	expired, err := state.ListExpiredLeases(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 0 {
		t.Fatalf("expired = %+v, want empty", expired)
	}
	// And the stale member should be pruned.
	if _, err := rdb.ZScore(ctx, leaseExpiryZSetKey, leaseExpiryMember("e1", "gone")).Result(); err != redis.Nil {
		t.Fatalf("stale index entry not pruned: %v", err)
	}
}

func mustUpsertRunning(t *testing.T, s *redisState, execID types.ExecutionID, name, token string, issued time.Time, ttl time.Duration) {
	t.Helper()
	if err := s.UpsertNode(context.Background(), &engine.NodeSnapshot{
		ExecutionID:   execID,
		Name:          name,
		Status:        types.NodeStatusRunning,
		LeaseID:       engine.LeaseID(token + "-id"),
		LeaseToken:    engine.LeaseToken(token),
		LeaseIssuedAt: issued.UTC(),
		LeaseTTL:      ttl,
	}); err != nil {
		t.Fatalf("upsert %q/%q: %v", execID, name, err)
	}
}
