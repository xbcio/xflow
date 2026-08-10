package control

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedisMetricsStoreForTest(t *testing.T) (*RedisMetricsStore, *redis.Client) {
	t.Helper()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	return NewRedisMetricsStore(rdb, DefaultMetricsRetention), rdb
}

func TestRedisMetricsStorePutThenListRoundTrips(t *testing.T) {
	store, _ := newRedisMetricsStoreForTest(t)
	ctx := context.Background()

	if err := store.Put(ctx, "runner-a", []byte("payload-a")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := store.Put(ctx, "runner-b", []byte("payload-b")); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	got, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d entries, want 2: %v", len(got), got)
	}
	if string(got["runner-a"]) != "payload-a" || string(got["runner-b"]) != "payload-b" {
		t.Errorf("payload mismatch: %q / %q", got["runner-a"], got["runner-b"])
	}
}

func TestRedisMetricsStorePutOverwritesPreviousReport(t *testing.T) {
	store, _ := newRedisMetricsStoreForTest(t)
	ctx := context.Background()

	for _, body := range []string{"old", "new"} {
		if err := store.Put(ctx, "runner-a", []byte(body)); err != nil {
			t.Fatalf("Put %s: %v", body, err)
		}
	}
	got, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if string(got["runner-a"]) != "new" {
		t.Errorf("retained %q, want the latest snapshot %q", got["runner-a"], "new")
	}
	if len(got) != 1 {
		t.Errorf("List returned %d entries, want 1 (a report replaces, never accumulates)", len(got))
	}
}

func TestRedisMetricsStoreExpiresPayload(t *testing.T) {
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	store := NewRedisMetricsStore(rdb, 90*time.Second)
	ctx := context.Background()

	if err := store.Put(ctx, "runner-a", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Prove the value existed before asserting it is gone: a store that never
	// wrote anything would otherwise pass this test.
	before, err := store.List(ctx)
	if err != nil || len(before) != 1 {
		t.Fatalf("precondition failed: List = %v, err = %v", before, err)
	}

	mr.FastForward(91 * time.Second)

	after, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List after expiry: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("List returned %d entries after retention elapsed, want 0", len(after))
	}
}

func TestRedisMetricsStoreListPrunesStaleIndexMembers(t *testing.T) {
	// The index SET has no TTL of its own, so an expired payload leaves a
	// dangling member. List must drop it AND clean it up, or the index grows
	// without bound across a fleet's lifetime of runner ids.
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	store := NewRedisMetricsStore(rdb, 90*time.Second)
	ctx := context.Background()

	if err := store.Put(ctx, "gone", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	mr.FastForward(91 * time.Second)
	if _, err := store.List(ctx); err != nil {
		t.Fatalf("List: %v", err)
	}

	members, err := rdb.SMembers(ctx, store.indexKey()).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("index still holds %v after the payload expired", members)
	}
}

func TestRedisMetricsStoreListIsEmptyWhenNothingReported(t *testing.T) {
	store, _ := newRedisMetricsStoreForTest(t)
	got, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List on empty store: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List = %v, want empty", got)
	}
}

func TestRedisMetricsStoreSharesDataAcrossInstances(t *testing.T) {
	// Two RedisMetricsStore values over one Redis stand in for two server
	// replicas: this is the entire reason the store is not a process-local map.
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	replicaA := NewRedisMetricsStore(rdb, DefaultMetricsRetention)
	replicaB := NewRedisMetricsStore(rdb, DefaultMetricsRetention)
	ctx := context.Background()

	if err := replicaA.Put(ctx, "runner-a", []byte("payload")); err != nil {
		t.Fatalf("Put on replica A: %v", err)
	}
	got, err := replicaB.List(ctx)
	if err != nil {
		t.Fatalf("List on replica B: %v", err)
	}
	if string(got["runner-a"]) != "payload" {
		t.Errorf("replica B saw %q, want the payload replica A accepted", got["runner-a"])
	}
}
