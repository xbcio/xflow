package rstate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// TestMiniredisEntryActivationContract runs the shared EntryActivationStore
// contract against the Redis/Lua backend using miniredis (no external deps).
func TestMiniredisEntryActivationContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunEntryActivationContract(t, func(t *testing.T) engine.EntryActivationStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return NewEntryActivationStore(rdb, time.Minute)
	})
}

// TestRedisEntryActivationContract runs the shared EntryActivationStore contract
// against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisEntryActivationContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	// Skip cleanly when the addr is set but the server is unreachable (the
	// podman env may be down) rather than failing every subtest.
	probe := redis.NewClient(&redis.Options{Addr: addr})
	pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := probe.Ping(pctx).Err(); err != nil {
		_ = probe.Close()
		t.Skipf("redis at %s unreachable: %v", addr, err)
	}
	_ = probe.Close()
	statestoretest.RunEntryActivationContract(t, func(t *testing.T) engine.EntryActivationStore {
		return NewEntryActivationStore(freshRealRedis(t, addr), time.Minute)
	})
}

func TestEntryActivationRedisKeyReplicaCompatibility(t *testing.T) {
	ns := namespace.Namespace("tenant-a")
	if got, want := entryActivationRedisKey(ns, "wf-a", "v7", "kafka-group", 0), "xflow:ns:tenant-a:entryact:{wf-a|v7|kafka-group}"; got != want {
		t.Fatalf("replica zero key = %q, want legacy key %q", got, want)
	}
	if got, want := entryActivationRedisKey(ns, "wf-a", "v7", "kafka-group", 1), "xflow:ns:tenant-a:entryact:{wf-a|v7|kafka-group}:replica:1"; got != want {
		t.Fatalf("replica one key = %q, want %q", got, want)
	}
	if got, want := entryActivationScanPattern(ns), "xflow:ns:tenant-a:entryact:{*}*"; got != want {
		t.Fatalf("scan pattern = %q, want %q", got, want)
	}
}

func TestEntryActivationStoreReadsLegacyReplicaZeroHash(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ns := namespace.Namespace("legacy-ns")
	legacyKey := "xflow:ns:legacy-ns:entryact:{wf-legacy|v1|kafka-entry}"
	// This is the pre-replica hash shape: its key has no suffix and its fields
	// have no replica_index. Upgraded readers must decode it as replica zero and
	// the namespace scan must continue to discover it.
	if err := rdb.HSet(ctx, legacyKey,
		"namespace", string(ns),
		"workflow_id", "wf-legacy",
		"workflow_version", "v1",
		"entry_unit_id", "kafka-entry",
		"node_type", "kafka.source",
		"package_hash", "legacy-package",
		"desired", "1",
		"generation", "4",
	).Err(); err != nil {
		t.Fatalf("write legacy hash: %v", err)
	}

	store := NewEntryActivationStore(rdb, time.Minute)
	key := engine.EntryActivationKey{
		Namespace:       ns,
		WorkflowID:      "wf-legacy",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-entry",
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get legacy activation: ok=%v err=%v", ok, err)
	}
	if got.ReplicaIndex != 0 || got.PackageHash != "legacy-package" || got.Generation != 4 || !got.Desired {
		t.Fatalf("legacy activation decoded incorrectly: %+v", got)
	}

	list, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List legacy namespace: %v", err)
	}
	if len(list) != 1 || list[0].ReplicaIndex != 0 || list[0].EntryUnitID != "kafka-entry" {
		t.Fatalf("scan did not preserve legacy activation: %+v", list)
	}
}
