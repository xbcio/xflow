package rstate

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

func miniredisOrphanBackend(t *testing.T) statestoretest.ConcurrencyOrphanBackend {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rstateOrphanBackend(t, rdb, memstore.New())
}

func rstateOrphanBackend(t *testing.T, rdb redis.UniversalClient, mirror *memstore.Store) statestoretest.ConcurrencyOrphanBackend {
	t.Helper()
	ctx := context.Background()
	state := New(rdb, mirror, time.Minute)
	return statestoretest.ConcurrencyOrphanBackend{
		Store:  state,
		Mirror: mirror,
		LoseGraph: func(t *testing.T, id types.ExecutionID) {
			t.Helper()
			if err := rdb.Del(ctx, execKey(namespace.Default, id, "graph")).Err(); err != nil {
				t.Fatalf("delete graph for %q: %v", id, err)
			}
		},
		AgeStatus: func(t *testing.T, id types.ExecutionID, age time.Duration) {
			t.Helper()
			key := execKey(namespace.Default, id, "updated_at_ms")
			if err := rdb.Set(ctx, key, time.Now().UTC().Add(-age).UnixMilli(), 0).Err(); err != nil {
				t.Fatalf("age status for %q: %v", id, err)
			}
		},
		RecordActivity: func(t *testing.T, id types.ExecutionID) {
			t.Helper()
			if err := rdb.Set(ctx, activityAtKey(namespace.Default, id), time.Now().UTC().UnixMilli(), 0).Err(); err != nil {
				t.Fatalf("record activity for %q: %v", id, err)
			}
		},
		AgeActivity: func(t *testing.T, id types.ExecutionID, age time.Duration) {
			t.Helper()
			if err := rdb.Set(ctx, activityAtKey(namespace.Default, id), time.Now().UTC().Add(-age).UnixMilli(), 0).Err(); err != nil {
				t.Fatalf("age activity for %q: %v", id, err)
			}
		},
		ForgetActivity: func(t *testing.T, id types.ExecutionID) {
			t.Helper()
			if err := rdb.Del(ctx, activityAtKey(namespace.Default, id)).Err(); err != nil {
				t.Fatalf("forget activity for %q: %v", id, err)
			}
		},
		ForgetExecution: func(t *testing.T, id types.ExecutionID) {
			t.Helper()
			if err := rdb.Del(ctx,
				execKey(namespace.Default, id, "status"),
				execKey(namespace.Default, id, "graph"),
				execKey(namespace.Default, id, "updated_at_ms"),
				execKey(namespace.Default, id, "workflow_id"),
				activityAtKey(namespace.Default, id),
			).Err(); err != nil {
				t.Fatalf("forget execution %q: %v", id, err)
			}
		},
		TerminalStatusWithoutRelease: func(t *testing.T, id types.ExecutionID) {
			t.Helper()
			// The Lua half of a reclaim writes exactly this status and stops;
			// the membership and the durable mirror are untouched, which is the
			// crash residue the reconcile pass must repair.
			if err := rdb.Set(ctx, execKey(namespace.Default, id, "status"),
				string(types.ExecutionStatusCanceled), 0).Err(); err != nil {
				t.Fatalf("write terminal residue for %q: %v", id, err)
			}
		},
		ReconcileSlots: func(t *testing.T) {
			t.Helper()
			if _, err := state.ReconcileConcurrencySlots(ctx, 64); err != nil {
				t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
			}
		},
	}
}

// TestMiniredisConcurrencyOrphanContract runs the shared orphan contract
// against the Lua backend. miniredis runs a different Lua engine than Redis, so
// the real-server variant below is not redundant: the atomic reclaim script is
// the part under test.
func TestMiniredisConcurrencyOrphanContract(t *testing.T) {
	statestoretest.RunConcurrencyOrphanContract(t, rstateCapabilityManifest(), miniredisOrphanBackend)
}

func TestRedisConcurrencyOrphanContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunConcurrencyOrphanContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ConcurrencyOrphanBackend {
		return rstateOrphanBackend(t, freshRealRedis(t, addr), memstore.New())
	})
}

// TestRedisClusterConcurrencyOrphanContract runs the same contract against a
// real Redis Cluster when XFLOW_TEST_REDIS_CLUSTER_ADDRS is set. Neither
// miniredis nor a single-node server enforces hash slots, so they accept a
// reclaim script that names keys from different slots. This variant is what
// proves the activity_at_ms key really shares the execution's {id} tag: a
// CROSSSLOT regression fails here and nowhere else.
func TestRedisClusterConcurrencyOrphanContract(t *testing.T) {
	raw := os.Getenv("XFLOW_TEST_REDIS_CLUSTER_ADDRS")
	if raw == "" {
		t.Skip("XFLOW_TEST_REDIS_CLUSTER_ADDRS not set; skipping real Redis Cluster orphan contract")
	}
	addrs := make([]string, 0, 3)
	for _, addr := range strings.Split(raw, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		t.Fatalf("XFLOW_TEST_REDIS_CLUSTER_ADDRS=%q contains no addresses", raw)
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis cluster unavailable at %v: %v", addrs, err)
	}
	flush := func() {
		if err := rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		}); err != nil {
			t.Fatalf("flush cluster databases: %v", err)
		}
	}
	flush()
	t.Cleanup(flush)
	statestoretest.RunConcurrencyOrphanContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ConcurrencyOrphanBackend {
		return rstateOrphanBackend(t, rdb, memstore.New())
	})
}
