package timeout

// This package had zero test files before this one — `git ls-files` on this
// directory returns monitor.go and nothing else. Every test below
// runs against a real Redis instance because Monitor's own correctness
// (isTimeoutTargetDead, the ZSET peek/remove/requeue dance) is entirely about
// what is actually stored in Redis; a fake Cmdable would just re-describe the
// mock's behavior, not the monitor's.
//
// The success path — a delivered timeout that gets ZREM'd after
// engine.TimeoutNode returns nil (monitor.go:229-233) — is deliberately NOT
// covered here: reaching it needs a fully wired engine with an active graph
// and a suspended node's resume lock free, i.e. rstate.Store (Redis-backed
// StateStore, itself needing a store.Store SQL dependency per
// rstate/state.go:67) or an equivalent from-scratch graph fixture. That is a
// bigger investment than this sweep's scope; every test here instead drives
// engine.TimeoutNode into its one *deterministic* failure — a fresh
// *engine.Engine over an in-memory backend.local state store returns
// (nil, false, nil) for an unknown execution ID (memory_state.go:188-196),
// which engine/signal.go:162-164 turns into engine.ErrExecutionInactive — and
// asserts what Monitor does with that failure, which is the code path that
// actually branches on Redis state (isTimeoutTargetDead).

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// realRedisOrSkip connects to XFLOW_TEST_REDIS_ADDR (the podman test env
// documented in docs/TESTING.md), skipping cleanly when unset or unreachable
// so this file never silently no-ops in a way that reads as a pass.
func realRedisOrSkip(t *testing.T) redis.Cmdable {
	t.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis at %s unreachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// deadEngine returns an *engine.Engine whose TimeoutNode call always fails
// with engine.ErrExecutionInactive for any execution ID this test made up,
// because the in-memory backend never had that ID registered
// (memory_state.go:188-196 -> engine/signal.go:162-164). No queue consumer is
// started (Bind is never called): TimeoutNode never reaches Enqueue because
// loadActiveGraph already reports the execution inactive.
func deadEngine() *engine.Engine {
	b := local.New()
	return engine.New(b.State(), b.Queue())
}

func uniqueID(t *testing.T) types.ExecutionID {
	t.Helper()
	return types.ExecutionID(fmt.Sprintf("timeout-test-%s-%d", t.Name(), time.Now().UnixNano()))
}

// TestIsTimeoutTargetDead exercises the Redis-reading decision monitor.go's
// retry loop hinges on. It is a pure function of two Redis string keys, so
// this is a plain table-driven test with no engine involved.
func TestIsTimeoutTargetDead(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ns := namespace.Default
	m := New(rdb, nil, nil, nil, time.Minute)
	ctx := context.Background()

	cases := []struct {
		name       string
		execStatus types.ExecutionStatus // "" = do not set the key
		nodeStatus types.NodeStatus      // "" = do not set the key
		wantDead   bool
	}{
		{
			name:       "terminal execution status is dead",
			execStatus: types.ExecutionStatusFailed,
			wantDead:   true,
		},
		{
			name:       "running execution, no node status: not dead",
			execStatus: types.ExecutionStatusRunning,
			wantDead:   false,
		},
		{
			name:       "node still suspended, no exec status: NOT dead",
			nodeStatus: types.NodeStatusSuspended,
			wantDead:   false,
		},
		{
			name:       "node already resumed to success: dead",
			nodeStatus: types.NodeStatusSuccess,
			wantDead:   true,
		},
		{
			name:     "neither key set: not dead (default, keep retrying)",
			wantDead: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execID := uniqueID(t)
			nodeName := "node-under-test"

			execKey := rstate.ExecKey(ns, execID, "status")
			nodeKey := rstate.NodeStatusKey(ns, execID, nodeName)
			t.Cleanup(func() {
				_ = rdb.Del(context.Background(), execKey, nodeKey).Err()
			})

			if tc.execStatus != "" {
				if err := rdb.Set(ctx, execKey, string(tc.execStatus), time.Minute).Err(); err != nil {
					t.Fatalf("seed exec status: %v", err)
				}
			}
			if tc.nodeStatus != "" {
				if err := rdb.Set(ctx, nodeKey, string(tc.nodeStatus), time.Minute).Err(); err != nil {
					t.Fatalf("seed node status: %v", err)
				}
			}

			got := m.isTimeoutTargetDead(ctx, ns, execID, nodeName)
			if got != tc.wantDead {
				t.Fatalf("isTimeoutTargetDead() = %v, want %v", got, tc.wantDead)
			}
		})
	}
}

// TestProcessTimeoutKey_MalformedMemberRemoved proves a member that fails the
// "\x00"-split parse (monitor.go:177-182) is dropped from the ZSET rather than
// left to be re-peeked (and re-fail to parse) on every poll forever.
func TestProcessTimeoutKey_MalformedMemberRemoved(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ctx := context.Background()
	m := New(rdb, deadEngine(), nil, nil, time.Minute)

	key := fmt.Sprintf("test:timeout:malformed:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })

	member := "this-member-has-no-null-byte-separator"
	now := time.Now()
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(now.Add(-time.Minute).Unix()), Member: member}).Err(); err != nil {
		t.Fatalf("seed ZSET: %v", err)
	}

	nowUnix := fmt.Sprintf("%d", now.Unix())
	m.processTimeoutKey(ctx, namespace.Default, key, now, nowUnix)

	card, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	if card != 0 {
		t.Fatalf("ZCARD after processing malformed member = %d, want 0 (must be dropped, not retried forever)", card)
	}
}

// TestProcessTimeoutKey_TransientFailureRequeues proves that when
// engine.TimeoutNode fails and the target is NOT dead (the default: no
// terminal status recorded anywhere), the member stays in the ZSET — never
// dropped — with its score bumped by exactly timeoutRetryBackoff (monitor.go
// #8's at-least-once guarantee). Asserting the exact score, not just
// presence, catches a wrong backoff constant as well as a dropped member.
func TestProcessTimeoutKey_TransientFailureRequeues(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ctx := context.Background()
	m := New(rdb, deadEngine(), nil, nil, time.Minute)

	key := fmt.Sprintf("test:timeout:transient:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })

	execID := uniqueID(t)
	nodeName := "node-under-test"
	member := string(execID) + "\x00" + nodeName

	now := time.Now()
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(now.Add(-time.Minute).Unix()), Member: member}).Err(); err != nil {
		t.Fatalf("seed ZSET: %v", err)
	}

	nowUnix := fmt.Sprintf("%d", now.Unix())
	m.processTimeoutKey(ctx, namespace.Default, key, now, nowUnix)

	card, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	if card != 1 {
		t.Fatalf("ZCARD after transient failure = %d, want 1 (member must not be lost)", card)
	}

	gotScore, err := rdb.ZScore(ctx, key, member).Result()
	if err != nil {
		t.Fatalf("ZSCORE: %v", err)
	}
	wantScore := float64(now.Add(timeoutRetryBackoff).Unix())
	if gotScore != wantScore {
		t.Fatalf("ZSCORE after requeue = %v, want %v (now + timeoutRetryBackoff)", gotScore, wantScore)
	}
}

// TestProcessTimeoutKey_DeadTargetRemoved proves that when engine.TimeoutNode
// fails AND the execution has already reached a terminal status, the member
// is removed rather than requeued forever (monitor.go:206-213): a terminal
// execution's timeout would otherwise wedge in the ZSET permanently, since
// nothing will ever un-terminal it.
func TestProcessTimeoutKey_DeadTargetRemoved(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ctx := context.Background()
	ns := namespace.Default
	m := New(rdb, deadEngine(), nil, nil, time.Minute)

	execID := uniqueID(t)
	nodeName := "node-under-test"
	execKey := rstate.ExecKey(ns, execID, "status")
	t.Cleanup(func() { _ = rdb.Del(context.Background(), execKey).Err() })

	if err := rdb.Set(ctx, execKey, string(types.ExecutionStatusSuccess), time.Minute).Err(); err != nil {
		t.Fatalf("seed exec status: %v", err)
	}

	key := fmt.Sprintf("test:timeout:dead:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })

	member := string(execID) + "\x00" + nodeName
	now := time.Now()
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(now.Add(-time.Minute).Unix()), Member: member}).Err(); err != nil {
		t.Fatalf("seed ZSET: %v", err)
	}

	nowUnix := fmt.Sprintf("%d", now.Unix())
	m.processTimeoutKey(ctx, ns, key, now, nowUnix)

	card, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	if card != 0 {
		t.Fatalf("ZCARD after dead-target delivery failure = %d, want 0 (must not wedge a terminal execution's timeout forever)", card)
	}
}

// TestProcessTimeoutsForNamespace_ScopedToNamespace proves the SCAN pattern
// (timeoutKeyPattern, monitor.go:27) never crosses a namespace boundary:
// processing namespace A must not touch a key that belongs to namespace B,
// even though both keys live in the same Redis keyspace side by side. Uses
// malformed members (removed unconditionally, monitor.go:179-182) so the
// assertion isolates the SCAN/pattern behavior from the engine-call branch
// already covered by the other tests above.
func TestProcessTimeoutsForNamespace_ScopedToNamespace(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ctx := context.Background()
	m := New(rdb, deadEngine(), nil, nil, time.Minute)

	stamp := time.Now().UnixNano()
	nsA := namespace.Namespace(fmt.Sprintf("test-ns-a-%d", stamp))
	nsB := namespace.Namespace(fmt.Sprintf("test-ns-b-%d", stamp))

	keyA := fmt.Sprintf("xflow:ns:%s:exec:{exec-a-%d}:timeouts", nsA, stamp)
	keyB := fmt.Sprintf("xflow:ns:%s:exec:{exec-b-%d}:timeouts", nsB, stamp)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), keyA, keyB).Err() })

	now := time.Now()
	pastScore := float64(now.Add(-time.Minute).Unix())
	memberA := "malformed-a"
	memberB := "malformed-b"
	if err := rdb.ZAdd(ctx, keyA, redis.Z{Score: pastScore, Member: memberA}).Err(); err != nil {
		t.Fatalf("seed keyA: %v", err)
	}
	if err := rdb.ZAdd(ctx, keyB, redis.Z{Score: pastScore, Member: memberB}).Err(); err != nil {
		t.Fatalf("seed keyB: %v", err)
	}

	nowUnix := fmt.Sprintf("%d", now.Unix())
	m.processTimeoutsForNamespace(ctx, nsA, now, nowUnix)

	cardA, err := rdb.ZCard(ctx, keyA).Result()
	if err != nil {
		t.Fatalf("ZCARD keyA: %v", err)
	}
	if cardA != 0 {
		t.Fatalf("ZCARD keyA after processing namespace A = %d, want 0 (its own key must be scanned)", cardA)
	}

	cardB, err := rdb.ZCard(ctx, keyB).Result()
	if err != nil {
		t.Fatalf("ZCARD keyB: %v", err)
	}
	if cardB != 1 {
		t.Fatalf("ZCARD keyB after processing namespace A = %d, want 1 (namespace B's key must be untouched)", cardB)
	}
}
