package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file holds the timeout index: a namespace-wide ZSET naming every
// execution that may hold a node timeout, so the timeout monitor reads due
// executions instead of walking the keyspace for `exec:{*}:timeouts` on every
// tick (measured at 46% of all Redis command time at 2.35M keys).
//
// It follows the outbox readiness-index protocol (state_outbox_index.go; read
// its file note for the full argument). In short:
//
//   - The per-execution timeout ZSET stays the single source of truth. The
//     index is a best-effort accelerator written by ordinary single-key
//     commands AFTER the transition, because it carries no `{id}` hash tag and
//     a script touching it would be CROSSSLOT in Redis Cluster.
//   - Registration scores the member at "now", a LOWER bound on the earliest
//     timeout, so it needs no read and can never hide a timeout behind the
//     read cutoff. A plain ZADD with the exact deadline would be wrong: ZADD
//     overwrites, so registering a later timeout would push back an earlier
//     one of another node of the same execution (ZADD LT needs Redis 6.2).
//   - The monitor re-arms a member to the exact minimum, or prunes it, after
//     processing; both verify by re-reading the per-execution ZSET
//     (RefreshTimeoutIndex).
//   - A lost registration (process death between write and ZADD, data written
//     before upgrade) is recovered by the monitor's throttled keyspace walk.
//
// Index scores are unix MILLISECONDS; per-execution timeout scores are unix
// SECONDS (see the suspend paths). Convert at the boundary.
//
// Add paths that register: SuspendOrConsume (suspendOrConsumeLua parks with
// a timeout), suspendOrConsumeMulti (park pipeline), ResuspendAtomic, and the
// monitor's own retry requeue (it refreshes afterwards). Signal delivery,
// cancel and cleanup only remove members; a stale member left by them costs
// one no-op read that prunes it.

// TimeoutIndexKey returns the namespace's timeout index. It deliberately does
// not match execScanPattern (no `exec:{`), so no keyspace walk mistakes it for
// an execution key.
func TimeoutIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:timeout-index", t)
}

// TimeoutZSetKey returns one execution's timeout ZSET (scores in unix seconds).
func TimeoutZSetKey(t namespace.Namespace, id types.ExecutionID) string {
	return timeoutZSetKey(t, id)
}

// TimeoutScanPattern is the keyspace glob of every per-execution timeout ZSET
// in a namespace, for the monitor's safety-net walk.
func TimeoutScanPattern(t namespace.Namespace) string {
	return execScanPattern(t, "timeouts")
}

// TimeoutExecutionID extracts the execution ID from a per-execution timeout
// key of namespace t; ok is false for any other key.
func TimeoutExecutionID(t namespace.Namespace, key string) (types.ExecutionID, bool) {
	prefix := fmt.Sprintf("xflow:ns:%s:exec:{", t)
	const suffix = "}:timeouts"
	if len(key) <= len(prefix)+len(suffix) || key[:len(prefix)] != prefix || key[len(key)-len(suffix):] != suffix {
		return "", false
	}
	return types.ExecutionID(key[len(prefix) : len(key)-len(suffix)]), true
}

// MarkTimeoutIndex registers an execution in the timeout index at now (ms), a
// lower bound on its earliest timeout.
func MarkTimeoutIndex(ctx context.Context, rdb redis.Cmdable, t namespace.Namespace, id types.ExecutionID) error {
	return rdb.ZAdd(ctx, TimeoutIndexKey(t), redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: string(id),
	}).Err()
}

// DueTimeoutExecutions returns up to limit executions whose index score is at
// or before cutoff. ZRANGEBYSCORE rather than ZRangeArgs keeps Redis 5.0
// compatibility (see state_lease.go).
func DueTimeoutExecutions(ctx context.Context, rdb redis.Cmdable, t namespace.Namespace, cutoff time.Time, limit int64) ([]types.ExecutionID, error) {
	members, err := rdb.ZRangeByScore(ctx, TimeoutIndexKey(t), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", cutoff.UnixMilli()),
		Count: limit,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read timeout index: %w", err)
	}
	out := make([]types.ExecutionID, len(members))
	for i, m := range members {
		out[i] = types.ExecutionID(m)
	}
	return out, nil
}

// RefreshTimeoutIndex makes the index agree with one execution's timeout ZSET:
// the exact earliest timeout, or no member when it has none.
//
// Re-arm verify: a producer that adds a timeout and registers "now" between
// our read and our ZADD is clobbered by our (later) exact score. The second
// read is ordered after our ZADD, so it sees that producer's member; if it is
// earlier we ZADD again. A producer writing after the second read also
// registers after our ZADD, so its score survives.
//
// Prune verify: same argument for ZREM — re-read after removing and re-add
// when the ZSET is no longer empty.
func RefreshTimeoutIndex(ctx context.Context, rdb redis.Cmdable, t namespace.Namespace, id types.ExecutionID) error {
	indexKey := TimeoutIndexKey(t)
	minSec, ok, err := earliestTimeoutScore(ctx, rdb, t, id)
	if err != nil {
		return err
	}
	if ok {
		if err := rdb.ZAdd(ctx, indexKey, redis.Z{Score: minSec * 1000, Member: string(id)}).Err(); err != nil {
			return fmt.Errorf("re-arm timeout index %q: %w", id, err)
		}
		again, ok, err := earliestTimeoutScore(ctx, rdb, t, id)
		if err != nil {
			return err
		}
		if ok && again < minSec {
			if err := rdb.ZAdd(ctx, indexKey, redis.Z{Score: again * 1000, Member: string(id)}).Err(); err != nil {
				return fmt.Errorf("re-arm timeout index %q: %w", id, err)
			}
		}
		return nil
	}
	if err := rdb.ZRem(ctx, indexKey, string(id)).Err(); err != nil {
		return fmt.Errorf("prune timeout index %q: %w", id, err)
	}
	again, ok, err := earliestTimeoutScore(ctx, rdb, t, id)
	if err != nil {
		return err
	}
	if ok {
		if err := rdb.ZAdd(ctx, indexKey, redis.Z{Score: again * 1000, Member: string(id)}).Err(); err != nil {
			return fmt.Errorf("repair timeout index %q: %w", id, err)
		}
	}
	return nil
}

func earliestTimeoutScore(ctx context.Context, rdb redis.Cmdable, t namespace.Namespace, id types.ExecutionID) (float64, bool, error) {
	scored, err := rdb.ZRangeWithScores(ctx, timeoutZSetKey(t, id), 0, 0).Result()
	if err != nil && err != redis.Nil {
		return 0, false, fmt.Errorf("read timeouts for %q: %w", id, err)
	}
	if len(scored) == 0 {
		return 0, false, nil
	}
	return scored[0].Score, true, nil
}

// markTimeoutIndex is the store-side registration after a write that added a
// timeout member. Best-effort: the transition is applied, so failure is
// logged, never returned, and the monitor's walk recovers it. Detached from
// the caller's cancellation for the reason detachedIndexContext gives.
func (s *Store) markTimeoutIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	if err := MarkTimeoutIndex(ctx, s.rdb, t, id); err != nil && s.logger != nil {
		s.logger.Warn("timeout index registration failed; the monitor's keyspace walk recovers it",
			"execution_id", string(id), "err", err)
	}
}
