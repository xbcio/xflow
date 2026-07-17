package rstate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

const defaultLeaseRepairBatch = 256

// reconcileLeaseIndexLua makes the lease expiry index agree with the current
// authoritative node state. The three keys share an execution hash tag, so a
// repair cannot re-add an index member after a concurrent terminal/revoke
// transition has removed the active lease.
//
// KEYS[1] = node status, KEYS[2] = node metadata, KEYS[3] = execution lease ZSET
// ARGV[1] = execution TTL seconds, ARGV[2] = execution-scoped ZSET member
// Returns 1 when a current running or committing lease was indexed, 0 when a
// stale or malformed lease was cleared from the index.
var reconcileLeaseIndexLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' and status ~= 'waiting' then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
if token == '' then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 0
end
local deadline = tonumber(redis.call('HGET', KEYS[2], 'lease_deadline_ms') or '0')
if deadline <= 0 then
    local issued = tonumber(redis.call('HGET', KEYS[2], 'lease_issued_at_ms') or '0')
    local ttl = tonumber(redis.call('HGET', KEYS[2], 'lease_ttl_ms') or '0')
    if issued > 0 and ttl > 0 then
        deadline = issued + ttl
        redis.call('HSET', KEYS[2], 'lease_deadline_ms', deadline)
    end
end
if deadline <= 0 then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 0
end
redis.call('ZADD', KEYS[3], deadline, ARGV[2])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[1]))
return 1
`)

// RepairLeaseIndex reconciles one bounded page of node status keys against
// the execution-scoped lease expiry indexes. It repairs a missing or
// mismatched ZSET member from the metadata deadline and removes members for
// terminal, tokenless, or malformed running state. The method is safe to call
// from multiple processes; each individual reconciliation is token/state
// checked atomically by Lua.
func (s *Store) RepairLeaseIndex(ctx context.Context, limit int) (reconciled int, err error) {
	started := time.Now()
	defer func() {
		s.observeLeaseRepair(reconciled, time.Since(started), err)
	}()

	if limit <= 0 {
		limit = defaultLeaseRepairBatch
	}

	s.leaseRepairMu.Lock()
	defer s.leaseRepairMu.Unlock()

	keys, next, err := s.rdb.Scan(ctx, s.leaseRepairCursor, "xflow:exec:{*}:node:*:status", int64(limit)).Result()
	if err != nil {
		return 0, fmt.Errorf("scan lease repair candidates: %w", err)
	}
	s.leaseRepairCursor = next

	reconciled = 0
	for _, statusKey := range keys {
		executionID, nodeName, ok := executionNodeFromStatusKey(statusKey)
		if !ok {
			continue
		}
		result, err := reconcileLeaseIndexLua.Run(ctx, s.rdb, []string{
			statusKey,
			nodeMetaKey(executionID, nodeName),
			leaseExpiryZSetKey(executionID),
		}, int(s.getExecTTL(executionID).Seconds()), leaseExpiryMember(executionID, nodeName)).Int64()
		if err != nil && err != redis.Nil {
			return reconciled, fmt.Errorf("reconcile lease index %q/%q: %w", executionID, nodeName, err)
		}
		if result == 1 {
			reconciled++
		}
	}
	return reconciled, nil
}

func executionNodeFromStatusKey(key string) (types.ExecutionID, string, bool) {
	const prefix = "xflow:exec:{"
	const nodeSeparator = "}:node:"
	const suffix = ":status"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	separator := strings.Index(rest, nodeSeparator)
	if separator <= 0 {
		return "", "", false
	}
	nodeName := strings.TrimSuffix(rest[separator+len(nodeSeparator):], suffix)
	if nodeName == "" {
		return "", "", false
	}
	return types.ExecutionID(rest[:separator]), nodeName, true
}
