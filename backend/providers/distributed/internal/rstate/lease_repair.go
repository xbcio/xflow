package rstate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
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
		s.observeLeaseRepair(ctx, reconciled, time.Since(started), err)
	}()

	if limit <= 0 {
		limit = defaultLeaseRepairBatch
	}

	s.leaseRepairMu.Lock()
	defer s.leaseRepairMu.Unlock()

	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return 0, fmt.Errorf("list namespaces for lease repair: %w", err)
	}
	reconciled = 0
	for _, t := range namespaces {
		if reconciled >= limit {
			break
		}
		n, err := s.repairLeaseIndexForNamespace(ctx, t, limit-reconciled)
		if err != nil {
			return reconciled, err
		}
		reconciled += n
	}
	return reconciled, nil
}

func (s *Store) repairLeaseIndexForNamespace(ctx context.Context, t namespace.Namespace, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	cursor := s.leaseRepairCursor[t]
	keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "node:*:status"), int64(limit)).Result()
	if err != nil {
		return 0, fmt.Errorf("scan lease repair candidates: %w", err)
	}
	s.leaseRepairCursor[t] = next

	reconciled := 0
	for _, statusKey := range keys {
		executionID, nodeName, ok := executionNodeFromStatusKey(statusKey)
		if !ok {
			continue
		}
		result, err := reconcileLeaseIndexLua.Run(ctx, s.rdb, []string{
			statusKey,
			nodeMetaKey(t, executionID, nodeName),
			leaseExpiryZSetKey(t, executionID),
		}, int(s.getExecTTL(ctx, executionID).Seconds()), leaseExpiryMember(executionID, nodeName)).Int64()
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
	const nodeSeparator = "}:node:"
	const suffix = ":status"
	if !strings.HasSuffix(key, suffix) {
		return "", "", false
	}
	// Strip the trailing :status, then locate the }:node: separator that ends
	// the execution id hash tag. The key shape is
	//xflow:ns:<namespace>:exec:{<id>}:node:<name>:status; the namespace prefix is
	// not needed here, only the execution id and node name.
	rest := strings.TrimSuffix(key, suffix)
	sepIdx := strings.Index(rest, nodeSeparator)
	if sepIdx <= 0 {
		return "", "", false
	}
	nodeName := rest[sepIdx+len(nodeSeparator):]
	if nodeName == "" {
		return "", "", false
	}
	// The execution id is the substring between '{' and '}' immediately before
	// the }:node: separator. The '}' is the first char of "}:node:", so the id
	// ends at sepIdx.
	braceOpen := strings.LastIndex(rest[:sepIdx], "{")
	braceClose := sepIdx // rest[sepIdx] == '}'
	if braceOpen < 0 || rest[braceClose] != '}' {
		return "", "", false
	}
	execID := rest[braceOpen+1 : braceClose]
	if execID == "" {
		return "", "", false
	}
	return types.ExecutionID(execID), nodeName, true
}
