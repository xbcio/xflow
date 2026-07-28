package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func expansionSubExecutionKey(t namespace.Namespace, id types.ExecutionID, nodeName string, leaseID engine.LeaseID) string {
	return execKey(t, id, fmt.Sprintf("node:%s:expansion:%s:subs", nodeName, leaseID))
}

// beginTaskExpansionLua converts a claimed parent into waiting without
// discarding its active lease. The unchanged expiry index lets the standard
// sweeper reclaim an expansion whose process dies before its batches finish.
var beginTaskExpansionLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'committing' then
    return 0
end
local leaseID = redis.call('HGET', KEYS[2], 'lease_id') or ''
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local activation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
if leaseID ~= ARGV[1] or token ~= ARGV[2] or attempt ~= tonumber(ARGV[3]) or activation ~= tonumber(ARGV[4]) then
    return 0
end
redis.call('SET', KEYS[1], 'waiting', 'EX', tonumber(ARGV[5]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[5]))
return 1
`)

// beginTaskExpansionWithOutboxLua persists the full expansion handoff in one
// execution-scoped Redis transaction: parent Waiting state, generation-scoped
// child records, and every queue-delivery intent become visible together.
var beginTaskExpansionWithOutboxLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'committing' then
    return 0
end
local leaseID = redis.call('HGET', KEYS[2], 'lease_id') or ''
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local activation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
if leaseID ~= ARGV[1] or token ~= ARGV[2] or attempt ~= tonumber(ARGV[3]) or activation ~= tonumber(ARGV[4]) then
    return 0
end
local ttl = tonumber(ARGV[5])
redis.call('SET', KEYS[1], 'waiting', 'EX', ttl)
redis.call('EXPIRE', KEYS[2], ttl)
local count = tonumber(ARGV[6])
local position = 7
for i = 1, count do
    local childID = ARGV[position]
    local childBody = ARGV[position + 1]
    local outboxID = ARGV[position + 2]
    local outboxBody = ARGV[position + 3]
    local availableAt = tonumber(ARGV[position + 4])
    redis.call('HSETNX', KEYS[3], childID, childBody)
    if redis.call('HSETNX', KEYS[5], outboxID, outboxBody) == 1 then
        redis.call('ZADD', KEYS[4], availableAt, outboxID)
    end
    position = position + 5
end
redis.call('EXPIRE', KEYS[3], ttl)
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)
return 1
`)

// createExpandedSubExecutionLua writes a child only while its exact parent
// generation is still waiting. HSETNX makes replay after a response loss
// idempotent without letting a stale batch replace a current child.
var createExpandedSubExecutionLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'waiting' then
    return 0
end
local leaseID = redis.call('HGET', KEYS[2], 'lease_id') or ''
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local activation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
if leaseID ~= ARGV[1] or token ~= ARGV[2] or attempt ~= tonumber(ARGV[3]) or activation ~= tonumber(ARGV[4]) then
    return 0
end
redis.call('HSETNX', KEYS[3], ARGV[5], ARGV[6])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
return 1
`)

// completeExpandedSubExecutionLua applies a batch result only to its current
// parent lease generation. It returns {accepted, all_done}; terminalization is
// separately token-fenced by CommitNode so recovery may safely win after this
// script returns.
var completeExpandedSubExecutionLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'waiting' then
    return {0, 0}
end
local leaseID = redis.call('HGET', KEYS[2], 'lease_id') or ''
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local activation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
if leaseID ~= ARGV[1] or token ~= ARGV[2] or attempt ~= tonumber(ARGV[3]) or activation ~= tonumber(ARGV[4]) then
    return {0, 0}
end
local raw = redis.call('HGET', KEYS[3], ARGV[5])
if not raw then
    return {0, 0}
end
local child = cjson.decode(raw)
if child.status == 'running' then
    child.status = ARGV[6]
    -- Splice the raw result JSON (ARGV[7]) into the child body verbatim instead
    -- of cjson.decode/encode round-tripping it. cjson mutates empty objects
    -- {} -> [] and loses int64 precision beyond 2^53; the signal delivery path
    -- avoids the same round-trip for the same reason. Only the user-controlled
    -- result value is fidelity-sensitive; the other child fields are simple
    -- strings/ints that re-encode losslessly.
    child.result = '__XFLOW_EXPANSION_RESULT__'
    local encoded = cjson.encode(child)
    local placeholder = '"__XFLOW_EXPANSION_RESULT__"'
    local startPos, endPos = encoded:find(placeholder, 1, true)
    if startPos then
        encoded = encoded:sub(1, startPos - 1) .. ARGV[7] .. encoded:sub(endPos + 1)
    else
        -- Fallback (placeholder unexpectedly absent): decode the result to keep
        -- correctness even though fidelity is not guaranteed on this branch.
        child.result = cjson.decode(ARGV[7])
        encoded = cjson.encode(child)
    end
    redis.call('HSET', KEYS[3], ARGV[5], encoded)
end
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[8]))
local children = redis.call('HVALS', KEYS[3])
for i = 1, #children do
    if cjson.decode(children[i]).status == 'running' then
        return {1, 0}
    end
end
return {1, 1}
`)

var _ engine.LeaseExpander = (*Store)(nil)
var _ engine.DurableLeaseExpander = (*Store)(nil)

func (s *Store) BeginTaskExpansionWithOutbox(ctx context.Context, lease *engine.TaskLease, children []engine.SubExecution, entries []engine.OutboxEntry) (bool, error) {
	if lease == nil || len(children) != len(entries) {
		return false, engine.ErrInvalidLeaseToken
	}
	ttl := s.getExecTTL(lease.Task.ExecutionID)
	t := namespace.FromContext(ctx)
	args := make([]any, 0, 6+len(children)*5)
	args = append(args, string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID, int(ttl.Seconds()), len(children))
	for index, child := range children {
		entry := entries[index]
		if child.ChildExecID == "" || child.ParentExecID != lease.Task.ExecutionID || child.ParentNode != lease.Task.NodeName || entry.ID == "" {
			return false, engine.ErrInvalidLeaseToken
		}
		childJSON, err := json.Marshal(child)
		if err != nil {
			return false, fmt.Errorf("marshal expansion child %d: %w", index, err)
		}
		outboxJSON, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return false, fmt.Errorf("marshal expansion outbox %d: %w", index, err)
		}
		availableAt := time.Now().UTC().UnixMilli()
		if !entry.AvailableAt.IsZero() {
			availableAt = entry.AvailableAt.UTC().UnixMilli()
		}
		args = append(args, string(child.ChildExecID), string(childJSON), entry.ID, outboxJSON, availableAt)
	}
	result, err := beginTaskExpansionWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		expansionSubExecutionKey(t, lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID),
		outboxReadyKey(t, lease.Task.ExecutionID),
		outboxBodyKey(t, lease.Task.ExecutionID),
	}, args...).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("begin durable expansion %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
		expansionSubExecutionKey(t, lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID),
		outboxReadyKey(t, lease.Task.ExecutionID),
		outboxBodyKey(t, lease.Task.ExecutionID),
	); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) BeginTaskExpansion(ctx context.Context, lease *engine.TaskLease) (bool, error) {
	if lease == nil {
		return false, engine.ErrInvalidLeaseToken
	}
	ttl := s.getExecTTL(lease.Task.ExecutionID)
	t := namespace.FromContext(ctx)
	result, err := beginTaskExpansionLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
	}, string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID, int(ttl.Seconds())).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("begin expansion %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
	); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) CreateExpandedSubExecution(ctx context.Context, lease *engine.TaskLease, sub *engine.SubExecution) (bool, error) {
	if lease == nil || sub == nil || sub.ChildExecID == "" {
		return false, engine.ErrInvalidLeaseToken
	}
	data, err := json.Marshal(sub)
	if err != nil {
		return false, fmt.Errorf("marshal expansion sub-execution: %w", err)
	}
	ttl := s.getExecTTL(lease.Task.ExecutionID)
	t := namespace.FromContext(ctx)
	key := expansionSubExecutionKey(t, lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID)
	result, err := createExpandedSubExecutionLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		key,
	}, string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID, string(sub.ChildExecID), string(data), int(ttl.Seconds())).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("create expansion sub-execution %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID, key); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) CompleteExpandedSubExecution(ctx context.Context, lease *engine.TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, bool, []map[string]any, error) {
	if lease == nil || childExecID == "" {
		return false, false, nil, engine.ErrInvalidLeaseToken
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return false, false, nil, fmt.Errorf("marshal expansion result: %w", err)
	}
	ttl := s.getExecTTL(lease.Task.ExecutionID)
	t := namespace.FromContext(ctx)
	key := expansionSubExecutionKey(t, lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID)
	response, err := completeExpandedSubExecutionLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		key,
	}, string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID, string(childExecID), string(status), string(resultJSON), int(ttl.Seconds())).Slice()
	if err != nil {
		return false, false, nil, fmt.Errorf("complete expansion sub-execution %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(response) != 2 {
		return false, false, nil, fmt.Errorf("complete expansion sub-execution %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, response)
	}
	accepted := redisResultInt(response[0]) == 1
	allDone := accepted && redisResultInt(response[1]) == 1
	if !allDone {
		return false, accepted, nil, nil
	}
	values, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return false, false, nil, fmt.Errorf("read expansion sub-executions %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	subs := make([]*engine.SubExecution, 0, len(values))
	for _, raw := range values {
		var sub engine.SubExecution
		if err := json.Unmarshal([]byte(raw), &sub); err != nil {
			return false, false, nil, fmt.Errorf("decode expansion sub-execution %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		subs = append(subs, &sub)
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].BatchIndex < subs[j].BatchIndex })
	results := make([]map[string]any, 0, len(subs))
	for _, sub := range subs {
		if sub.Result != nil {
			results = append(results, sub.Result)
		}
	}
	return true, true, results, nil
}
