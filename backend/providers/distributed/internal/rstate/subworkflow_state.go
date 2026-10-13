package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func subWorkflowLinkKey(t namespace.Namespace, id types.ExecutionID, nodeName string) string {
	return execKey(t, id, fmt.Sprintf("node:%s:subworkflow", nodeName))
}

// beginSubWorkflowWaitLua parks a claimed node on its child execution: the node
// moves running (or committing) → waiting with its lease identity untouched,
// the link record is written, and the delivery intents land in the ready ZSET
// and body hash. One execution-scoped script — every key here carries the
// {execution id} hash tag — so a reader never sees the wait without its link.
//
// The lease expiry ZSET is deliberately not touched: the deadline the node was
// claimed with (min of node timeout and the engine's wait TTL) is what the
// sweeper later reclaims it by, and waiting is one of the statuses that
// reclaim covers.
var beginSubWorkflowWaitLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' then
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
-- The node record carries the child too (design §6): the parent-side half of
-- the drill-down. It is a meta field rather than part of the status string so
-- it survives the commit that concludes the node — commitNodeLua HSETs its own
-- fields and never removes this one — which is what keeps a finished wait
-- auditable.
redis.call('HSET', KEYS[2], 'child_execution_id', ARGV[8])
redis.call('SET', KEYS[3], ARGV[6], 'EX', ttl)
local count = tonumber(ARGV[7])
local position = 9
for i = 1, count do
    local outboxID = ARGV[position]
    local outboxBody = ARGV[position + 1]
    local availableAt = tonumber(ARGV[position + 2])
    if redis.call('HSETNX', KEYS[5], outboxID, outboxBody) == 1 then
        redis.call('ZADD', KEYS[4], availableAt, outboxID)
    end
    position = position + 3
end
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)
return 1
`)

var _ engine.SubWorkflowLinkStore = (*Store)(nil)

func (s *Store) BeginSubWorkflowWait(ctx context.Context, lease *engine.TaskLease, link engine.SubWorkflowLink, entries []engine.OutboxEntry) (bool, error) {
	if lease == nil || link.ChildExecutionID == "" || link.ParentNode != lease.Task.NodeName {
		return false, engine.ErrInvalidLeaseToken
	}
	linkJSON, err := json.Marshal(link)
	if err != nil {
		return false, fmt.Errorf("marshal sub-workflow link: %w", err)
	}
	ttl := s.getExecTTL(ctx, lease.Task.ExecutionID)
	t := namespace.FromContext(ctx)
	args := make([]any, 0, 8+len(entries)*3)
	args = append(args,
		string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID,
		int(ttl.Seconds()), string(linkJSON), len(entries), string(link.ChildExecutionID))
	for index, entry := range entries {
		if entry.ID == "" {
			return false, engine.ErrInvalidLeaseToken
		}
		outboxJSON, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return false, fmt.Errorf("marshal sub-workflow outbox %d: %w", index, err)
		}
		availableAt := time.Now().UTC().UnixMilli()
		if !entry.AvailableAt.IsZero() {
			availableAt = entry.AvailableAt.UTC().UnixMilli()
		}
		args = append(args, entry.ID, outboxJSON, availableAt)
	}
	result, err := beginSubWorkflowWaitLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		subWorkflowLinkKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		outboxReadyKey(t, lease.Task.ExecutionID),
		outboxBodyKey(t, lease.Task.ExecutionID),
	}, args...).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("begin sub-workflow wait %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if len(entries) > 0 {
		s.markOutboxReadyIndex(ctx, t, lease.Task.ExecutionID)
	}
	return true, nil
}

func (s *Store) GetSubWorkflowLink(ctx context.Context, id types.ExecutionID, nodeName string) (engine.SubWorkflowLink, bool, error) {
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, subWorkflowLinkKey(t, id, nodeName)).Result()
	if err == redis.Nil {
		return engine.SubWorkflowLink{}, false, nil
	}
	if err != nil {
		return engine.SubWorkflowLink{}, false, fmt.Errorf("read sub-workflow link %q/%q: %w", id, nodeName, err)
	}
	var link engine.SubWorkflowLink
	if err := json.Unmarshal([]byte(raw), &link); err != nil {
		return engine.SubWorkflowLink{}, false, fmt.Errorf("decode sub-workflow link %q/%q: %w", id, nodeName, err)
	}
	return link, true, nil
}
