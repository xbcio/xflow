package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

func remainingNodesKey(id types.ExecutionID) string { return execKey(id, "remaining_nodes") }
func failedNodesKey(id types.ExecutionID) string    { return execKey(id, "failed_nodes") }
func advanceMarkerKey(id types.ExecutionID, name string, activationID int) string {
	return execKey(id, fmt.Sprintf("node:%s:advance:%d", name, activationID))
}
func scheduleKey(id types.ExecutionID, nodeIdx int) string {
	return execKey(id, fmt.Sprintf("schedule:%d", nodeIdx))
}
func outboxReadyKey(id types.ExecutionID) string    { return execKey(id, "outbox:ready") }
func outboxBodyKey(id types.ExecutionID) string     { return execKey(id, "outbox:body") }
func outboxAttemptsKey(id types.ExecutionID) string { return execKey(id, "outbox:attempts") }
func outboxDeadKey(id types.ExecutionID) string     { return execKey(id, "outbox:dead") }
func outboxDeadBodyKey(id types.ExecutionID) string { return execKey(id, "outbox:dead:body") }

// commitNodeLua is the durable linearization point for ordinary acyclic node
// results. It deliberately does not expose a committing state: validation,
// terminal state, completion counters, lease index removal, and advance
// outbox intent are persisted by one Redis command.
var commitNodeLua = redis.NewScript(`
local terminal = function(value)
    return value == 'success' or value == 'failed' or value == 'skipped' or value == 'canceled' or value == 'continued'
end
local status = redis.call('GET', KEYS[5])
local expectedActivation = tonumber(ARGV[5] or '0')
local isSystem = tonumber(ARGV[11] or '0')
if terminal(status) then
    local currentActivation = tonumber(redis.call('HGET', KEYS[6], 'activation_id') or '0')
    if currentActivation == expectedActivation then
        if isSystem == 1 and status == ARGV[1] then
            return {2, 0, ''}
        end
        local committed = redis.call('HGET', KEYS[6], 'committed_lease_token') or ''
        if isSystem == 0 and committed ~= '' and committed == ARGV[3] then
            return {2, 0, ''}
        end
    end
    return {0, 0, ''}
end
local executionStatus = redis.call('GET', KEYS[1])
if executionStatus == 'success' or executionStatus == 'failed' or executionStatus == 'canceled' or executionStatus == 'timeout' then
    return {3, 0, executionStatus}
end
if isSystem == 1 then
    local action = redis.call('HGET', KEYS[11], 'action') or ''
    if action ~= 'skip' or (status and status ~= 'pending') then
        return {0, 0, ''}
    end
else
    if status ~= 'running' and status ~= 'committing' and status ~= 'waiting' then
        return {0, 0, ''}
    end
    local leaseID = redis.call('HGET', KEYS[6], 'lease_id') or ''
    local leaseToken = redis.call('HGET', KEYS[6], 'lease_token') or ''
    local attempt = tonumber(redis.call('HGET', KEYS[6], 'attempt') or '0')
    local activation = tonumber(redis.call('HGET', KEYS[6], 'activation_id') or '0')
    if leaseID ~= ARGV[2] or leaseToken ~= ARGV[3] or attempt ~= tonumber(ARGV[4]) or activation ~= expectedActivation then
        return {0, 0, ''}
    end
end
local ttl = tonumber(ARGV[13])
if tonumber(ARGV[7]) == 1 then
    redis.call('SET', KEYS[7], ARGV[8], 'EX', ttl)
end
redis.call('SET', KEYS[5], ARGV[1], 'EX', ttl)
redis.call('HSET', KEYS[6],
    'lease_id', '',
    'lease_token', '',
    'lease_issued_at_ms', '0',
    'lease_ttl_ms', '0',
    'lease_deadline_ms', '0',
    'activation_id', ARGV[5],
    'auto_depth', ARGV[6],
    'port', ARGV[9],
    'error', ARGV[10],
    'committed_lease_token', ARGV[3],
    'committed_attempt', ARGV[4])
redis.call('EXPIRE', KEYS[6], ttl)
redis.call('ZREM', KEYS[8], ARGV[14])
local done = 0
local finalStatus = ''
if tonumber(ARGV[16]) == 0 then
    local remaining = redis.call('DECR', KEYS[3])
    redis.call('EXPIRE', KEYS[3], ttl)
    if ARGV[1] == 'failed' then
        redis.call('INCR', KEYS[4])
        redis.call('EXPIRE', KEYS[4], ttl)
    end
    if tonumber(ARGV[12]) == 1 or remaining <= 0 then
        finalStatus = 'success'
        if tonumber(ARGV[12]) == 1 or tonumber(redis.call('GET', KEYS[4]) or '0') > 0 then
            finalStatus = 'failed'
        end
        redis.call('SET', KEYS[1], finalStatus, 'EX', ttl)
        if finalStatus == 'failed' and ARGV[10] ~= '' then
            redis.call('SET', KEYS[2], ARGV[10], 'EX', ttl)
        end
        done = 1
    end
end
if done == 0 and tonumber(ARGV[12]) == 0 and ARGV[15] ~= '' then
    if redis.call('HSETNX', KEYS[10], ARGV[15], ARGV[17]) == 1 then
        redis.call('ZADD', KEYS[9], tonumber(ARGV[18]), ARGV[15])
        redis.call('EXPIRE', KEYS[9], ttl)
        redis.call('EXPIRE', KEYS[10], ttl)
    end
end
if tonumber(ARGV[16]) == 1 and done == 0 and tonumber(ARGV[12]) == 0 then
    local ncyc = tonumber(ARGV[22] or '0')
    if ncyc > 0 then
        for i = 1, ncyc do
            local cid = ARGV[22 + (i - 1) * 2 + 1]
            local cbody = ARGV[22 + (i - 1) * 2 + 2]
            if redis.call('HSETNX', KEYS[10], cid, cbody) == 1 then
                redis.call('ZADD', KEYS[9], tonumber(ARGV[18]), cid)
            end
        end
        redis.call('EXPIRE', KEYS[9], ttl)
        redis.call('EXPIRE', KEYS[10], ttl)
    elseif tonumber(ARGV[19] or '0') == 1 then
        finalStatus = ARGV[20]
        redis.call('SET', KEYS[1], finalStatus, 'EX', ttl)
        if finalStatus == 'failed' and ARGV[21] ~= '' then
            redis.call('SET', KEYS[2], ARGV[21], 'EX', ttl)
        end
        done = 1
    end
end
return {1, done, finalStatus}
`)

// advanceNodeLua converts all inbound arrivals from one completed source into
// exactly one execute or skip intent per destination. The advance marker turns
// response loss and repeated outbox delivery into no-ops.
var advanceNodeLua = redis.NewScript(`
local terminal = function(value)
    return value == 'success' or value == 'failed' or value == 'skipped' or value == 'canceled' or value == 'continued'
end
local executionStatus = redis.call('GET', KEYS[1])
if executionStatus == 'success' or executionStatus == 'failed' or executionStatus == 'canceled' or executionStatus == 'timeout' then
    return 0
end
if not terminal(redis.call('GET', KEYS[2])) then
    return 0
end
if redis.call('SET', KEYS[4], '1', 'NX', 'EX', tonumber(ARGV[2])) == false then
    return 0
end
local count = tonumber(ARGV[3])
local keypos = 7
local argpos = 5
for i = 1, count do
    local inDegreeKey = KEYS[keypos]
    local activeKey = KEYS[keypos + 1]
    local scheduleKey = KEYS[keypos + 2]
    local arrivals = tonumber(ARGV[argpos])
    local activeArrivals = tonumber(ARGV[argpos + 1])
    local mergeMode = ARGV[argpos + 2]
    local executeID = ARGV[argpos + 3]
    local executeBody = ARGV[argpos + 4]
    local skipID = ARGV[argpos + 5]
    local skipBody = ARGV[argpos + 6]
    local activeBefore = tonumber(redis.call('GET', activeKey) or '0')
    local remaining = redis.call('DECRBY', inDegreeKey, arrivals)
    if activeArrivals > 0 then
        redis.call('INCRBY', activeKey, activeArrivals)
    end
    local activeNow = tonumber(redis.call('GET', activeKey) or '0')
    redis.call('EXPIRE', inDegreeKey, tonumber(ARGV[2]))
    redis.call('EXPIRE', activeKey, tonumber(ARGV[2]))
    local action = redis.call('HGET', scheduleKey, 'action') or ''
    if action == '' then
        local nextAction = ''
        if mergeMode == 'wait_any' and activeArrivals > 0 and activeBefore == 0 then
            nextAction = 'execute'
        elseif remaining <= 0 then
            if activeNow > 0 then
                nextAction = 'execute'
            else
                nextAction = 'skip'
            end
        end
        if nextAction ~= '' then
            redis.call('HSET', scheduleKey, 'action', nextAction)
            redis.call('EXPIRE', scheduleKey, tonumber(ARGV[2]))
            local outboxID = executeID
            local outboxBody = executeBody
            if nextAction == 'skip' then
                outboxID = skipID
                outboxBody = skipBody
            end
            if redis.call('HSETNX', KEYS[6], outboxID, outboxBody) == 1 then
                redis.call('ZADD', KEYS[5], tonumber(ARGV[4]), outboxID)
                redis.call('EXPIRE', KEYS[5], tonumber(ARGV[2]))
                redis.call('EXPIRE', KEYS[6], tonumber(ARGV[2]))
            end
        end
    end
    keypos = keypos + 3
    argpos = argpos + 7
end
return 1
`)

var ackOutboxLua = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
`)

// recordOutboxFailureLua increments a durable delivery-attempt counter. Once
// the configured threshold is reached it removes the pending intent and moves
// its immutable body to an independent dead-letter index for operator review.
var recordOutboxFailureLua = redis.NewScript(`
local body = redis.call('HGET', KEYS[2], ARGV[1])
if not body then
    return {0, 0}
end
local attempts = redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
local ttl = tonumber(ARGV[4])
if attempts >= tonumber(ARGV[2]) then
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('HDEL', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    redis.call('HSET', KEYS[5], ARGV[1], body)
    redis.call('ZADD', KEYS[4], tonumber(ARGV[3]), ARGV[1])
    redis.call('EXPIRE', KEYS[4], ttl)
    redis.call('EXPIRE', KEYS[5], ttl)
    return {attempts, 1}
end
redis.call('EXPIRE', KEYS[3], ttl)
return {attempts, 0}
`)

// resetNodeForRetryWithOutboxLua combines the legacy retry reset with the
// durable retry delivery intent so a process crash cannot leave a pending node
// without a task to execute.
var resetNodeForRetryWithOutboxLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' and status ~= 'waiting' then
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
if token == '' or token ~= ARGV[1] then
    return 0
end
local ttl = tonumber(ARGV[2])
redis.call('SET', KEYS[1], 'pending', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '')
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSETNX', KEYS[5], ARGV[4], ARGV[5])
redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[4])
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)
return 1
`)

// revokeLeaseWithOutboxLua fences lease release and durable redelivery in one
// transition, eliminating the revoke-before-enqueue crash window.
var revokeLeaseWithOutboxLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' and status ~= 'waiting' then
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token')
if not token or token == '' or token ~= ARGV[1] then
    return 0
end
local ttl = tonumber(ARGV[2])
redis.call('SET', KEYS[1], 'pending', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '')
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSETNX', KEYS[5], ARGV[4], ARGV[5])
redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[4])
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)
return 1
`)

type redisOutboxEntry struct {
	ID          string      `json:"id"`
	Task        engine.Task `json:"task"`
	AutoDepth   int         `json:"auto_depth,omitempty"`
	Activation  int         `json:"activation_id,omitempty"`
	AvailableAt int64       `json:"available_at_ms,omitempty"`
	CreatedAt   int64       `json:"created_at_ms,omitempty"`
}

var _ engine.AtomicStateStore = (*Store)(nil)
var _ engine.LegacyNodeCommitter = (*Store)(nil)
var _ engine.OutboxFailureRecorder = (*Store)(nil)
var _ engine.OutboxMetricsReader = (*Store)(nil)

// CommitLeasedNode reuses the fenced Redis node transition for legacy cycle
// paths, whose follow-up scheduling remains outside CommitNode's DAG counter.
func (s *Store) CommitLeasedNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	return s.CommitNode(ctx, req)
}

// ResetNodeForRetryWithOutbox persists the retry reset and delayed delivery
// intent in one Redis Lua transition.
func (s *Store) ResetNodeForRetryWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, fmt.Errorf("retry outbox for %q/%q has empty ID", id, nodeName)
	}
	encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
	if err != nil {
		return false, err
	}
	availableAt := time.Now().UTC().UnixMilli()
	if !entry.AvailableAt.IsZero() {
		availableAt = entry.AvailableAt.UTC().UnixMilli()
	}
	ttl := s.getExecTTL(id)
	result, err := resetNodeForRetryWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(id, nodeName),
		nodeMetaKey(id, nodeName),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("reset node for retry with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(id, nodeName),
		nodeMetaKey(id, nodeName),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
	); err != nil {
		return false, err
	}
	return true, nil
}

// RevokeLeaseWithOutbox persists a token-fenced lease release and the exact
// redelivery task in one Redis Lua transition.
func (s *Store) RevokeLeaseWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	if token == "" {
		return false, nil
	}
	if entry.ID == "" {
		return false, fmt.Errorf("requeue outbox for %q/%q has empty ID", id, nodeName)
	}
	encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
	if err != nil {
		return false, err
	}
	availableAt := time.Now().UTC().UnixMilli()
	if !entry.AvailableAt.IsZero() {
		availableAt = entry.AvailableAt.UTC().UnixMilli()
	}
	ttl := s.getExecTTL(id)
	result, err := revokeLeaseWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(id, nodeName),
		nodeMetaKey(id, nodeName),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(id, nodeName),
		nodeMetaKey(id, nodeName),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
	); err != nil {
		return false, err
	}
	return true, nil
}

// CommitNode implements engine.AtomicStateStore using one fenced Redis Lua
// transition. SQL projection remains best-effort and runs only after Redis has
// accepted the authoritative state.
func (s *Store) CommitNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	ttl := s.getExecTTL(req.ExecutionID)
	outputJSON := ""
	if req.StoreOutput {
		encoded, err := json.Marshal(req.Output)
		if err != nil {
			return engine.CommitNodeResult{}, fmt.Errorf("marshal output %q/%q: %w", req.ExecutionID, req.NodeName, err)
		}
		outputJSON = string(encoded)
	}
	advanceID := ""
	advanceJSON := ""
	if req.AdvanceTask != nil {
		advanceID = redisAdvanceOutboxID(req.ExecutionID, req.NodeName, req.ActivationID)
		encoded, err := marshalRedisOutboxEntry(advanceID, *req.AdvanceTask, time.Time{})
		if err != nil {
			return engine.CommitNodeResult{}, err
		}
		advanceJSON = encoded
	}
	storeOutput := 0
	if req.StoreOutput {
		storeOutput = 1
	}
	system := 0
	if req.System {
		system = 1
	}
	fatal := 0
	if req.Fatal {
		fatal = 1
	}
	allowCycles := 0
	if g, err := s.LoadGraph(ctx, req.ExecutionID); err == nil && g != nil && g.AllowCycles {
		allowCycles = 1
	}
	cyclicComplete := 0
	cyclicFinalStatus := ""
	cyclicFinalError := ""
	if req.CyclicComplete {
		cyclicComplete = 1
		cyclicFinalStatus = string(req.CyclicFinalStatus)
		cyclicFinalError = req.CyclicFinalError
	}
	cyclicArgs := make([]any, 0, len(req.CyclicOutbox)*2)
	for _, entry := range req.CyclicOutbox {
		body, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return engine.CommitNodeResult{}, err
		}
		cyclicArgs = append(cyclicArgs, entry.ID, body)
	}
	args := []any{
		string(req.Status), string(req.LeaseID), string(req.LeaseToken), req.Attempt,
		req.ActivationID, req.AutoDepth, storeOutput, outputJSON, req.Port, req.Error,
		system, fatal, int(ttl.Seconds()), leaseExpiryMember(req.ExecutionID, req.NodeName),
		advanceID, allowCycles, advanceJSON, time.Now().UTC().UnixMilli(),
		cyclicComplete, cyclicFinalStatus, cyclicFinalError, len(req.CyclicOutbox),
	}
	args = append(args, cyclicArgs...)
	result, err := commitNodeLua.Run(ctx, s.rdb, []string{
		execKey(req.ExecutionID, "status"),
		execKey(req.ExecutionID, "error"),
		remainingNodesKey(req.ExecutionID),
		failedNodesKey(req.ExecutionID),
		nodeStatusKey(req.ExecutionID, req.NodeName),
		nodeMetaKey(req.ExecutionID, req.NodeName),
		outputKey(req.ExecutionID, req.NodeName),
		leaseExpiryZSetKey(req.ExecutionID),
		outboxReadyKey(req.ExecutionID),
		outboxBodyKey(req.ExecutionID),
		scheduleKey(req.ExecutionID, req.NodeIdx),
	}, args...).Slice()
	if err != nil {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if len(result) != 3 {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unexpected result %v", req.ExecutionID, req.NodeName, result)
	}
	code := redisResultInt(result[0])
	out := engine.CommitNodeResult{}
	switch code {
	case 0:
		out.Outcome = engine.CommitOutcomeStaleToken
	case 1:
		out.Outcome = engine.CommitOutcomeAccepted
		out.Applied = true
		out.ExecutionDone = redisResultInt(result[1]) == 1
		out.ExecutionStatus = types.ExecutionStatus(redisResultString(result[2]))
		if !out.ExecutionDone && !req.Fatal {
			if advanceID != "" {
				out.OutboxIDs = append(out.OutboxIDs, advanceID)
			}
			for _, entry := range req.CyclicOutbox {
				out.OutboxIDs = append(out.OutboxIDs, entry.ID)
			}
		}
	case 2:
		out.Outcome = engine.CommitOutcomeDuplicateTerminal
	case 3:
		out.Outcome = engine.CommitOutcomeExecutionInactive
	default:
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unknown outcome %d", req.ExecutionID, req.NodeName, code)
	}
	if out.ExecutionDone {
		s.evictExecutionCaches(req.ExecutionID)
	}
	if out.Applied && s.db != nil && !s.transient {
		output, _ := json.Marshal(req.Output)
		rec := &store.NodeRecord{
			ExecutionID: req.ExecutionID,
			NodeName:    req.NodeName,
			Status:      req.Status,
			LeaseID:     string(req.LeaseID),
			LeaseToken:  string(req.LeaseToken),
			Attempt:     req.Attempt,
			Output:      output,
			Port:        req.Port,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "commit_node", func(ctx context.Context) error { return s.db.UpsertNode(ctx, rec) })
	}
	return out, nil
}

// AdvanceNode implements engine.AtomicStateStore by atomically applying all
// destination arrivals and creating durable execution/skip intents.
func (s *Store) AdvanceNode(ctx context.Context, req engine.AdvanceNodeRequest) (engine.AdvanceNodeResult, error) {
	if len(req.Arrivals) == 0 {
		return engine.AdvanceNodeResult{Applied: true}, nil
	}
	ttl := s.getExecTTL(req.ExecutionID)
	keys := []string{
		execKey(req.ExecutionID, "status"),
		nodeStatusKey(req.ExecutionID, req.NodeName),
		nodeMetaKey(req.ExecutionID, req.NodeName),
		advanceMarkerKey(req.ExecutionID, req.NodeName, req.ActivationID),
		outboxReadyKey(req.ExecutionID),
		outboxBodyKey(req.ExecutionID),
	}
	args := []any{req.ActivationID, int(ttl.Seconds()), len(req.Arrivals), time.Now().UTC().UnixMilli()}
	outboxIDs := make([]string, 0, len(req.Arrivals))
	for _, arrival := range req.Arrivals {
		keys = append(keys,
			inDegreeKey(req.ExecutionID, arrival.NodeIdx),
			activeInputsKey(req.ExecutionID, arrival.NodeIdx),
			scheduleKey(req.ExecutionID, arrival.NodeIdx),
		)
		executeID := redisExecuteOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		skipID := redisSkipOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		executeJSON, err := marshalRedisOutboxEntry(executeID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{})
		if err != nil {
			return engine.AdvanceNodeResult{}, err
		}
		skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			Type:         engine.TaskTypeNodeSkip,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{})
		if err != nil {
			return engine.AdvanceNodeResult{}, err
		}
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON)
		outboxIDs = append(outboxIDs, executeID, skipID)
	}
	applied, err := advanceNodeLua.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil {
		return engine.AdvanceNodeResult{}, fmt.Errorf("advance node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if applied == 0 {
		return engine.AdvanceNodeResult{}, nil
	}
	return engine.AdvanceNodeResult{Applied: true, OutboxIDs: outboxIDs}, nil
}

// ListOutbox returns ready entries for one execution. It leaves entries in
// Redis until AckOutbox so enqueue/ack response loss is retried safely.
func (s *Store) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	ids, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: outboxReadyKey(id), Start: "-inf", Stop: fmt.Sprintf("%d", before.UnixMilli()), ByScore: true, Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list outbox %q: %w", id, err)
	}
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		raw, err := s.rdb.HGet(ctx, outboxBodyKey(id), entryID).Result()
		if err == redis.Nil {
			_ = ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(id), outboxBodyKey(id), outboxAttemptsKey(id)}, entryID).Err()
			continue
		}
		if err != nil {
			return out, fmt.Errorf("read outbox %q/%q: %w", id, entryID, err)
		}
		entry, err := unmarshalRedisOutboxEntry(raw)
		if err != nil {
			return out, fmt.Errorf("decode outbox %q/%q: %w", id, entryID, err)
		}
		out = append(out, entry)
	}
	return out, nil
}

// AckOutbox removes an already-enqueued entry atomically and idempotently.
func (s *Store) AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error {
	if err := ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(id), outboxBodyKey(id), outboxAttemptsKey(id)}, entryID).Err(); err != nil {
		return fmt.Errorf("ack outbox %q/%q: %w", id, entryID, err)
	}
	return nil
}

// RecordOutboxFailure records a failed queue handoff and moves an intent to
// execution-scoped dead-letter storage after maxAttempts failures.
func (s *Store) RecordOutboxFailure(ctx context.Context, id types.ExecutionID, entryID string, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
	if maxAttempts <= 0 {
		maxAttempts = engine.DefaultOutboxMaxDeliveryAttempts
	}
	ttl := s.getExecTTL(id)
	result, err := recordOutboxFailureLua.Run(ctx, s.rdb, []string{
		outboxReadyKey(id),
		outboxBodyKey(id),
		outboxAttemptsKey(id),
		outboxDeadKey(id),
		outboxDeadBodyKey(id),
	}, entryID, maxAttempts, time.Now().UTC().UnixMilli(), int(ttl.Seconds())).Slice()
	if err != nil {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: %w", id, entryID, err)
	}
	if len(result) != 2 {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: unexpected result %v", id, entryID, result)
	}
	failure := engine.OutboxDeliveryFailure{Attempts: int(redisResultInt(result[0])), DeadLettered: redisResultInt(result[1]) == 1}
	if err := s.refreshTransientTTL(ctx, id,
		outboxReadyKey(id),
		outboxBodyKey(id),
		outboxAttemptsKey(id),
		outboxDeadKey(id),
		outboxDeadBodyKey(id),
	); err != nil {
		return engine.OutboxDeliveryFailure{}, err
	}
	return failure, nil
}

// OutboxMetrics scans durable pending and dead-letter indexes to provide
// aggregate backlog metrics. It is a recovery/observability path, not part of
// the task-delivery hot path.
func (s *Store) OutboxMetrics(ctx context.Context) (engine.OutboxMetricsSnapshot, error) {
	var snapshot engine.OutboxMetricsSnapshot
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, "xflow:exec:{*}:outbox:ready", 128).Result()
		if err != nil {
			return engine.OutboxMetricsSnapshot{}, fmt.Errorf("scan pending outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return engine.OutboxMetricsSnapshot{}, fmt.Errorf("count pending outbox %q: %w", key, err)
			}
			snapshot.Pending += int(count)
			if count == 0 {
				continue
			}
			id, ok := executionIDFromKey(key)
			if !ok {
				continue
			}
			entries, err := s.rdb.HVals(ctx, outboxBodyKey(id)).Result()
			if err != nil {
				return engine.OutboxMetricsSnapshot{}, fmt.Errorf("read pending outbox bodies %q: %w", key, err)
			}
			for _, raw := range entries {
				entry, err := unmarshalRedisOutboxEntry(raw)
				if err != nil {
					return engine.OutboxMetricsSnapshot{}, fmt.Errorf("decode pending outbox body %q: %w", key, err)
				}
				createdAt := entry.CreatedAt
				if createdAt.IsZero() {
					createdAt = entry.AvailableAt
				}
				if !createdAt.IsZero() && (snapshot.OldestPendingAt.IsZero() || createdAt.Before(snapshot.OldestPendingAt)) {
					snapshot.OldestPendingAt = createdAt
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	cursor = 0
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, "xflow:exec:{*}:outbox:dead", 128).Result()
		if err != nil {
			return engine.OutboxMetricsSnapshot{}, fmt.Errorf("scan dead-letter outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return engine.OutboxMetricsSnapshot{}, fmt.Errorf("count dead-letter outbox %q: %w", key, err)
			}
			snapshot.DeadLettered += int(count)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return snapshot, nil
}

// ListOutboxExecutions scans execution-scoped ready indexes. The index is
// authoritative per execution; scanning is only a recovery discovery path.
func (s *Store) ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error) {
	if limit <= 0 {
		return nil, nil
	}
	ids := make(map[types.ExecutionID]struct{})
	var cursor uint64
	for len(ids) < limit {
		keys, next, err := s.rdb.Scan(ctx, cursor, "xflow:exec:{*}:outbox:ready", 128).Result()
		if err != nil {
			return nil, fmt.Errorf("scan outbox indexes: %w", err)
		}
		for _, key := range keys {
			id, ok := executionIDFromKey(key)
			if ok {
				ids[id] = struct{}{}
			}
			if len(ids) >= limit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	out := make([]types.ExecutionID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func marshalRedisOutboxEntry(id string, task engine.Task, availableAt time.Time) (string, error) {
	entry := redisOutboxEntry{
		ID:          id,
		Task:        task,
		AutoDepth:   task.AutoDepth,
		Activation:  task.ActivationID,
		AvailableAt: availableAt.UnixMilli(),
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	if availableAt.IsZero() {
		entry.AvailableAt = 0
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("marshal outbox %q: %w", id, err)
	}
	return string(data), nil
}

func unmarshalRedisOutboxEntry(raw string) (engine.OutboxEntry, error) {
	var encoded redisOutboxEntry
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		return engine.OutboxEntry{}, err
	}
	encoded.Task.AutoDepth = encoded.AutoDepth
	encoded.Task.ActivationID = encoded.Activation
	entry := engine.OutboxEntry{ID: encoded.ID, Task: encoded.Task}
	if encoded.AvailableAt > 0 {
		entry.AvailableAt = time.UnixMilli(encoded.AvailableAt).UTC()
	}
	if encoded.CreatedAt > 0 {
		entry.CreatedAt = time.UnixMilli(encoded.CreatedAt).UTC()
	}
	return entry, nil
}

func redisAdvanceOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("advance/%s/%s/%d", id, name, activationID)
}
func redisExecuteOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("execute/%s/%s/%d", id, name, activationID)
}
func redisSkipOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("skip/%s/%s/%d", id, name, activationID)
}

func redisResultInt(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		var parsed int64
		_, _ = fmt.Sscanf(typed, "%d", &parsed)
		return parsed
	default:
		return 0
	}
}

func redisResultString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func executionIDFromKey(key string) (types.ExecutionID, bool) {
	const prefix = "xflow:exec:{"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	end := strings.IndexByte(rest, '}')
	if end <= 0 {
		return "", false
	}
	return types.ExecutionID(rest[:end]), true
}

func (s *Store) evictExecutionCaches(id types.ExecutionID) {
	s.mu.Lock()
	delete(s.graphs, id)
	s.mu.Unlock()
	s.ttlMu.Lock()
	delete(s.execTTLs, id)
	s.ttlMu.Unlock()
}
