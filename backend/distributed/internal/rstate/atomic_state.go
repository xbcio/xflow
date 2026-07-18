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

// outboxDeadMetaKey holds compact immutable per-entry metadata (node + activation)
// as a per-entry hash, written at dead-letter time so the activation-safe replay
// guard can read node/activation without parsing the JSON body. Per-entry hashing
// avoids any delimiter ambiguity under Redis Lua 5.1.
func outboxDeadMetaKey(id types.ExecutionID, entryID string) string {
	return execKey(id, "outbox:dead:meta:"+entryID)
}

// outboxReplayEntryIdxKey maps a dead-letter entry ID to the RequestID of the
// replay that moved it, so a concurrent or retried replay with a different
// RequestID returns already_replayed (with the original receipt) instead of
// degrading to not_found once the dead body is gone.
func outboxReplayEntryIdxKey(id types.ExecutionID) string { return execKey(id, "replay:entryidx") }

// outboxReplayReceiptKey holds the authoritative immutable receipt for one
// replay RequestID. It is written atomically with the dead→ready move and
// survives the loss of the dead body, so a retry with the same RequestID
// recovers the original outcome and AuditID.
func outboxReplayReceiptKey(id types.ExecutionID, requestID string) string {
	return execKey(id, "replay:receipt:"+requestID)
}

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
// its immutable body to an independent dead-letter index for operator review,
// alongside compact node/activation metadata so later replay can guard against
// stale activations without parsing the JSON body.
// KEYS: 1=outbox:ready 2=outbox:body 3=outbox:attempts 4=outbox:dead 5=outbox:dead:body 6=outbox:dead:meta
// ARGV: 1=entryID 2=maxAttempts 3=now_ms 4=ttl_seconds 5=node_name 6=activation_id
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
    redis.call('HSET', KEYS[6], 'node', ARGV[5], 'activation', ARGV[6])
    redis.call('EXPIRE', KEYS[4], ttl)
    redis.call('EXPIRE', KEYS[5], ttl)
    redis.call('EXPIRE', KEYS[6], ttl)
    return {attempts, 1}
end
redis.call('EXPIRE', KEYS[3], ttl)
return {attempts, 0}
`)

// replayDeadLetterLua atomically and activation-safely moves a dead-lettered
// entry back to the ready set, writing an immutable receipt keyed by RequestID
// so a lost response can be recovered by retrying with the same RequestID.
//
// KEYS: 1=outbox:dead 2=outbox:dead:body 3=outbox:ready 4=outbox:body
//       5=outbox:attempts 6=exec:status 7=outbox:dead:meta 8=replay:entryidx
// ARGV: 1=entryID 2=now_ms 3=ttl_seconds 4=request_id 5=operator 6=reason 7=exec_id
// Returns {outcome, audit_id, node, activation} where outcome:
//   1=replayed 2=rejected_terminal 3=rejected_inactive 4=rejected_node_terminal
//   5=rejected_activation_mismatch 6=already_replayed 0=not_found
//
// Node status/meta keys are derived inside the script from the dead-meta node
// name; all keys share the execution hash tag so they are co-located on a
// single-node (G0) or hash-tagged Cluster (G2) deployment.
var replayDeadLetterLua = redis.NewScript(`
local entryID = ARGV[1]
local requestID = ARGV[4]
local execID = ARGV[7]
local receiptKey = 'xflow:exec:{' .. execID .. '}:replay:receipt:' .. requestID

-- decode a receipt hash into node/activation/audit_id
local readReceipt = function(key)
    if redis.call('EXISTS', key) == 0 then return nil end
    return {
        node = redis.call('HGET', key, 'node') or '',
        activation = redis.call('HGET', key, 'activation') or '',
        audit_id = redis.call('HGET', key, 'audit_id') or '',
    }
end

-- 1. Idempotency: same RequestID already replayed -> return original receipt.
local r = readReceipt(receiptKey)
if r then
    return {6, r.audit_id, r.node, r.activation}
end
-- Different RequestID for an already-replayed entry?
local priorReqID = redis.call('HGET', KEYS[8], entryID)
if priorReqID and priorReqID ~= '' then
    local priorKey = 'xflow:exec:{' .. execID .. '}:replay:receipt:' .. priorReqID
    local pr = readReceipt(priorKey)
    if pr then
        return {6, pr.audit_id, pr.node, pr.activation}
    end
end

-- 2. Execution status guard.
local status = redis.call('GET', KEYS[6])
if not status then
    return {3, '', '', ''}
end
if status == 'success' or status == 'failed' or status == 'canceled' or status == 'timeout' then
    return {2, '', '', ''}
end

-- 3. Read dead body + immutable meta (per-entry hash fields).
local body = redis.call('HGET', KEYS[2], entryID)
if not body then
    return {0, '', '', ''}
end
local nodeName = redis.call('HGET', KEYS[7], 'node') or ''
local entryActivation = redis.call('HGET', KEYS[7], 'activation') or ''

-- 4. Node guard: reject if the node is terminal, or if the entry's
--    activation no longer matches the node's current activation (stale
--    cyclic re-entry). Skipped only when meta is absent (legacy entry).
if nodeName ~= '' then
    local nodeStatusKey = 'xflow:exec:{' .. execID .. '}:node:' .. nodeName .. ':status'
    local nodeMetaKey   = 'xflow:exec:{' .. execID .. '}:node:' .. nodeName .. ':meta'
    local nstatus = redis.call('GET', nodeStatusKey)
    if nstatus then
        if nstatus == 'success' or nstatus == 'failed' or nstatus == 'skipped'
           or nstatus == 'canceled' or nstatus == 'continued' then
            return {4, '', nodeName, entryActivation}
        end
    end
    if entryActivation ~= '' then
        local currentActivation = redis.call('HGET', nodeMetaKey, 'activation_id') or ''
        if currentActivation ~= '' and currentActivation ~= entryActivation then
            return {5, '', nodeName, entryActivation}
        end
    end
end

-- 5. Atomic dead->ready move: preserve body, reset attempts.
local ttl = tonumber(ARGV[3])
redis.call('HSET', KEYS[4], entryID, body)
redis.call('ZADD', KEYS[3], tonumber(ARGV[2]), entryID)
redis.call('HDEL', KEYS[5], entryID)
redis.call('ZREM', KEYS[1], entryID)
redis.call('HDEL', KEYS[2], entryID)
redis.call('DEL', KEYS[7])
redis.call('EXPIRE', KEYS[3], ttl)
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)

-- 6. Write authoritative immutable receipt + entry index.
local auditID = requestID .. ':' .. ARGV[2]
redis.call('HSET', receiptKey,
    'node', nodeName,
    'activation', entryActivation,
    'audit_id', auditID,
    'outcome', 'replayed',
    'operator', ARGV[5],
    'reason', ARGV[6],
    'entry_id', entryID,
    'ts_ms', ARGV[2])
redis.call('EXPIRE', receiptKey, ttl)
redis.call('HSET', KEYS[8], entryID, requestID)
redis.call('EXPIRE', KEYS[8], ttl)

return {1, auditID, nodeName, entryActivation}
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
var _ engine.DeadLetterStore = (*Store)(nil)

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
	if g, err := s.LoadGraph(ctx, req.ExecutionID); err == nil && g != nil && g.AllowCycles() {
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
// execution-scoped dead-letter storage after maxAttempts failures. It also
// writes compact node/activation metadata so later replay can guard against
// stale activations without parsing the entry body.
func (s *Store) RecordOutboxFailure(ctx context.Context, id types.ExecutionID, entry engine.OutboxEntry, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
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
		outboxDeadMetaKey(id, entry.ID),
	}, entry.ID, maxAttempts, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		entry.Task.NodeName, entry.Task.ActivationID).Slice()
	if err != nil {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: %w", id, entry.ID, err)
	}
	if len(result) != 2 {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: unexpected result %v", id, entry.ID, result)
	}
	failure := engine.OutboxDeliveryFailure{Attempts: int(redisResultInt(result[0])), DeadLettered: redisResultInt(result[1]) == 1}
	if err := s.refreshTransientTTL(ctx, id,
		outboxReadyKey(id),
		outboxBodyKey(id),
		outboxAttemptsKey(id),
		outboxDeadKey(id),
		outboxDeadBodyKey(id),
		outboxDeadMetaKey(id, entry.ID),
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

// ListDeadLetters returns one page of dead-lettered outbox entries for one
// execution, ordered oldest-first. It reads the dead-letter body hash directly;
// entries are not removed. Pagination uses a stable opaque cursor (the last
// entry ID returned); an empty cursor starts from the oldest entry. limit<=0
// defaults to a bounded page size. The returned NextCursor is empty when the
// page is the last.
func (s *Store) ListDeadLetters(ctx context.Context, id types.ExecutionID, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	const defaultLimit, maxLimit = 100, 500
	if page.Limit <= 0 {
		page.Limit = defaultLimit
	}
	if page.Limit > maxLimit {
		page.Limit = maxLimit
	}
	// Stable lexicographic pagination by entry ID (a member cursor). The dead
	// index is append-only by unique entry ID, so a member cursor is stable for
	// the lifetime of a listing. Over-fetch by one to detect a next page.
	args := redis.ZRangeArgs{
		Key: outboxDeadKey(id), Start: "-", Stop: "+", ByLex: true, Offset: 0, Count: int64(page.Limit + 1),
	}
	if page.Cursor != "" {
		// Exclusive lower bound: resume strictly after the cursor member.
		args.Start = "(" + page.Cursor
	}
	ids, err := s.rdb.ZRangeArgs(ctx, args).Result()
	if err != nil {
		return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, err)
	}
	var nextCursor string
	if len(ids) > page.Limit {
		nextCursor = ids[page.Limit-1]
		ids = ids[:page.Limit]
	}
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		raw, err := s.rdb.HGet(ctx, outboxDeadBodyKey(id), entryID).Result()
		if err == redis.Nil {
			// body missing while index still references it: self-heal by removing the stale index entry
			_ = s.rdb.ZRem(ctx, outboxDeadKey(id), entryID).Err()
			continue
		}
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("read dead letter %q/%q: %w", id, entryID, err)
		}
		entry, err := unmarshalRedisOutboxEntry(raw)
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("decode dead letter %q/%q: %w", id, entryID, err)
		}
		out = append(out, entry)
	}
	return engine.DeadLetterList{Entries: out, NextCursor: nextCursor}, nil
}

// ReplayDeadLetter moves a dead-lettered entry atomically and activation-safely
// back to the ready set so the OutboxDispatcher redelivers it. Concurrent or
// retried replays of the same entry (or RequestID) collapse to a single
// ReplayReplayed and return ReplayAlreadyReplayed with the original AuditID.
// Replays of terminal/expired executions, terminal nodes, or stale activations
// are rejected without mutating state.
func (s *Store) ReplayDeadLetter(ctx context.Context, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	if req.EntryID == "" {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayNotFound, ExecutionID: req.ExecutionID}, nil
	}
	requestID := req.RequestID
	if requestID == "" {
		requestID = req.EntryID
	}
	ttl := s.getExecTTL(req.ExecutionID)
	result, err := replayDeadLetterLua.Run(ctx, s.rdb, []string{
		outboxDeadKey(req.ExecutionID),
		outboxDeadBodyKey(req.ExecutionID),
		outboxReadyKey(req.ExecutionID),
		outboxBodyKey(req.ExecutionID),
		outboxAttemptsKey(req.ExecutionID),
		execKey(req.ExecutionID, "status"),
		outboxDeadMetaKey(req.ExecutionID, req.EntryID),
		outboxReplayEntryIdxKey(req.ExecutionID),
	}, req.EntryID, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		requestID, req.Operator, req.Reason, string(req.ExecutionID)).Slice()
	if err != nil {
		return engine.ReplayDeadLetterResult{}, fmt.Errorf("replay dead letter %q/%q: %w", req.ExecutionID, req.EntryID, err)
	}
	if len(result) != 4 {
		return engine.ReplayDeadLetterResult{}, fmt.Errorf("replay dead letter %q/%q: unexpected result %v", req.ExecutionID, req.EntryID, result)
	}
	outcome := replayOutcomeFromInt(redisResultInt(result[0]))
	return engine.ReplayDeadLetterResult{
		Outcome:      outcome,
		AuditID:      redisResultString(result[1]),
		ExecutionID:  req.ExecutionID,
		NodeID:       redisResultString(result[2]),
		ActivationID: redisResultString(result[3]),
	}, nil
}

func replayOutcomeFromInt(n int64) engine.DeadLetterReplayOutcome {
	switch n {
	case 1:
		return engine.ReplayReplayed
	case 2:
		return engine.ReplayRejectedTerminal
	case 3:
		return engine.ReplayRejectedInactive
	case 4:
		return engine.ReplayRejectedNodeTerminal
	case 5:
		return engine.ReplayRejectedActivationMismatch
	case 6:
		return engine.ReplayAlreadyReplayed
	default:
		return engine.ReplayNotFound
	}
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
