package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

func remainingNodesKey(t tenant.TenantID, id types.ExecutionID) string {
	return execKey(t, id, "remaining_nodes")
}
func failedNodesKey(t tenant.TenantID, id types.ExecutionID) string { return execKey(t, id, "failed_nodes") }
func advanceMarkerKey(t tenant.TenantID, id types.ExecutionID, name string, activationID int) string {
	return execKey(t, id, fmt.Sprintf("node:%s:advance:%d", name, activationID))
}
func scheduleKey(t tenant.TenantID, id types.ExecutionID, nodeIdx int) string {
	return execKey(t, id, fmt.Sprintf("schedule:%d", nodeIdx))
}
func outboxReadyKey(t tenant.TenantID, id types.ExecutionID) string    { return execKey(t, id, "outbox:ready") }
func outboxBodyKey(t tenant.TenantID, id types.ExecutionID) string     { return execKey(t, id, "outbox:body") }
func outboxAttemptsKey(t tenant.TenantID, id types.ExecutionID) string { return execKey(t, id, "outbox:attempts") }
func outboxDeadKey(t tenant.TenantID, id types.ExecutionID) string    { return execKey(t, id, "outbox:dead") }
func outboxDeadBodyKey(t tenant.TenantID, id types.ExecutionID) string { return execKey(t, id, "outbox:dead:body") }

// outboxDeadMetaKey holds compact immutable per-entry metadata (node + activation)
// as a per-entry hash, written at dead-letter time so the activation-safe replay
// guard can read node/activation without parsing the JSON body. Per-entry hashing
// avoids any delimiter ambiguity under Redis Lua 5.1.
func outboxDeadMetaKey(t tenant.TenantID, id types.ExecutionID, entryID string) string {
	return execKey(t, id, "outbox:dead:meta:"+entryID)
}

// outboxReplayEntryIdxKey maps a dead-letter entry ID to the RequestID of the
// replay that moved it, so a concurrent or retried replay with a different
// RequestID returns already_replayed (with the original receipt) instead of
// degrading to not_found once the dead body is gone.
func outboxReplayEntryIdxKey(t tenant.TenantID, id types.ExecutionID) string {
	return execKey(t, id, "replay:entryidx")
}

// outboxReplayReceiptKey holds the authoritative immutable receipt for one
// replay RequestID. It is written atomically with the dead→ready move and
// survives the loss of the dead body, so a retry with the same RequestID
// recovers the original outcome and AuditID.
func outboxReplayReceiptKey(t tenant.TenantID, id types.ExecutionID, requestID string) string {
	return execKey(t, id, "replay:receipt:"+requestID)
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
// alongside compact node/activation/intent metadata so later replay can guard
// against stale activations without parsing the JSON body, and can branch its
// guard rules by intent source (root/retry/requeue/resume/advance/execute/skip).
// KEYS: 1=outbox:ready 2=outbox:body 3=outbox:attempts 4=outbox:dead 5=outbox:dead:body 6=outbox:dead:meta
// ARGV: 1=entryID 2=maxAttempts 3=now_ms 4=ttl_seconds 5=node_name 6=activation_id
//       7=intent (entry ID prefix: root/retry/requeue/resume/advance/execute/skip)
//       8=task_type (engine.TaskType int; the kind of queued task)
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
    redis.call('HSET', KEYS[6], 'node', ARGV[5], 'activation', ARGV[6], 'intent', ARGV[7], 'task_type', ARGV[8])
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
// ARGV: 1=entryID 2=now_ms 3=ttl_seconds 4=request_id 5=operator 6=reason 7=exec_id 8=tenant
// Returns {outcome, audit_id, node, activation} where outcome:
//   1=replayed 2=rejected_terminal 3=rejected_inactive 4=rejected_node_terminal
//   5=rejected_activation_mismatch 6=already_replayed 7=rejected_metadata_missing
//   0=not_found
//
// Outcome 7 (rejected_metadata_missing) covers BOTH:
//   (a) the per-entry dead-meta hash is absent or lacks node/activation
//       (a legacy entry written before the meta hash existed); AND
//   (b) the node-level guard state is missing or unrecognised — i.e. the
//       node:status key does not exist, holds a value outside the known
//       terminal ∪ eligible-non-terminal allowlist, or node:meta lacks an
//       activation_id field. In any of these cases the activation-staleness
//       guard cannot be evaluated safely, so the entry must NOT move.
//
// Fail-closed contract: when any guard state required to evaluate
// activation-staleness is absent or unrecognised, the script returns outcome 7
// (rejected_metadata_missing) WITHOUT moving the entry. Terminal node statuses
// (success/failed/skipped/canceled/continued) remain distinguished as outcome 4
// (rejected_node_terminal). An immutable receipt is written for every
// determinable rejection (terminal/inactive/node_terminal/activation_mismatch/
// metadata_missing) so a retry with the same RequestID recovers the same
// outcome and AuditID instead of degrading to not_found. The first segment
// reads the stored outcome (not a hardcoded already_replayed) so rejected
// receipts recover as the same rejection.
//
// Eligible non-terminal node statuses are branched by intent (step 5): each
// intent source (root/retry/requeue/resume/advance/execute/skip) has a
// different "safe to replay" precondition, because the lifecycle leaves node
// status in different states when each intent is dead-lettered. A single
// running/committing/waiting allowlist would wrongly reject the typical
// initial/retry/requeue/resume dead-letter (pending/suspended/absent).
//
// Node status/meta keys are derived inside the script from the dead-meta node
// name; all keys share the execution hash tag so they are co-located on a
// single-node (G0) or hash-tagged Cluster (G2) deployment. The tenant ARGV
// carries the brace-less tenant prefix so the derived keys match the
// caller's key schema (xflow:t<tenant>:exec:{<id>}:...). The tenant is
// server-issued (from context), never trusted from a client request body.
var replayDeadLetterLua = redis.NewScript(`
local entryID = ARGV[1]
local nowMs = ARGV[2]
local requestID = ARGV[4]
local execID = ARGV[7]
local tenant = ARGV[8]
local keyPrefix = 'xflow:t' .. tenant .. ':exec:{' .. execID .. '}:'
local receiptKey = keyPrefix .. 'replay:receipt:' .. requestID
local operator = ARGV[5]
local reason = ARGV[6]
local ttl = tonumber(ARGV[3])

-- outcomeCode maps a stored receipt outcome string to the int the caller
-- expects. A 'replayed' receipt recovered under the same RequestID surfaces as
-- already_replayed (6) — the move happened, the caller is just re-confirming.
-- Rejection outcomes recover as themselves so the same RequestID always yields
-- the same stable rejection.
local outcomeCode = function(s)
    if s == 'replayed' then return 6
    elseif s == 'rejected_terminal' then return 2
    elseif s == 'rejected_inactive' then return 3
    elseif s == 'rejected_node_terminal' then return 4
    elseif s == 'rejected_activation_mismatch' then return 5
    elseif s == 'rejected_metadata_missing' then return 7
    end
    -- Unknown outcomes fall back to 6 (already_replayed). This is defensive:
    -- writeReceipt must only ever persist the outcome strings listed above.
    -- If a new outcome is added to writeReceipt without a matching branch here,
    -- it would silently surface as already_replayed; keep this mapping in sync.
    return 6
end

-- decode a receipt hash into outcome/node/activation/audit_id
local readReceipt = function(key)
    if redis.call('EXISTS', key) == 0 then return nil end
    return {
        outcome = redis.call('HGET', key, 'outcome') or '',
        node = redis.call('HGET', key, 'node') or '',
        activation = redis.call('HGET', key, 'activation') or '',
        audit_id = redis.call('HGET', key, 'audit_id') or '',
    }
end

-- writeReceipt persists an immutable receipt. It carries only operational
-- metadata (no task payload/body) so the audit trail never logs sensitive
-- request content. The audit_id is requestID:nowMs so it is stable for the
-- lifetime of one RequestID and recoverable on retry.
local writeReceipt = function(key, outcomeStr, node, activation, auditID)
    redis.call('HSET', key,
        'node', node,
        'activation', activation,
        'audit_id', auditID,
        'outcome', outcomeStr,
        'operator', operator,
        'reason', reason,
        'entry_id', entryID,
        'ts_ms', nowMs)
    redis.call('EXPIRE', key, ttl)
end

-- 1. Idempotency: same RequestID already produced a receipt -> recover the
--    stored outcome + original audit_id (covers both replayed and rejections).
local r = readReceipt(receiptKey)
if r then
    return {outcomeCode(r.outcome), r.audit_id, r.node, r.activation}
end
-- Different RequestID for an already-replayed entry? The entry index is only
-- written on a successful dead->ready move, so a hit here means the entry was
-- moved under another RequestID: collapse to that receipt.
local priorReqID = redis.call('HGET', KEYS[8], entryID)
if priorReqID and priorReqID ~= '' then
    local priorKey = keyPrefix .. 'replay:receipt:' .. priorReqID
    local pr = readReceipt(priorKey)
    if pr then
        return {outcomeCode(pr.outcome), pr.audit_id, pr.node, pr.activation}
    end
end

-- 2. Read dead body + immutable meta (per-entry hash fields). Body missing
--    means the entry is gone (replayed then expired, or never dead-lettered):
--    a stable not_found with no receipt.
local body = redis.call('HGET', KEYS[2], entryID)
if not body then
    return {0, '', '', ''}
end
local nodeName = redis.call('HGET', KEYS[7], 'node') or ''
local entryActivation = redis.call('HGET', KEYS[7], 'activation') or ''

-- 3. Fail-closed metadata guard: if the per-entry meta is absent or missing
--    node/activation (a legacy entry), the node/activation guards cannot be
--    evaluated. Do NOT move; write a recoverable rejection receipt.
local intent = redis.call('HGET', KEYS[7], 'intent') or ''
if nodeName == '' or entryActivation == '' then
    local auditID = requestID .. ':' .. nowMs
    writeReceipt(receiptKey, 'rejected_metadata_missing', nodeName, entryActivation, auditID)
    return {7, auditID, nodeName, entryActivation}
end

-- 4. Execution status guard. Terminal/inactive executions reject with a
--    recoverable receipt (node/activation from meta are available here).
--    canceling is NOT in the eligible allowlist: a cancel must not be bypassed
--    by replay (the control-plane cancel flow owns retry/requeue cleanup during
--    canceling). Only the explicit "running" execution status proceeds.
local status = redis.call('GET', KEYS[6])
if not status then
    local auditID = requestID .. ':' .. nowMs
    writeReceipt(receiptKey, 'rejected_inactive', nodeName, entryActivation, auditID)
    return {3, auditID, nodeName, entryActivation}
end
if status ~= 'running' then
    local auditID = requestID .. ':' .. nowMs
    writeReceipt(receiptKey, 'rejected_terminal', nodeName, entryActivation, auditID)
    return {2, auditID, nodeName, entryActivation}
end

-- 5. Intent-branched node guard. The previous single allowlist
--    (running/committing/waiting) wrongly rejected the typical dead-letter:
--    resetNodeForRetry/revokeLease set node:status=pending before enqueueing
--    retry/requeue; CreateExecutionWithOutbox writes root entries with no
--    node:status yet; Suspend writes resume entries with node suspended. Each
--    intent has a different "safe to replay" precondition, evaluated below.
--    An empty/unknown intent is a legacy entry → fail-closed outcome 7.
local nodeStatusKey = keyPrefix .. 'node:' .. nodeName .. ':status'
local nodeMetaKey   = keyPrefix .. 'node:' .. nodeName .. ':meta'
local nstatus = redis.call('GET', nodeStatusKey)
local auditID = requestID .. ':' .. nowMs

local isTerminal = nstatus == 'success' or nstatus == 'failed' or nstatus == 'skipped'
    or nstatus == 'canceled' or nstatus == 'continued'

-- Intent-branched terminal/non-terminal guard.
--
-- advance is the exception to the terminal-node rule. An advance entry's
-- source node is ALWAYS terminal: commitNodeLua writes the source terminal
-- and the advance outbox in one atomic script (the entry cannot exist
-- otherwise). A terminal source is therefore the REQUIRED precondition for
-- replaying an advance — the old "terminal rejects regardless of intent"
-- branch made every real advance dead-letter return
-- rejected_node_terminal and left downstream nodes un-advanced. Idempotency
-- on redelivery is guaranteed by the advance marker (advanceNodeLua SET NX
-- on advanceMarkerKey), NOT by the node status. A non-terminal or absent
-- source for an advance entry means the guard state is inconsistent with the
-- advance lifecycle → fail-closed (7).
if intent == 'advance' then
    if not isTerminal then
        writeReceipt(receiptKey, 'rejected_metadata_missing', nodeName, entryActivation, auditID)
        return {7, auditID, nodeName, entryActivation}
    end
    -- terminal source confirmed; fall through to the activation guard.
else
    -- every non-advance intent: a terminal node means the dead-lettered task
    -- has already completed → must not re-deliver (4).
    if isTerminal then
        writeReceipt(receiptKey, 'rejected_node_terminal', nodeName, entryActivation, auditID)
        return {4, auditID, nodeName, entryActivation}
    end
    -- nodeAllows reports whether nstatus is an eligible non-terminal value
    -- for the given intent. nstatus may be false (key absent) for root/execute/
    -- skip, which is normal at scheduling time. advance is handled above and
    -- is intentionally absent here.
    local nodeAllows = function(value)
        if intent == 'root' then
            -- initial root intent: node has not started executing; no node:status is
            -- expected. Any non-terminal value is acceptable; absence is acceptable.
            return value == false or value == 'pending' or value == 'running'
                or value == 'committing' or value == 'waiting'
        elseif intent == 'retry' or intent == 'requeue' then
            -- reset/revoke set node:status=pending before enqueueing; dispatcher may
            -- have since moved it to running/committing/waiting. suspended/missing/
            -- unknown is corrupt guard state.
            return value == 'pending' or value == 'running'
                or value == 'committing' or value == 'waiting'
        elseif intent == 'resume' then
            -- resume (signal/timer/timeout) targets a suspended or pending node.
            return value == 'suspended' or value == 'pending'
        elseif intent == 'execute' or intent == 'skip' then
            -- scheduling-stage intents: target node may have no status yet, or be
            -- pending/running/committing/waiting once dispatched. suspended is not
            -- a valid target for a fresh schedule.
            return value == false or value == 'pending' or value == 'running'
                or value == 'committing' or value == 'waiting'
        end
        -- unknown/empty intent (legacy entry): fail-closed.
        return false
    end
    if not nodeAllows(nstatus) then
        writeReceipt(receiptKey, 'rejected_metadata_missing', nodeName, entryActivation, auditID)
        return {7, auditID, nodeName, entryActivation}
    end
end

-- 6. Activation guard (fail-closed). For intents whose node has a current
--    activation, the entry's activation must match — stale cyclic re-entry is
--    rejected. root/execute/skip skip this when node:status is absent (the node
--    has not started, so there is no current activation to compare and no
--    staleness risk); when node:status IS present they still require the
--    current activation to match. advance always reaches here with a terminal
--    nstatus (handled above), so its activation guard always runs (cyclic
--    re-entry check against the source node's current activation).
local skipActivation = (intent == 'root' or intent == 'execute'
    or intent == 'skip') and nstatus == false
if not skipActivation then
    local currentActivation = redis.call('HGET', nodeMetaKey, 'activation_id') or ''
    if currentActivation == '' then
        writeReceipt(receiptKey, 'rejected_metadata_missing', nodeName, entryActivation, auditID)
        return {7, auditID, nodeName, entryActivation}
    end
    if currentActivation ~= entryActivation then
        writeReceipt(receiptKey, 'rejected_activation_mismatch', nodeName, entryActivation, auditID)
        return {5, auditID, nodeName, entryActivation}
    end
end

-- 6. Atomic dead->ready move: preserve body, reset attempts.
redis.call('HSET', KEYS[4], entryID, body)
redis.call('ZADD', KEYS[3], tonumber(nowMs), entryID)
redis.call('HDEL', KEYS[5], entryID)
redis.call('ZREM', KEYS[1], entryID)
redis.call('HDEL', KEYS[2], entryID)
redis.call('DEL', KEYS[7])
redis.call('EXPIRE', KEYS[3], ttl)
redis.call('EXPIRE', KEYS[4], ttl)
redis.call('EXPIRE', KEYS[5], ttl)

-- 7. Write authoritative immutable receipt + entry index.
local auditID = requestID .. ':' .. nowMs
writeReceipt(receiptKey, 'replayed', nodeName, entryActivation, auditID)
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

// listDeadLettersAfterFetchHook is a test-only hook invoked after
// ListDeadLetters fetches the dead-letter index but before it constructs the
// next cursor. It exists solely to exercise races that are no longer possible
// after the WithScores fetch made score capture atomic.
var listDeadLettersAfterFetchHook func()

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
	t := tenant.FromContext(ctx)
	result, err := resetNodeForRetryWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("reset node for retry with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
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
	t := tenant.FromContext(ctx)
	result, err := revokeLeaseWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
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
	t := tenant.FromContext(ctx)
	result, err := commitNodeLua.Run(ctx, s.rdb, []string{
		execKey(t, req.ExecutionID, "status"),
		execKey(t, req.ExecutionID, "error"),
		remainingNodesKey(t, req.ExecutionID),
		failedNodesKey(t, req.ExecutionID),
		nodeStatusKey(t, req.ExecutionID, req.NodeName),
		nodeMetaKey(t, req.ExecutionID, req.NodeName),
		outputKey(t, req.ExecutionID, req.NodeName),
		leaseExpiryZSetKey(t, req.ExecutionID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
		scheduleKey(t, req.ExecutionID, req.NodeIdx),
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
	t := tenant.FromContext(ctx)
	keys := []string{
		execKey(t, req.ExecutionID, "status"),
		nodeStatusKey(t, req.ExecutionID, req.NodeName),
		nodeMetaKey(t, req.ExecutionID, req.NodeName),
		advanceMarkerKey(t, req.ExecutionID, req.NodeName, req.ActivationID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
	}
	args := []any{req.ActivationID, int(ttl.Seconds()), len(req.Arrivals), time.Now().UTC().UnixMilli()}
	outboxIDs := make([]string, 0, len(req.Arrivals))
	for _, arrival := range req.Arrivals {
		keys = append(keys,
			inDegreeKey(t, req.ExecutionID, arrival.NodeIdx),
			activeInputsKey(t, req.ExecutionID, arrival.NodeIdx),
			scheduleKey(t, req.ExecutionID, arrival.NodeIdx),
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
	t := tenant.FromContext(ctx)
	ids, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: outboxReadyKey(t, id), Start: "-inf", Stop: fmt.Sprintf("%d", before.UnixMilli()), ByScore: true, Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list outbox %q: %w", id, err)
	}
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		raw, err := s.rdb.HGet(ctx, outboxBodyKey(t, id), entryID).Result()
		if err == redis.Nil {
			_ = ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(t, id), outboxBodyKey(t, id), outboxAttemptsKey(t, id)}, entryID).Err()
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
	t := tenant.FromContext(ctx)
	if err := ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(t, id), outboxBodyKey(t, id), outboxAttemptsKey(t, id)}, entryID).Err(); err != nil {
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
	t := tenant.FromContext(ctx)
	result, err := recordOutboxFailureLua.Run(ctx, s.rdb, []string{
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
	}, entry.ID, maxAttempts, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		entry.Task.NodeName, entry.Task.ActivationID,
		deadLetterIntent(entry.ID), int(entry.Task.Type)).Slice()
	if err != nil {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: %w", id, entry.ID, err)
	}
	if len(result) != 2 {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: unexpected result %v", id, entry.ID, result)
	}
	failure := engine.OutboxDeliveryFailure{Attempts: int(redisResultInt(result[0])), DeadLettered: redisResultInt(result[1]) == 1}
	if err := s.refreshTransientTTL(ctx, id,
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
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
	tenants, err := s.listTenants(ctx)
	if err != nil {
		return engine.OutboxMetricsSnapshot{}, fmt.Errorf("list tenants for outbox metrics: %w", err)
	}
	for _, t := range tenants {
		if err := s.scanOutboxMetricsForTenant(ctx, t, &snapshot); err != nil {
			return engine.OutboxMetricsSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (s *Store) scanOutboxMetricsForTenant(ctx context.Context, t tenant.TenantID, snapshot *engine.OutboxMetricsSnapshot) error {
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan pending outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count pending outbox %q: %w", key, err)
			}
			snapshot.Pending += int(count)
			if count == 0 {
				continue
			}
			id, ok := executionIDFromKey(key)
			if !ok {
				continue
			}
			entries, err := s.rdb.HVals(ctx, outboxBodyKey(t, id)).Result()
			if err != nil {
				return fmt.Errorf("read pending outbox bodies %q: %w", key, err)
			}
			for _, raw := range entries {
				entry, err := unmarshalRedisOutboxEntry(raw)
				if err != nil {
					return fmt.Errorf("decode pending outbox body %q: %w", key, err)
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
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:dead"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan dead-letter outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count dead-letter outbox %q: %w", key, err)
			}
			snapshot.DeadLettered += int(count)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// ListDeadLetters returns one page of dead-lettered outbox entries for one
// execution, ordered oldest-first by (score, member) — i.e. dead-letter
// timestamp, with entry ID as the tie-break so same-millisecond dead-letters
// still paginate stably. It reads the dead-letter body hash directly; entries
// are not removed. Pagination uses an opaque HMAC-signed cursor carrying the
// (score, member) resume point; an empty cursor starts from the oldest entry.
// The cursor is bound to the execution and signed with a process-local key,
// so a tampered, cross-execution, or stale (post-restart) cursor returns
// ErrCursorExpired and the caller must restart from the first page. limit<=0
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
	t := tenant.FromContext(ctx)
	deadKey := outboxDeadKey(t, id)

	cursorScore, cursorMember, err := s.decodeDeadLetterCursor(page.Cursor, id)
	if err != nil {
		return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, err)
	}

	// Stable (score, member) pagination. The dead-letter index is a ZSET keyed
	// by dead-letter timestamp (now_ms) with the unique entry ID as member, so
	// Redis' native (score, member) ordering gives a stable total order. To
	// resume strictly after (cursorScore, cursorMember) we fetch the same-score
	// tail (members with score == cursorScore and member > cursorMember) and
	// the strictly-higher scores, then concatenate. Over-fetch by one to detect
	// a next page without an extra round-trip.
	// captured atomically with its member, so the next cursor can be signed
	// without a separate ZScore round-trip. That removes the race where the
	// boundary entry is replayed/deleted between listing and scoring.
	type deadLetterIndexEntry struct {
		Member string
		Score  float64
	}

	var entries []deadLetterIndexEntry
	if page.Cursor == "" {
		zs, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key: deadKey, Start: "-inf", Stop: "+inf", ByScore: true, Offset: 0, Count: int64(page.Limit + 1),
		}).Result()
		if err != nil {
			return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, err)
		}
		entries = make([]deadLetterIndexEntry, 0, len(zs))
		for _, z := range zs {
			m, ok := z.Member.(string)
			if !ok {
				continue
			}
			entries = append(entries, deadLetterIndexEntry{Member: m, Score: z.Score})
		}
	} else {
		// Same-score tail: members at cursorScore, filtered in Go to member >
		// cursorMember (Redis orders same-score members by member lex, so the
		// slice is already in correct order).
		sameScore, serr := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key: deadKey, Start: formatScore(cursorScore), Stop: formatScore(cursorScore), ByScore: true, Offset: 0, Count: -1,
		}).Result()
		if serr != nil {
			return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, serr)
		}
		entries = make([]deadLetterIndexEntry, 0, len(sameScore)+page.Limit+1)
		for _, z := range sameScore {
			m, ok := z.Member.(string)
			if !ok {
				continue
			}
			if m > cursorMember {
				entries = append(entries, deadLetterIndexEntry{Member: m, Score: z.Score})
			}
		}
		// Strictly-higher scores. Bound by limit+1 minus what we already have,
		// but keep at least limit+1 so the next-page detection stays correct
		// when the same-score tail was shorter than a page.
		higher, herr := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key: deadKey, Start: "(" + formatScore(cursorScore), Stop: "+inf", ByScore: true, Offset: 0, Count: int64(page.Limit + 1),
		}).Result()
		if herr != nil {
			return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, herr)
		}
		for _, z := range higher {
			m, ok := z.Member.(string)
			if !ok {
				continue
			}
			entries = append(entries, deadLetterIndexEntry{Member: m, Score: z.Score})
		}
	}

	if listDeadLettersAfterFetchHook != nil {
		listDeadLettersAfterFetchHook()
	}

	var nextCursor string
	if len(entries) > page.Limit {
		// Build the next cursor from the (score, member) of the last entry on
		// this page. The score was captured atomically with the member in the
		// ZRangeWithScores fetch above, so a concurrent replay/delete of the
		// boundary entry cannot leave the cursor empty or drop later entries.
		boundary := entries[page.Limit-1]
		nextCursor = s.encodeDeadLetterCursor(id, boundary.Score, boundary.Member)
		entries = entries[:page.Limit]
	}

	out := make([]engine.OutboxEntry, 0, len(entries))
	for _, e := range entries {
		raw, err := s.rdb.HGet(ctx, outboxDeadBodyKey(t, id), e.Member).Result()
		if err == redis.Nil {
			// body missing while index still references it: self-heal by removing the stale index entry
			_ = s.rdb.ZRem(ctx, deadKey, e.Member).Err()
			continue
		}
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("read dead letter %q/%q: %w", id, e.Member, err)
		}
		entry, err := unmarshalRedisOutboxEntry(raw)
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("decode dead letter %q/%q: %w", id, e.Member, err)
		}
		out = append(out, entry)
	}
	return engine.DeadLetterList{Entries: out, NextCursor: nextCursor}, nil
}

// formatScore renders a ZSET score as the exact string Redis round-trips, so a
// cursor's exclusive/inclusive score bounds match the stored member score
// byte-for-byte. Redis scores are IEEE-754 doubles; 'f' with -1 precision
// produces the decimal representation Go and Redis agree on for millisecond
// timestamps.
func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
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
	t := tenant.FromContext(ctx)
	result, err := replayDeadLetterLua.Run(ctx, s.rdb, []string{
		outboxDeadKey(t, req.ExecutionID),
		outboxDeadBodyKey(t, req.ExecutionID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
		outboxAttemptsKey(t, req.ExecutionID),
		execKey(t, req.ExecutionID, "status"),
		outboxDeadMetaKey(t, req.ExecutionID, req.EntryID),
		outboxReplayEntryIdxKey(t, req.ExecutionID),
	}, req.EntryID, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		requestID, req.Operator, req.Reason, string(req.ExecutionID), string(t)).Slice()
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
	case 7:
		return engine.ReplayRejectedMetadataMissing
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
	tenants, err := s.listTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tenants for outbox discovery: %w", err)
	}
	for _, t := range tenants {
		if len(ids) >= limit {
			break
		}
		if err := s.scanOutboxExecutionsForTenant(ctx, t, limit, ids); err != nil {
			return nil, err
		}
	}
	out := make([]types.ExecutionID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *Store) scanOutboxExecutionsForTenant(ctx context.Context, t tenant.TenantID, limit int, ids map[types.ExecutionID]struct{}) error {
	var cursor uint64
	for len(ids) < limit {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan outbox indexes: %w", err)
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
	return nil
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

// deadLetterIntent extracts the intent prefix from an outbox entry ID
// (root/retry/requeue/resume/advance/execute/skip). It is written into the
// dead-letter meta hash at dead-letter time so replayDeadLetterLua can branch
// its guard rules by intent source instead of applying a single
// running/committing/waiting allowlist that wrongly rejects the typical
// initial/retry/requeue/resume dead-letters (whose node status is pending,
// suspended, or absent at dead-letter time). An empty/unknown intent falls
// back to "" and is treated by replay as legacy metadata → outcome 7.
func deadLetterIntent(entryID string) string {
	for i := 0; i < len(entryID); i++ {
		if entryID[i] == '/' {
			return entryID[:i]
		}
	}
	return ""
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

func (s *Store) evictExecutionCaches(id types.ExecutionID) {
	s.mu.Lock()
	delete(s.graphs, id)
	s.mu.Unlock()
	s.ttlMu.Lock()
	delete(s.execTTLs, id)
	s.ttlMu.Unlock()
}
