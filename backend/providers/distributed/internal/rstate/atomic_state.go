package rstate

import (
	"github.com/redis/go-redis/v9"
)

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
        -- Guard: do not overwrite canceling/terminal execution status (same
        -- fence as updateExecutionStatusLua). A concurrent Cancel may have
        -- transitioned the execution to canceling/canceled while nodes were
        -- still completing; honor the cancel rather than stomping it.
        local curExec = redis.call('GET', KEYS[1])
        if curExec == 'canceling' or curExec == 'canceled' or curExec == 'timeout' or curExec == 'success' or curExec == 'failed' then
            done = 0
        else
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
end
if done == 0 and tonumber(ARGV[12]) == 0 and ARGV[15] ~= '' and redis.call('GET', KEYS[1]) ~= 'canceling' then
    if redis.call('HSETNX', KEYS[10], ARGV[15], ARGV[17]) == 1 then
        redis.call('ZADD', KEYS[9], tonumber(ARGV[18]), ARGV[15])
        redis.call('EXPIRE', KEYS[9], ttl)
        redis.call('EXPIRE', KEYS[10], ttl)
    end
end
if tonumber(ARGV[16]) == 1 and done == 0 and tonumber(ARGV[12]) == 0 and redis.call('GET', KEYS[1]) ~= 'canceling' then
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
        -- Guard: do not overwrite canceling/terminal (same fence as above).
        local curExec2 = redis.call('GET', KEYS[1])
        if curExec2 ~= 'canceling' and curExec2 ~= 'canceled' and curExec2 ~= 'timeout' and curExec2 ~= 'success' and curExec2 ~= 'failed' then
            finalStatus = ARGV[20]
            redis.call('SET', KEYS[1], finalStatus, 'EX', ttl)
            if finalStatus == 'failed' and ARGV[21] ~= '' then
                redis.call('SET', KEYS[2], ARGV[21], 'EX', ttl)
            end
            done = 1
        end
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
-- Defense in depth: do not schedule any downstream work while a Cancel is in
-- flight (execution 'canceling'). cleanupOnCancel wipes the outbox when the
-- execution reaches 'canceled', but blocking new advances during the canceling
-- window avoids racing a downstream schedule against that teardown.
if executionStatus == 'canceling' then
    return 0
end
-- Fail closed when the source node's activation_id field is absent or empty:
-- the activation-staleness guard cannot be evaluated safely, so the advance
-- must not proceed (and must not mutate the marker, counters, or outbox).
-- Only when a real activation_id is present do we compare it against the
-- task's activation. This rejects the prior defect where a missing field
-- was coerced to 0 by 'or 0' and matched an activation-0 task.
local rawActivation = redis.call('HGET', KEYS[3], 'activation_id')
if rawActivation == false or rawActivation == nil or rawActivation == '' then
    return 0
end
local storedActivation = tonumber(rawActivation)
if storedActivation == nil or storedActivation ~= tonumber(ARGV[1]) then
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
//
// When an entry is moved to dead-letter storage, any prior replay:entryidx
// mapping for this entryID is cleared (KEYS[7]). This allows a subsequent
// ReplayDeadLetter with a new RequestID to proceed rather than being
// permanently rejected as already_replayed from a prior cycle's stale index.
//
// Ghost dead-letter guard: the leading body-existence check (KEYS[2]) is the
// complete guard against dead-lettering an entry whose delivery already
// succeeded and was acked. The delivery protocol keeps every ready entry in the
// ready ZSET (KEYS[1]) until AckOutbox; ack is the *only* removal path outside
// this script, and ackOutboxLua removes the ready-ZSET member and the body hash
// field TOGETHER in one atomic Lua (ZREM ready + HDEL body). Every producer
// (create/commit/advance/retry/requeue/resume) likewise writes the body and the
// ready member together. So body-presence and ready-membership are always
// observably equivalent: there is no state where the body is gone but the entry
// is still in ready, or vice versa. Because Redis runs each Lua script to
// completion with no interleaving, a lagging failure report that runs AFTER an
// ack always finds no body here and returns {0,0} before reaching the
// dead-letter branch — it can never resurrect an acked entry into dead-letter.
// A redundant ZSCORE(KEYS[1]) check would therefore add nothing: it can only
// differ from the body check in a state that never occurs, and it cannot help
// the inverse ordering (a failure report whose atomic block runs just BEFORE
// the ack), because at that instant the entry is legitimately still in ready.
// That pre-ack ordering is inherent to at-least-once delivery reporting and is
// owned by the caller (a delivery that succeeded acks and does not also report
// failure), not solvable by any read-side guard in this script.
//
// KEYS: 1=outbox:ready 2=outbox:body 3=outbox:attempts 4=outbox:dead 5=outbox:dead:body 6=outbox:dead:meta 7=replay:entryidx
// ARGV: 1=entryID 2=maxAttempts 3=now_ms 4=ttl_seconds 5=node_name 6=activation_id
//
//	7=intent (entry ID prefix: root/retry/requeue/resume/advance/execute/skip)
//	8=task_type (engine.TaskType int; the kind of queued task)
var recordOutboxFailureLua = redis.NewScript(`
local body = redis.call('HGET', KEYS[2], ARGV[1])
if not body then
    -- Ghost dead-letter guard: no body means the entry was already acked
    -- (ackOutboxLua HDELs the body and ZREMs ready atomically) or never
    -- existed. Either way a delivery cannot be failed into dead-letter here,
    -- so return the no-op {attempts=0, deadLettered=0} shape. This fully
    -- closes the post-ack race; see the doc comment above for why an extra
    -- ZSCORE(KEYS[1]) check would be redundant.
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
    -- Clear stale replay:entryidx mapping so a second dead-letter cycle does
    -- not permanently block re-replay with a new RequestID.
    redis.call('HDEL', KEYS[7], ARGV[1])
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
//
//	5=outbox:attempts 6=exec:status 7=outbox:dead:meta 8=replay:entryidx
//
// ARGV: 1=entryID 2=now_ms 3=ttl_seconds 4=request_id 5=operator 6=reason 7=exec_id 8=namespace
// Returns {outcome, audit_id, node, activation} where outcome:
//
//	1=replayed 2=rejected_terminal 3=rejected_inactive 4=rejected_node_terminal
//	5=rejected_activation_mismatch 6=already_replayed 7=rejected_metadata_missing
//	0=not_found
//
// Outcome 7 (rejected_metadata_missing) covers BOTH:
//
//	(a) the per-entry dead-meta hash is absent or lacks node/activation
//	    (a legacy entry written before the meta hash existed); AND
//	(b) the node-level guard state is missing or unrecognised — i.e. the
//	    node:status key does not exist, holds a value outside the known
//	    terminal ∪ eligible-non-terminal allowlist, or node:meta lacks an
//	    activation_id field. In any of these cases the activation-staleness
//	    guard cannot be evaluated safely, so the entry must NOT move.
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
// single-node (G0) or hash-tagged Cluster (G2) deployment. The namespace ARGV
// carries the brace-less namespace prefix so the derived keys match the
// caller's key schema (xflow:ns:<namespace>:exec:{<id>}:...). The namespace is
// server-issued (from context), never trusted from a client request body.
var replayDeadLetterLua = redis.NewScript(`
local entryID = ARGV[1]
local nowMs = ARGV[2]
local requestID = ARGV[4]
local execID = ARGV[7]
local namespace = ARGV[8]
local keyPrefix = 'xflow:ns:' .. namespace .. ':exec:{' .. execID .. '}:'
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
