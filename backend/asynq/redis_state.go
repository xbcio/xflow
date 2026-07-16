package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

const defaultExecTTL = 24 * time.Hour

// ---------------------------------------------------------------------------
// Redis key helpers — all keys use {id} as a hash tag for cluster compatibility
// ---------------------------------------------------------------------------

func execKey(id types.ExecutionID, suffix string) string {
	return fmt.Sprintf("xflow:exec:{%s}:%s", id, suffix)
}

func nodeStatusKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:status", id, name)
}

func nodeMetaKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:meta", id, name)
}

func outputKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:output:%s", id, name)
}

func signalKey(id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:signal:%s", id, signalName)
}

func waiterKey(id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:waiter:%s", id, signalName)
}

func waiterSpecKey(id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:waiter_spec", id, nodeName)
}

func signalBatchKey(id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:signals", id, nodeName)
}

func inDegreeKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:exec:{%s}:indegree:%d", id, nodeIdx)
}

func activeInputsKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:exec:{%s}:active_inputs:%d", id, nodeIdx)
}

func resumeLockKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:resume_lock", id, name)
}

func suspendedNodesKey(id types.ExecutionID) string {
	return fmt.Sprintf("xflow:exec:{%s}:suspended_nodes", id)
}

// leaseExpiryZSetKey is the execution-scoped lease-deadline index used by
// the sweeper. It shares the execution hash tag, so lease state, metadata,
// and expiry discovery can be updated in one Redis Cluster-safe Lua script.
func leaseExpiryZSetKey(id types.ExecutionID) string {
	return execKey(id, "leases")
}

// leaseExpiryMember packs execID and node name into a ZSET member. Retaining
// the execution ID makes index reconciliation robust against malformed keys
// and legacy members during rolling upgrades.
func leaseExpiryMember(id types.ExecutionID, name string) string {
	return string(id) + "|" + name
}

// splitLeaseMember reverses leaseExpiryMember. Returns ok=false when the
// member is malformed (should never happen in prod but guards against dirty
// data).
func splitLeaseMember(member string) (types.ExecutionID, string, bool) {
	idx := strings.IndexByte(member, '|')
	if idx <= 0 {
		return "", "", false
	}
	return types.ExecutionID(member[:idx]), member[idx+1:], true
}

// ---------------------------------------------------------------------------
// Lua scripts
// ---------------------------------------------------------------------------

// propagateLua atomically decrements in-degree and increments active inputs.
// Returns {newInDegree, activeInputCount}.
var propagateLua = redis.NewScript(`
local isActive = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
if isActive == 1 then
    redis.call('INCR', KEYS[2])
    redis.call('EXPIRE', KEYS[2], ttl)
end
local newVal = redis.call('DECR', KEYS[1])
local ai = tonumber(redis.call('GET', KEYS[2]) or '0')
return {newVal, ai}
`)

// suspendOrConsumeLua atomically checks for an existing signal or parks the node.
// KEYS[1] = signal key, KEYS[2] = node status key, KEYS[3] = waiter key,
// KEYS[4] = suspended_nodes SET, KEYS[5] = resume_lock key
// ARGV[1] = node name, ARGV[2] = ttl seconds
// Returns signal payload JSON (consumed) or nil (suspended).
var suspendOrConsumeLua = redis.NewScript(`
local signal = redis.call('GET', KEYS[1])
if signal then
    redis.call('DEL', KEYS[1])
    return signal
end
redis.call('DEL', KEYS[5])
redis.call('SET', KEYS[2], 'suspended', 'EX', ARGV[2])
redis.call('SET', KEYS[3], ARGV[1], 'EX', ARGV[2])
redis.call('SADD', KEYS[4], ARGV[1])
return nil
`)

// suspendOrConsumeMultiCollectLua atomically consumes every pre-delivered
// signal of a multi-signal suspend into the batch hash in one transition.
// Doing GET→HSET→DEL per signal as separate commands left a window where a
// crash after DEL removed a signal before the quorum check parked the node,
// losing the signal. This script collects all ready signals atomically; even
// if the caller crashes before the quorum check, the signals remain in the
// batch hash for the next suspend/deliver attempt.
// KEYS[1] = signal batch hash key, KEYS[2..] = signal keys (one per awaited signal)
// ARGV[1] = ttl seconds, ARGV[2..] = signal names (parallel to KEYS[2..])
// Returns a flat array [name, raw, name, raw, ...] of consumed signals.
var suspendOrConsumeMultiCollectLua = redis.NewScript(`
local batchKey = KEYS[1]
local ttl = tonumber(ARGV[1])
local collected = {}
for i = 2, #KEYS do
    local signalKey = KEYS[i]
    local signalName = ARGV[i]
    local raw = redis.call('GET', signalKey)
    if raw then
        redis.call('DEL', signalKey)
        redis.call('HSET', batchKey, signalName, raw)
        table.insert(collected, signalName)
        table.insert(collected, raw)
    end
end
if #collected > 0 then
    redis.call('EXPIRE', batchKey, ttl)
end
return collected
`)

// deliverSignalWithOutboxLua atomically consumes an external signal that wakes
// a suspended waiter and writes the resume delivery intent to the outbox in the
// same transition. This closes the legacy window where signalOrStoreLua consumed
// the signal (DEL waiter + SREM suspended) and the engine then enqueued the
// resume task separately — a crash between them lost the resume permanently.
//
// Single-signal: GET waiter; if present, DEL signal+waiter+resumeLock+waiterSpec,
// SREM suspended, ZREM timeout, and write the resume outbox entry (whose body
// already carries the payload). If no waiter, SET the signal for later.
//
// Multi-signal: HSET the signal into the batch; if quorum reached, build the All
// payload into the entry body, clean up waiters/spec/batch, and write the entry.
// If quorum not yet reached, return "" (stored in batch).
//
// KEYS[1] = signal key, KEYS[2] = waiter key, KEYS[3] = suspended_nodes SET,
// KEYS[4] = signal batch hash (multi), KEYS[5] = waiter spec key,
// KEYS[6] = resume lock key, KEYS[7] = outbox ready ZSET, KEYS[8] = outbox body hash,
// KEYS[9] = timeout ZSET, KEYS[10..] = multi waiter keys (cleanup on quorum).
// ARGV[1] = signal data JSON, ARGV[2] = ttl seconds, ARGV[3] = node name,
// ARGV[4] = outbox entry id, ARGV[5] = outbox entry body JSON,
// ARGV[6] = now ms, ARGV[7] = multi flag, ARGV[8] = quorum,
// ARGV[9] = signal name, ARGV[10] = timeout member.
// Returns nodeName (committed) or "" (stored / quorum not reached).
var deliverSignalWithOutboxLua = redis.NewScript(`
local signalKey = KEYS[1]
local waiterKey = KEYS[2]
local suspendedKey = KEYS[3]
local batchKey = KEYS[4]
local waiterSpecKey = KEYS[5]
local resumeLockKey = KEYS[6]
local outboxReady = KEYS[7]
local outboxBody = KEYS[8]
local timeoutKey = KEYS[9]
local dataJSON = ARGV[1]
local ttl = tonumber(ARGV[2])
local nodeName = ARGV[3]
local entryID = ARGV[4]
local entryBody = ARGV[5]
local nowMs = tonumber(ARGV[6])
local multi = tonumber(ARGV[7]) == 1
local quorum = tonumber(ARGV[8])
local signalName = ARGV[9]
local timeoutMember = ARGV[10]

local waiter = redis.call('GET', waiterKey)
if not waiter then
    redis.call('SET', signalKey, dataJSON, 'EX', ttl)
    return ''
end

local writeOutbox = function(body)
    redis.call('HSETNX', outboxBody, entryID, body)
    redis.call('ZADD', outboxReady, nowMs, entryID)
    redis.call('EXPIRE', outboxReady, ttl)
    redis.call('EXPIRE', outboxBody, ttl)
end

if multi then
    redis.call('HSET', batchKey, signalName, dataJSON)
    redis.call('EXPIRE', batchKey, ttl)
    local values = redis.call('HGETALL', batchKey)
    if #values / 2 < quorum then
        return ''
    end
    local all = {}
    for i = 1, #values, 2 do
        all[values[i]] = cjson.decode(values[i + 1])
    end
    local entry = cjson.decode(entryBody)
    entry.task.payload.All = all
    for i = 10, #KEYS do
        redis.call('DEL', KEYS[i])
    end
    redis.call('DEL', waiterSpecKey, batchKey, resumeLockKey)
    redis.call('SREM', suspendedKey, nodeName)
    if timeoutMember ~= '' then
        redis.call('ZREM', timeoutKey, timeoutMember)
    end
    writeOutbox(cjson.encode(entry))
    return nodeName
end

redis.call('DEL', signalKey, waiterKey, resumeLockKey, waiterSpecKey)
redis.call('SREM', suspendedKey, nodeName)
if timeoutMember ~= '' then
    redis.call('ZREM', timeoutKey, timeoutMember)
end
writeOutbox(entryBody)
return nodeName
`)

// updateExecutionStatusLua atomically compares-and-sets the execution status
// with cancel-aware fencing. It prevents a cyclic completion (or any
// post-cancel transition) from overwriting a status that Cancel already drove
// to a terminal or canceling state:
//
//   - A terminal status (success/failed/canceled/timeout) is never overwritten.
//   - canceling blocks any non-canceled write, so a concurrent completeExecution
//     (→ success/failed) cannot stomp the in-flight cancellation.
//   - canceled→canceled and running→canceling/canceled proceed normally.
//
// KEYS[1] = status key, KEYS[2] = error key (may be empty to skip error write)
// ARGV[1] = new status, ARGV[2] = error message ("" = none), ARGV[3] = ttl seconds
// Returns 1 (applied) or 0 (skipped: fenced by a terminal/canceling status).
var updateExecutionStatusLua = redis.NewScript(`
local terminal = function(v)
    return v == 'success' or v == 'failed' or v == 'canceled' or v == 'timeout'
end
local current = redis.call('GET', KEYS[1])
if current then
    if terminal(current) then
        return 0
    end
    if current == 'canceling' and ARGV[1] ~= 'canceled' then
        return 0
    end
end
local ttl = tonumber(ARGV[3])
redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
if ARGV[2] ~= '' then
    if KEYS[2] ~= '' then
        redis.call('SET', KEYS[2], ARGV[2], 'EX', ttl)
    end
end
return 1
`)

// signalOrStoreLua atomically wakes a suspended waiter or stores the signal.
// KEYS[1] = signal key, KEYS[2] = waiter key, KEYS[3] = suspended_nodes SET
// ARGV[1] = signal payload JSON, ARGV[2] = ttl seconds
// Returns nodeName (woke a waiter) or nil (signal stored).
var signalOrStoreLua = redis.NewScript(`
local waiter = redis.call('GET', KEYS[2])
if waiter then
    redis.call('DEL', KEYS[2])
    redis.call('SREM', KEYS[3], waiter)
    return waiter
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return nil
`)

// upsertNodeLua atomically checks if the node is in a terminal state before writing.
// KEYS[1] = node status key, KEYS[2] = output key (optional, may be empty string), KEYS[3] = node meta hash
// ARGV[1] = new status, ARGV[2] = output JSON (or ""), ARGV[3] = ttl seconds, ARGV[4] = activation id
// Returns 1 (written) or 0 (skipped, already terminal).
var upsertNodeLua = redis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing == 'success' or existing == 'failed' or existing == 'skipped' or existing == 'canceled' or existing == 'continued' then
    local oldActivation = tonumber(redis.call('HGET', KEYS[3], 'activation_id') or '0')
    local newActivation = tonumber(ARGV[4] or '0')
    if newActivation <= oldActivation then
        return 0
    end
end
if existing == 'committing' or existing == 'waiting' then
    if ARGV[1] == 'running' then
        return 0
    end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
if ARGV[2] ~= '' then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
end
return 1
`)

// claimTaskLeaseLua remains only for suspend and experimental expansion paths.
// A committed claim retains the original token, deadline, task metadata, and
// expiry-index member so a crash between claim and finalization is reclaimable.
// KEYS[1] = node status key, KEYS[2] = node meta hash, KEYS[3] = lease expiry ZSET
// ARGV[1] = expected lease token, ARGV[2] = ttl seconds, ARGV[3] = expected activation id,
// ARGV[4] = lease expiry ZSET member
// Returns {valid, status}.
var claimTaskLeaseLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if not status then
    return {0, ''}
end
local expectedActivation = tonumber(ARGV[3] or '0')
if expectedActivation > 0 then
    local currentActivation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
    if currentActivation ~= expectedActivation then
        return {0, status}
    end
end
if status == 'success' or status == 'failed' or status == 'skipped' or status == 'canceled' or status == 'continued' then
    redis.call('ZREM', KEYS[3], ARGV[4])
    return {1, status}
end
if status ~= 'running' then
    return {0, status}
end
local token = redis.call('HGET', KEYS[2], 'lease_token')
if not token or token == '' or token ~= ARGV[1] then
    return {0, status}
end
redis.call('SET', KEYS[1], 'committing', 'EX', tonumber(ARGV[2]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
return {1, 'committing'}
`)

// suspendTaskLeaseLua finishes a claimed lease with a token-fenced suspended
// state. Every key is execution-scoped, keeping the script Redis Cluster-safe.
// KEYS: status, meta, output, lease index, suspended set, resume lock,
// old waiter, waiter spec, signal batch, then signal/waiter key pairs.
// ARGV: lease id, token, attempt, activation, ttl, lease member, store output,
// output JSON, node name, multi flag, quorum, signal count, spec JSON, names.
// Returns {committed, signal name, signal payload JSON, multi-payload JSON}.
var suspendTaskLeaseLua = redis.NewScript(`
local terminal = function(value)
    return value == 'success' or value == 'failed' or value == 'skipped' or value == 'canceled' or value == 'continued'
end
if redis.call('GET', KEYS[1]) ~= 'committing' then
    return {0, '', '', ''}
end
local leaseID = redis.call('HGET', KEYS[2], 'lease_id') or ''
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local activation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
if leaseID ~= ARGV[1] or token ~= ARGV[2] or attempt ~= tonumber(ARGV[3]) or activation ~= tonumber(ARGV[4]) then
    return {0, '', '', ''}
end
local ttl = tonumber(ARGV[5])
if tonumber(ARGV[7]) == 1 then
    redis.call('SET', KEYS[3], ARGV[8], 'EX', ttl)
end
redis.call('SET', KEYS[1], 'suspended', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_id', '', 'lease_token', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '')
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('ZREM', KEYS[4], ARGV[6])
redis.call('DEL', KEYS[6], KEYS[7], KEYS[8], KEYS[9])

local multi = tonumber(ARGV[10]) == 1
local quorum = tonumber(ARGV[11])
local count = tonumber(ARGV[12])
local selectedName = ''
local selectedPayload = ''
for i = 1, count do
    local signalKey = KEYS[9 + i]
    local waiterKey = KEYS[9 + count + i]
    local signalName = ARGV[13 + i]
    local payload = redis.call('GET', signalKey)
    if payload then
        redis.call('DEL', signalKey)
        if multi then
            redis.call('HSET', KEYS[9], signalName, payload)
            if selectedName == '' then
                selectedName = signalName
                selectedPayload = payload
            end
        elseif selectedName == '' then
            selectedName = signalName
            selectedPayload = payload
        end
    end
end

local clearWaiters = function()
    for i = 1, count do
        redis.call('DEL', KEYS[9 + count + i])
    end
    redis.call('SREM', KEYS[5], ARGV[9])
end
if multi then
    local values = redis.call('HGETALL', KEYS[9])
    if #values / 2 >= quorum then
        local all = {}
        for i = 1, #values, 2 do
            all[values[i]] = values[i + 1]
        end
        clearWaiters()
        redis.call('DEL', KEYS[8], KEYS[9])
        return {1, selectedName, selectedPayload, cjson.encode(all)}
    end
    redis.call('SET', KEYS[8], ARGV[13], 'EX', ttl)
    redis.call('EXPIRE', KEYS[9], ttl)
else
    if selectedName ~= '' then
        clearWaiters()
        return {1, selectedName, selectedPayload, ''}
    end
end
for i = 1, count do
    redis.call('SET', KEYS[9 + count + i], ARGV[9], 'EX', ttl)
end
redis.call('SADD', KEYS[5], ARGV[9])
redis.call('EXPIRE', KEYS[5], ttl)
return {1, '', '', ''}
`)

// acquireTaskLeaseLua atomically validates whether a queued task may become a
// running lease and, when allowed, writes the status, metadata, and its
// execution-scoped expiry-index member in one Redis Cluster-safe command.
// KEYS[1] = node status key, KEYS[2] = node meta hash, KEYS[3] = lease expiry ZSET
// ARGV[1] = new lease id
// ARGV[2] = new lease token
// ARGV[3] = issued-at unix millis
// ARGV[4] = exec ttl seconds
// ARGV[5] = task activation id
// ARGV[6] = task auto depth
// ARGV[7] = lease ttl millis
// ARGV[8] = lease expiry ZSET member
// ARGV[9] = queued task type
// ARGV[10] = queued resume payload JSON (or empty)
// ARGV[11] = queued task node index
// Returns {acquired, prev_status, prev_attempt, prev_activation_id,
// prev_auto_depth, prev_lease_token, prev_issued_at_ms, prev_lease_ttl_ms}.
var acquireTaskLeaseLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
local prevAttempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local prevActivation = tonumber(redis.call('HGET', KEYS[2], 'activation_id') or '0')
local prevAutoDepth = tonumber(redis.call('HGET', KEYS[2], 'auto_depth') or '0')
local prevLeaseToken = redis.call('HGET', KEYS[2], 'lease_token') or ''
local prevIssuedAtMs = tonumber(redis.call('HGET', KEYS[2], 'lease_issued_at_ms') or '0')
local prevLeaseTTLms = tonumber(redis.call('HGET', KEYS[2], 'lease_ttl_ms') or '0')
local taskActivation = tonumber(ARGV[5] or '0')
local nowMs = tonumber(ARGV[3] or '0')

if taskActivation > 0 and prevActivation > taskActivation then
	return {0, status or '', prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
end

if status == 'success' or status == 'failed' or status == 'skipped' or status == 'canceled' or status == 'continued' then
	if taskActivation <= 0 or prevActivation >= taskActivation then
		return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
	end
end

if status == 'committing' or status == 'waiting' then
	return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
end

if status == 'running' and prevLeaseToken ~= '' then
	if prevIssuedAtMs == 0 or prevLeaseTTLms <= 0 or nowMs < (prevIssuedAtMs + prevLeaseTTLms) then
		return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
	end
end

local nextAttempt = prevAttempt + 1
if nextAttempt <= 0 then
	nextAttempt = 1
end
redis.call('SET', KEYS[1], 'running', 'EX', tonumber(ARGV[4]))
redis.call('HSET', KEYS[2],
	'lease_id', ARGV[1],
	'lease_token', ARGV[2],
	'attempt', nextAttempt,
	'activation_id', taskActivation,
	'auto_depth', tonumber(ARGV[6] or '0'),
	'lease_issued_at_ms', nowMs,
	'lease_ttl_ms', tonumber(ARGV[7] or '0'),
	'lease_deadline_ms', nowMs + tonumber(ARGV[7] or '0'),
	'lease_task_type', tonumber(ARGV[9] or '0'),
	'lease_payload', ARGV[10] or '',
	'node_idx', tonumber(ARGV[11] or '0'))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
local leaseTTLms = tonumber(ARGV[7] or '0')
if leaseTTLms > 0 and ARGV[2] ~= '' then
    redis.call('ZADD', KEYS[3], nowMs + leaseTTLms, ARGV[8])
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[4]))
else
    redis.call('ZREM', KEYS[3], ARGV[8])
end
return {1, status or '', prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
`)

// Returns 1 (acquired) or 0 (already locked).
var resumeNodeLua = redis.NewScript(`
local locked = redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1]))
if not locked then return 0 end
return 1
`)

// resetNodeForRetryLua and the unfenced ResetNodeForRetry method were removed:
// the engine retry path uses the fenced ResetNodeForRetryWithOutbox
// (AtomicStateStore) instead. The unfenced transition risked overwriting an
// active lease owned by another worker.

// revokeLeaseLua atomically verifies and clears a still-current Running or
// Committing lease, including its expiry-index member.
// KEYS[1] = node status key, KEYS[2] = node meta hash, KEYS[3] = lease expiry ZSET
// ARGV[1] = expected lease token, ARGV[2] = ttl seconds, ARGV[3] = lease expiry ZSET member
// Returns 1 (revoked) or 0 (race lost — commit already ran or token stale).
var revokeLeaseLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' and status ~= 'waiting' then
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token')
if not token or token == '' or token ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], 'pending', 'EX', tonumber(ARGV[2]))
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
redis.call('ZREM', KEYS[3], ARGV[3])
return 1
`)

// revokeSignalLua atomically removes a signal that has not yet been consumed.
// KEYS[1] = signal key, KEYS[2] = waiter key (stores node name waiting for this signal)
// ARGV[1] = resume lock key prefix (xflow:exec:{id}:node:)
// ARGV[2] = resume lock key suffix (:resume_lock)
// Returns 1 (revoked) or 0 (signal not found or already consumed/resumed).
var revokeSignalLua = redis.NewScript(`
local signal = redis.call('GET', KEYS[1])
if not signal then return 0 end
local nodeName = redis.call('GET', KEYS[2])
if nodeName then
    local lockKey = ARGV[1] .. nodeName .. ARGV[2]
    if redis.call('EXISTS', lockKey) == 1 then return 0 end
end
redis.call('DEL', KEYS[1])
return 1
`)

// resuspendAtomicLua atomically transitions a node from one suspended signal to another.
// KEYS[1] = resume_lock key, KEYS[2] = old_waiter key, KEYS[3] = new_signal key,
// KEYS[4] = new_waiter key, KEYS[5] = suspended_nodes SET key
// ARGV[1] = node name, ARGV[2] = ttl seconds
var resuspendAtomicLua = redis.NewScript(`
redis.call('DEL', KEYS[1])
if KEYS[2] ~= KEYS[4] then
    redis.call('DEL', KEYS[2])
end
local signal = redis.call('GET', KEYS[3])
if signal then
    redis.call('DEL', KEYS[3])
    redis.call('SREM', KEYS[5], ARGV[1])
    return signal
end
redis.call('SET', KEYS[4], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SADD', KEYS[5], ARGV[1])
return nil
`)

// checkCompletionLua atomically checks all node statuses.
// Returns 0 (not complete), 1 (success), -1 (failed).
var checkCompletionLua = redis.NewScript(`
local execStatus = redis.call('GET', KEYS[1])
if execStatus == 'success' or execStatus == 'failed' or execStatus == 'canceled' then
    return 0
end
local anyFailed = false
for i = 2, #KEYS do
    local ns = redis.call('GET', KEYS[i])
    if ns == 'failed' then
        anyFailed = true
    elseif ns ~= 'success' and ns ~= 'skipped' and ns ~= 'canceled' and ns ~= 'continued' then
        return 0
    end
end
local final = anyFailed and 'failed' or 'success'
redis.call('SET', KEYS[1], final, 'EX', tonumber(ARGV[1]))
return anyFailed and -1 or 1
`)

// ---------------------------------------------------------------------------
// redisState implements engine.StateStore.
// ---------------------------------------------------------------------------

type redisState struct {
	rdb                    *redis.Client
	db                     store.Store // may be nil
	execTTL                time.Duration
	transient              bool
	transientTTL           time.Duration
	transientCompletionTTL time.Duration

	// in-memory graph cache for CheckCompletion (avoids Redis round-trips per node)
	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph

	// per-execution TTL overrides (set via SubmitOption)
	ttlMu    sync.RWMutex
	execTTLs map[types.ExecutionID]time.Duration

	// leaseRepairCursor advances a bounded reconciliation scan across node
	// status keys. The mutex prevents concurrent control-plane maintenance
	// loops from repeatedly scanning the same Redis page.
	leaseRepairMu     sync.Mutex
	leaseRepairCursor uint64

	// Audit-trail observability — Redis is system-of-record; the store/sqlstore
	// audit trail is best-effort. auditWrite routes failures through these
	// instead of silently dropping them.
	audit         AuditObserver
	auditCounters *auditCounters
	leaseObserver LeaseObserver
	logger        engine.Logger
}

func newRedisState(rdb *redis.Client, db store.Store, execTTL time.Duration) *redisState {
	return &redisState{
		rdb:           rdb,
		db:            db,
		execTTL:       execTTL,
		graphs:        make(map[types.ExecutionID]*graph.Graph),
		execTTLs:      make(map[types.ExecutionID]time.Duration),
		audit:         noopAuditObserver{},
		auditCounters: &auditCounters{},
	}
}

func (s *redisState) ttlSec() int {
	if s.transient && s.transientTTL > 0 {
		return int(s.transientTTL.Seconds())
	}
	return int(s.execTTL.Seconds())
}

// getExecTTL returns the per-execution TTL override if set, otherwise the adapter default.
func (s *redisState) getExecTTL(id types.ExecutionID) time.Duration {
	s.ttlMu.RLock()
	ttl := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	if s.transient && s.transientTTL > 0 {
		return s.transientTTL
	}
	return s.execTTL
}

func executionKeySetKey(id types.ExecutionID) string {
	return execKey(id, "keys")
}

func (s *redisState) expireExecutionKeys(ctx context.Context, id types.ExecutionID, ttl time.Duration, newKeys ...string) error {
	if ttl <= 0 {
		return nil
	}
	keySet := executionKeySetKey(id)
	if len(newKeys) > 0 {
		members := make([]any, 0, len(newKeys)+1)
		for _, key := range newKeys {
			if key != "" {
				members = append(members, key)
			}
		}
		members = append(members, keySet)
		if err := s.rdb.SAdd(ctx, keySet, members...).Err(); err != nil {
			return fmt.Errorf("track execution keys %q: %w", id, err)
		}
	}
	keys, err := s.rdb.SMembers(ctx, keySet).Result()
	if err != nil {
		return fmt.Errorf("read execution keys %q: %w", id, err)
	}
	keys = append(keys, keySet)
	if len(keys) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, key := range keys {
		pipe.Expire(ctx, key, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("expire execution keys %q: %w", id, err)
	}
	return nil
}

func (s *redisState) refreshTransientTTL(ctx context.Context, id types.ExecutionID, newKeys ...string) error {
	if !s.transient {
		return nil
	}
	return s.expireExecutionKeys(ctx, id, s.transientTTL, newKeys...)
}

func (s *redisState) shortenTransientCompletionTTL(ctx context.Context, id types.ExecutionID, newKeys ...string) error {
	if !s.transient {
		return nil
	}
	return s.expireExecutionKeys(ctx, id, s.transientCompletionTTL, newKeys...)
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *redisState) CreateExecution(ctx context.Context, e *engine.ExecutionSnapshot) error {
	return s.createExecution(ctx, e, nil)
}

// CreateExecutionWithOutbox commits execution metadata and its initial durable
// delivery intents in one Redis transaction. The SQL audit projection remains
// a best-effort follow-up and is not part of scheduling correctness.
func (s *redisState) CreateExecutionWithOutbox(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	return s.createExecution(ctx, e, entries)
}

// createExecution persists the execution, its outbox entries, and the SQL audit
// projection. The Redis pipeline (including outbox entries scored available-now)
// commits before the SQL write; on SQL failure, cleanupCreatedExecution deletes
// the Redis keys including the outbox. There is a narrow (SQL-call-duration)
// window where the outbox dispatcher could pick up an entry and enqueue an asynq
// task that references an execution about to be cleaned up — that orphan task
// fails on dequeue and is retried/dead-lettered by asynq (self-healing, not
// data corruption). Keeping the outbox in the same pipeline is intentional: it
// is the common (no-SQL-failure) path that must never strand a root task.
func (s *redisState) createExecution(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	ttl := s.execTTL

	// Check for per-execution TTL override from context.
	if override, ok := engine.ExecutionTTLFromContext(ctx); ok {
		ttl = override
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = override
		s.ttlMu.Unlock()
	}

	// Serialize graph for persistence (allows recovery after queue consumer restart).
	graphJSON, err := json.Marshal(e.Graph)
	if err != nil {
		return fmt.Errorf("marshal graph for %q: %w", e.ID, err)
	}

	var rec *store.ExecutionRecord
	if s.db != nil && !s.transient {
		now := time.Now()
		var recErr error
		rec, recErr = buildExecutionRecord(ctx, e, now)
		if recErr != nil {
			return recErr
		}
	}

	pipe := s.rdb.TxPipeline()
	keys := []string{execKey(e.ID, "status"), execKey(e.ID, "graph")}
	pipe.Set(ctx, execKey(e.ID, "status"), string(e.Status), ttl)
	pipe.Set(ctx, execKey(e.ID, "graph"), string(graphJSON), ttl)
	if e.Params != nil {
		paramsJSON, err := json.Marshal(e.Params)
		if err != nil {
			return fmt.Errorf("marshal execution params for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(e.ID, "params"), string(paramsJSON), ttl)
		keys = append(keys, execKey(e.ID, "params"))
	}
	if e.Runtime != nil {
		runtimeJSON, err := json.Marshal(e.Runtime)
		if err != nil {
			return fmt.Errorf("marshal execution runtime for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(e.ID, "runtime"), string(runtimeJSON), ttl)
		keys = append(keys, execKey(e.ID, "runtime"))
	}
	if e.TraceID != "" {
		pipe.Set(ctx, execKey(e.ID, "trace_id"), e.TraceID, ttl)
		keys = append(keys, execKey(e.ID, "trace_id"))
	}
	if e.SpanID != "" {
		pipe.Set(ctx, execKey(e.ID, "span_id"), e.SpanID, ttl)
		keys = append(keys, execKey(e.ID, "span_id"))
	}
	// Acyclic executions use these counters as the O(1) completion source of
	// truth. Cyclic graphs retain their activation-based completion protocol.
	if e.Graph != nil && !e.Graph.AllowCycles {
		pipe.Set(ctx, remainingNodesKey(e.ID), len(e.Graph.Nodes), ttl)
		pipe.Set(ctx, failedNodesKey(e.ID), 0, ttl)
		keys = append(keys, remainingNodesKey(e.ID), failedNodesKey(e.ID))
	}
	// Seed in-degree counters.
	if e.Graph != nil {
		for i, d := range e.Graph.InDegree {
			if d > 0 {
				pipe.Set(ctx, inDegreeKey(e.ID, i), d, ttl)
				keys = append(keys, inDegreeKey(e.ID, i))
			}
		}
	}
	if len(entries) > 0 {
		readyKey := outboxReadyKey(e.ID)
		bodyKey := outboxBodyKey(e.ID)
		availableNow := time.Now().UTC().UnixMilli()
		for _, entry := range entries {
			if entry.ID == "" {
				return fmt.Errorf("create execution %q: empty outbox entry ID", e.ID)
			}
			encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
			if err != nil {
				return fmt.Errorf("create execution %q outbox %q: %w", e.ID, entry.ID, err)
			}
			availableAt := availableNow
			if !entry.AvailableAt.IsZero() {
				availableAt = entry.AvailableAt.UTC().UnixMilli()
			}
			pipe.HSet(ctx, bodyKey, entry.ID, encoded)
			pipe.ZAdd(ctx, readyKey, redis.Z{Score: float64(availableAt), Member: entry.ID})
		}
		pipe.Expire(ctx, readyKey, ttl)
		pipe.Expire(ctx, bodyKey, ttl)
		keys = append(keys, readyKey, bodyKey)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create execution %q: %w", e.ID, err)
	}
	if err := s.refreshTransientTTL(ctx, e.ID, keys...); err != nil {
		return err
	}

	// Dual-write to store.
	if rec != nil {
		if err := s.db.CreateExecution(ctx, rec); err != nil {
			s.cleanupCreatedExecution(ctx, e)
			return fmt.Errorf("store create execution: %w", err)
		}
	}

	// Cache graph for CheckCompletion only after the durable create path has
	// accepted the execution.
	s.mu.Lock()
	s.graphs[e.ID] = e.Graph
	s.mu.Unlock()
	return nil
}

func (s *redisState) cleanupCreatedExecution(ctx context.Context, e *engine.ExecutionSnapshot) {
	s.ttlMu.Lock()
	delete(s.execTTLs, e.ID)
	s.ttlMu.Unlock()

	pipe := s.rdb.Pipeline()
	pipe.Del(ctx,
		execKey(e.ID, "status"),
		execKey(e.ID, "graph"),
		execKey(e.ID, "error"),
		execKey(e.ID, "params"),
		execKey(e.ID, "runtime"),
		execKey(e.ID, "trace_id"),
		execKey(e.ID, "span_id"),
		remainingNodesKey(e.ID),
		failedNodesKey(e.ID),
		leaseExpiryZSetKey(e.ID),
		outboxReadyKey(e.ID),
		outboxBodyKey(e.ID),
		outboxAttemptsKey(e.ID),
		outboxDeadKey(e.ID),
		outboxDeadBodyKey(e.ID),
		executionKeySetKey(e.ID),
	)
	if e.Graph != nil {
		for i, node := range e.Graph.Nodes {
			pipe.Del(ctx,
				inDegreeKey(e.ID, i),
				activeInputsKey(e.ID, i),
				scheduleKey(e.ID, i),
				nodeStatusKey(e.ID, node.Name),
				nodeMetaKey(e.ID, node.Name),
				outputKey(e.ID, node.Name),
			)
		}
	}
	_, _ = pipe.Exec(ctx)
	s.mu.Lock()
	delete(s.graphs, e.ID)
	s.mu.Unlock()
}

func buildExecutionRecord(ctx context.Context, e *engine.ExecutionSnapshot, now time.Time) (*store.ExecutionRecord, error) {
	rec := &store.ExecutionRecord{
		ExecutionID: e.ID,
		Status:      e.Status,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if def, ok := engine.WorkflowDefFromContext(ctx); ok {
		rec.WorkflowName = def.Name
		defJSON, err := json.Marshal(def)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow definition for %q: %w", e.ID, err)
		}
		rec.WorkflowDef = defJSON
	}
	if e.Params != nil {
		paramsJSON, err := json.Marshal(e.Params)
		if err != nil {
			return nil, fmt.Errorf("marshal execution params for %q: %w", e.ID, err)
		}
		rec.Params = paramsJSON
	}
	if e.Runtime != nil {
		runtimeJSON, err := json.Marshal(e.Runtime)
		if err != nil {
			return nil, fmt.Errorf("marshal execution runtime for %q: %w", e.ID, err)
		}
		rec.Runtime = runtimeJSON
	}
	rec.TraceID = e.TraceID
	rec.SpanID = e.SpanID
	return rec, nil
}

func (s *redisState) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	ttl := s.getExecTTL(id)
	// Compare-and-set with cancel-aware fencing: a terminal or canceling status
	// blocks non-canceled overwrites, so a concurrent cyclic completeExecution
	// cannot stomp an in-flight Cancel.
	errKey := ""
	if errMsg != "" {
		errKey = execKey(id, "error")
	}
	applied, err := updateExecutionStatusLua.Run(ctx, s.rdb,
		[]string{execKey(id, "status"), errKey},
		string(status), errMsg, int(ttl.Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("update execution status %q: %w", id, err)
	}
	keys := []string{execKey(id, "status")}
	if errKey != "" {
		keys = append(keys, errKey)
	}
	// Clean up timeout ZSET entries when execution is canceled.
	if applied == 1 && status == types.ExecutionStatusCanceled {
		s.cleanupOnCancel(ctx, id)
	}
	if applied == 1 && types.IsTerminalExecutionStatus(status) {
		if err := s.shortenTransientCompletionTTL(ctx, id, keys...); err != nil {
			return err
		}
		// Redis persists the graph for later inspection/reload; the in-process
		// cache and per-execution TTL override must not survive a terminal state.
		s.evictExecutionCaches(id)
	} else if applied == 1 {
		if err := s.refreshTransientTTL(ctx, id, keys...); err != nil {
			return err
		}
	}
	if applied == 1 && s.db != nil && !s.transient {
		s.auditWrite(ctx, "update_execution_status", func(ctx context.Context) error {
			return s.db.UpdateExecutionStatus(ctx, id, status, errMsg)
		})
	}
	_ = s.PublishExecutionEvent(ctx, engine.ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

func (s *redisState) GetExecution(ctx context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	val, err := s.rdb.Get(ctx, execKey(id, "status")).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get execution %q: %w", id, err)
	}
	s.mu.RLock()
	g := s.graphs[id]
	s.mu.RUnlock()
	var params map[string]any
	if raw, err := s.rdb.Get(ctx, execKey(id, "params")).Bytes(); err == nil {
		_ = json.Unmarshal(raw, &params)
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution params %q: %w", id, err)
	}
	var runtime *types.Runtime
	if raw, err := s.rdb.Get(ctx, execKey(id, "runtime")).Bytes(); err == nil {
		var decoded types.Runtime
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("unmarshal execution runtime %q: %w", id, err)
		}
		runtime = &decoded
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution runtime %q: %w", id, err)
	}
	var traceID string
	if raw, err := s.rdb.Get(ctx, execKey(id, "trace_id")).Result(); err == nil {
		traceID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution trace ID %q: %w", id, err)
	}
	var spanID string
	if raw, err := s.rdb.Get(ctx, execKey(id, "span_id")).Result(); err == nil {
		spanID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution span ID %q: %w", id, err)
	}
	return &engine.ExecutionSnapshot{
		ID:      id,
		Graph:   g,
		Status:  types.ExecutionStatus(val),
		Params:  params,
		Runtime: runtime,
		TraceID: traceID,
		SpanID:  spanID,
	}, nil
}

func (s *redisState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	// Check in-memory cache first.
	s.mu.RLock()
	g := s.graphs[id]
	s.mu.RUnlock()
	if g != nil {
		return g, nil
	}

	// Load from Redis.
	raw, err := s.rdb.Get(ctx, execKey(id, "graph")).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load graph %q: %w", id, err)
	}

	g = &graph.Graph{}
	if err := json.Unmarshal([]byte(raw), g); err != nil {
		return nil, fmt.Errorf("unmarshal graph %q: %w", id, err)
	}

	// Re-populate the in-memory cache.
	s.mu.Lock()
	s.graphs[id] = g
	s.mu.Unlock()

	return g, nil
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

// UpsertNode writes a node snapshot. Unlike the fenced AcquireTaskLease (which
// performs status + meta + lease-index in one Lua transition), this path writes
// status (upsertNodeLua), then meta (HSet), then the lease index (ZAdd) as
// separate commands — a crash between them can leave the meta hash stale
// relative to the status key. This is the snapshot/recovery path, not the
// active lease-acquisition path, and the LeaseSweeper self-heals the divergence
// by pruning lease-index entries whose meta lacks a token. Callers that need a
// fenced transition must use AcquireTaskLease / CommitNode instead.
func (s *redisState) UpsertNode(ctx context.Context, n *engine.NodeSnapshot) error {
	key := nodeStatusKey(n.ExecutionID, n.Name)
	outKey := outputKey(n.ExecutionID, n.Name)
	metaKey := nodeMetaKey(n.ExecutionID, n.Name)

	var outputJSON string
	if n.Output != nil {
		b, _ := json.Marshal(n.Output)
		outputJSON = string(b)
	}
	var leasePayloadJSON string
	if n.LeasePayload != nil {
		encoded, err := json.Marshal(n.LeasePayload)
		if err != nil {
			return fmt.Errorf("marshal node lease payload %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		leasePayloadJSON = string(encoded)
	}

	ttl := s.getExecTTL(n.ExecutionID)
	_, err := upsertNodeLua.Run(ctx, s.rdb,
		[]string{key, outKey, metaKey},
		string(n.Status), outputJSON, int(ttl.Seconds()), n.ActivationID,
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("upsert node %q/%q: %w", n.ExecutionID, n.Name, err)
	}
	keys := []string{key}
	if outputJSON != "" {
		keys = append(keys, outKey)
	}
	if n.LeaseID != "" || n.LeaseToken != "" || n.Attempt != 0 || n.ActivationID != 0 || n.AutoDepth != 0 || !n.LeaseIssuedAt.IsZero() || n.Port != "" || n.Error != "" || n.CommittedLeaseToken != "" || n.CommittedAttempt != 0 {
		meta := map[string]any{
			"lease_id":        string(n.LeaseID),
			"lease_token":     string(n.LeaseToken),
			"attempt":         n.Attempt,
			"activation_id":   n.ActivationID,
			"auto_depth":      n.AutoDepth,
			"node_idx":        n.NodeIdx,
			"lease_task_type": int(n.LeaseTaskType),
			"lease_payload":   leasePayloadJSON,
		}
		if !n.LeaseIssuedAt.IsZero() {
			meta["lease_issued_at_ms"] = n.LeaseIssuedAt.UnixMilli()
		}
		if n.LeaseTTL > 0 {
			meta["lease_ttl_ms"] = n.LeaseTTL.Milliseconds()
			if !n.LeaseIssuedAt.IsZero() {
				meta["lease_deadline_ms"] = n.LeaseIssuedAt.Add(n.LeaseTTL).UnixMilli()
			}
		}
		if n.Port != "" {
			meta["port"] = n.Port
		}
		if n.Error != "" {
			meta["error"] = n.Error
		}
		if n.CommittedLeaseToken != "" {
			meta["committed_lease_token"] = string(n.CommittedLeaseToken)
			meta["committed_attempt"] = n.CommittedAttempt
		}
		if err := s.rdb.HSet(ctx, metaKey, meta).Err(); err != nil {
			return fmt.Errorf("upsert node lease %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		if err := s.rdb.Expire(ctx, metaKey, ttl).Err(); err != nil {
			return fmt.Errorf("expire node lease %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		keys = append(keys, metaKey)
	}
	// Lease-expiry discovery is per execution so it shares the hash tag with
	// the node status and metadata. AcquireTaskLease updates all three in one
	// Lua command; this path keeps generic snapshot upserts recoverable too.
	leaseIndexKey := leaseExpiryZSetKey(n.ExecutionID)
	member := leaseExpiryMember(n.ExecutionID, n.Name)
	keys = append(keys, leaseIndexKey)
	if (n.Status == types.NodeStatusRunning || n.Status == types.NodeStatusCommitting || n.Status == types.NodeStatusWaiting) && n.LeaseToken != "" && !n.LeaseIssuedAt.IsZero() && n.LeaseTTL > 0 {
		expiryMs := float64(n.LeaseIssuedAt.Add(n.LeaseTTL).UnixMilli())
		if err := s.rdb.ZAdd(ctx, leaseIndexKey, redis.Z{Score: expiryMs, Member: member}).Err(); err != nil {
			return fmt.Errorf("index lease expiry %q/%q: %w", n.ExecutionID, n.Name, err)
		}
	} else if n.Status != types.NodeStatusRunning && n.Status != types.NodeStatusCommitting && n.Status != types.NodeStatusWaiting {
		// Terminal, suspended, and pending nodes have no recoverable lease.
		if err := s.rdb.ZRem(ctx, leaseIndexKey, member).Err(); err != nil {
			return fmt.Errorf("remove lease expiry %q/%q: %w", n.ExecutionID, n.Name, err)
		}
	}
	if err := s.refreshTransientTTL(ctx, n.ExecutionID, keys...); err != nil {
		return err
	}

	if s.db != nil && !s.transient {
		var outBytes []byte
		if n.Output != nil {
			outBytes, _ = json.Marshal(n.Output)
		}
		rec := &store.NodeRecord{
			ExecutionID: n.ExecutionID,
			NodeName:    n.Name,
			Status:      n.Status,
			LeaseID:     string(n.LeaseID),
			LeaseToken:  string(n.LeaseToken),
			Attempt:     n.Attempt,
			Output:      outBytes,
			Port:        n.Port,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "upsert_node", func(ctx context.Context) error {
			return s.db.UpsertNode(ctx, rec)
		})
	}
	return nil
}

func (s *redisState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*engine.NodeSnapshot, error) {
	val, err := s.rdb.Get(ctx, nodeStatusKey(id, name)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get node %q/%q: %w", id, name, err)
	}
	ns := &engine.NodeSnapshot{ExecutionID: id, Name: name, Status: types.NodeStatus(val)}
	meta, err := s.rdb.HGetAll(ctx, nodeMetaKey(id, name)).Result()
	if err != nil {
		return nil, fmt.Errorf("get node lease %q/%q: %w", id, name, err)
	}
	ns.LeaseID = engine.LeaseID(meta["lease_id"])
	ns.LeaseToken = engine.LeaseToken(meta["lease_token"])
	if nodeIdx := meta["node_idx"]; nodeIdx != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(nodeIdx, "%d", &parsed); scanErr == nil {
			ns.NodeIdx = parsed
		}
	}
	if attempt := meta["attempt"]; attempt != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(attempt, "%d", &parsed); scanErr == nil {
			ns.Attempt = parsed
		}
	}
	if activationID := meta["activation_id"]; activationID != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(activationID, "%d", &parsed); scanErr == nil {
			ns.ActivationID = parsed
		}
	}
	if autoDepth := meta["auto_depth"]; autoDepth != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(autoDepth, "%d", &parsed); scanErr == nil {
			ns.AutoDepth = parsed
		}
	}
	if issued := meta["lease_issued_at_ms"]; issued != "" {
		var ms int64
		if _, scanErr := fmt.Sscanf(issued, "%d", &ms); scanErr == nil && ms > 0 {
			ns.LeaseIssuedAt = time.UnixMilli(ms).UTC()
		}
	}
	if ttlMs := meta["lease_ttl_ms"]; ttlMs != "" {
		var ms int64
		if _, scanErr := fmt.Sscanf(ttlMs, "%d", &ms); scanErr == nil && ms > 0 {
			ns.LeaseTTL = time.Duration(ms) * time.Millisecond
		}
	}
	if taskType := meta["lease_task_type"]; taskType != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(taskType, "%d", &parsed); scanErr == nil {
			ns.LeaseTaskType = engine.TaskType(parsed)
		}
	}
	if rawPayload := meta["lease_payload"]; rawPayload != "" {
		var payload types.SignalPayload
		if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
			return nil, fmt.Errorf("decode node lease payload %q/%q: %w", id, name, err)
		}
		ns.LeasePayload = &payload
	}
	ns.Port = meta["port"]
	ns.Error = meta["error"]
	ns.CommittedLeaseToken = engine.LeaseToken(meta["committed_lease_token"])
	if committedAttempt := meta["committed_attempt"]; committedAttempt != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(committedAttempt, "%d", &parsed); scanErr == nil {
			ns.CommittedAttempt = parsed
		}
	}
	return ns, nil
}

func (s *redisState) AcquireTaskLease(ctx context.Context, lease *engine.TaskLease) (previous *engine.NodeSnapshot, acquired bool, err error) {
	started := time.Now()
	defer func() {
		result := "acquired"
		if err != nil {
			result = "error"
		} else if !acquired {
			result = "rejected"
		}
		s.observeLeaseAcquire(result, time.Since(started))
	}()

	ttl := s.getExecTTL(lease.Task.ExecutionID)
	payloadJSON := ""
	if lease.Task.Payload != nil {
		encoded, err := json.Marshal(lease.Task.Payload)
		if err != nil {
			return nil, false, fmt.Errorf("marshal lease payload %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		payloadJSON = string(encoded)
	}
	result, err := acquireTaskLeaseLua.Run(ctx, s.rdb,
		[]string{
			nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName),
			nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName),
			leaseExpiryZSetKey(lease.Task.ExecutionID),
		},
		string(lease.LeaseID), string(lease.LeaseToken), lease.IssuedAt.UnixMilli(), int(ttl.Seconds()), lease.Task.ActivationID, lease.Task.AutoDepth, lease.TTL.Milliseconds(), leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName), int(lease.Task.Type), payloadJSON, lease.Task.NodeIdx,
	).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("acquire task lease %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(result) != 8 {
		return nil, false, fmt.Errorf("acquire task lease %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, result)
	}

	asInt64 := func(v any) int64 {
		switch n := v.(type) {
		case int64:
			return n
		case string:
			var parsed int64
			if _, err := fmt.Sscanf(n, "%d", &parsed); err == nil {
				return parsed
			}
		}
		return 0
	}
	asString := func(v any) string {
		if s, ok := v.(string); ok {
			return s
		}
		return ""
	}

	acquired = asInt64(result[0]) == 1
	prevStatus := asString(result[1])
	var prev *engine.NodeSnapshot
	if prevStatus != "" {
		prev = &engine.NodeSnapshot{
			ExecutionID:  lease.Task.ExecutionID,
			Name:         lease.Task.NodeName,
			NodeIdx:      lease.Task.NodeIdx,
			Status:       types.NodeStatus(prevStatus),
			Attempt:      int(asInt64(result[2])),
			ActivationID: int(asInt64(result[3])),
			AutoDepth:    int(asInt64(result[4])),
			LeaseToken:   engine.LeaseToken(asString(result[5])),
		}
		if ms := asInt64(result[6]); ms > 0 {
			prev.LeaseIssuedAt = time.UnixMilli(ms).UTC()
		}
		if ms := asInt64(result[7]); ms > 0 {
			prev.LeaseTTL = time.Duration(ms) * time.Millisecond
		}
	}
	if !acquired {
		return prev, false, nil
	}

	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(lease.Task.ExecutionID),
	); err != nil {
		return nil, false, err
	}

	if s.db != nil {
		attempt := 1
		if prev != nil {
			attempt = prev.Attempt + 1
		}
		rec := &store.NodeRecord{
			ExecutionID: lease.Task.ExecutionID,
			NodeName:    lease.Task.NodeName,
			Status:      types.NodeStatusRunning,
			LeaseID:     string(lease.LeaseID),
			LeaseToken:  string(lease.LeaseToken),
			Attempt:     attempt,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "acquire_task_lease", func(ctx context.Context) error {
			return s.db.UpsertNode(ctx, rec)
		})
	}

	return prev, true, nil
}

// leaseIndexBatchLimit caps a single ListExpiredLeases scan. Small enough that
// the sweeper stays quick under heavy backlog; the sweeper re-polls until the
// list drains, so this is not a coverage cap, only a per-call bound.
const leaseIndexBatchLimit = 256

func (s *redisState) ListExpiredLeases(ctx context.Context, before time.Time) (expired []engine.ExpiredLease, err error) {
	started := time.Now()
	defer func() {
		s.observeLeaseExpiryScan(len(expired), time.Since(started), err)
	}()

	const scanCount = int64(128)

	max := fmt.Sprintf("%d", before.UnixMilli())
	out := make([]engine.ExpiredLease, 0, leaseIndexBatchLimit)
	seenIndexes := make(map[string]struct{})
	var cursor uint64

	for len(out) < leaseIndexBatchLimit {
		indexKeys, next, err := s.rdb.Scan(ctx, cursor, "xflow:exec:{*}:leases", scanCount).Result()
		if err != nil {
			return out, fmt.Errorf("scan lease indexes: %w", err)
		}
		for _, indexKey := range indexKeys {
			if _, seen := seenIndexes[indexKey]; seen {
				continue
			}
			seenIndexes[indexKey] = struct{}{}
			indexExecID, validIndex := executionIDFromKey(indexKey)
			if !validIndex {
				continue
			}

			remaining := leaseIndexBatchLimit - len(out)
			members, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key: indexKey, Start: "-inf", Stop: max, ByScore: true, Offset: 0, Count: int64(remaining),
			}).Result()
			if err != nil {
				return out, fmt.Errorf("list expired leases for %q: %w", indexExecID, err)
			}
			for _, member := range members {
				execID, nodeName, ok := splitLeaseMember(member)
				if !ok || execID != indexExecID {
					if err := s.rdb.ZRem(ctx, indexKey, member).Err(); err != nil {
						return out, fmt.Errorf("prune malformed lease index member %q: %w", member, err)
					}
					continue
				}

				status, err := s.rdb.Get(ctx, nodeStatusKey(execID, nodeName)).Result()
				if err == redis.Nil || (err == nil && status != string(types.NodeStatusRunning) && status != string(types.NodeStatusCommitting) && status != string(types.NodeStatusWaiting)) {
					if removeErr := s.rdb.ZRem(ctx, indexKey, member).Err(); removeErr != nil {
						return out, fmt.Errorf("prune stale lease %q/%q: %w", execID, nodeName, removeErr)
					}
					continue
				}
				if err != nil {
					return out, fmt.Errorf("read node status %q/%q: %w", execID, nodeName, err)
				}

				meta, err := s.rdb.HGetAll(ctx, nodeMetaKey(execID, nodeName)).Result()
				if err != nil {
					return out, fmt.Errorf("read node meta %q/%q: %w", execID, nodeName, err)
				}
				if meta["lease_token"] == "" {
					if removeErr := s.rdb.ZRem(ctx, indexKey, member).Err(); removeErr != nil {
						return out, fmt.Errorf("prune tokenless lease %q/%q: %w", execID, nodeName, removeErr)
					}
					continue
				}

				var deadlineMs, issuedAtMs, leaseTTLms int64
				parseInt64(meta["lease_deadline_ms"], func(value int64) { deadlineMs = value })
				parseInt64(meta["lease_issued_at_ms"], func(value int64) { issuedAtMs = value })
				parseInt64(meta["lease_ttl_ms"], func(value int64) { leaseTTLms = value })
				if deadlineMs <= 0 && issuedAtMs > 0 && leaseTTLms > 0 {
					// Compatibility with leases written before lease_deadline_ms was
					// introduced. The repaired index is then based on the same
					// durable metadata, not its prior ZSET score.
					deadlineMs = issuedAtMs + leaseTTLms
				}
				if deadlineMs > before.UnixMilli() {
					if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: float64(deadlineMs), Member: member}).Err(); err != nil {
						return out, fmt.Errorf("repair lease index %q/%q: %w", execID, nodeName, err)
					}
					continue
				}

				lease := engine.ExpiredLease{
					ExecutionID: execID,
					NodeName:    nodeName,
					LeaseID:     engine.LeaseID(meta["lease_id"]),
					LeaseToken:  engine.LeaseToken(meta["lease_token"]),
				}
				if issuedAtMs > 0 {
					lease.IssuedAt = time.UnixMilli(issuedAtMs).UTC()
				}
				if leaseTTLms > 0 {
					lease.TTL = time.Duration(leaseTTLms) * time.Millisecond
				}
				parseInt64(meta["node_idx"], func(value int64) { lease.NodeIdx = int(value) })
				parseInt64(meta["activation_id"], func(value int64) { lease.ActivationID = int(value) })
				parseInt64(meta["auto_depth"], func(value int64) { lease.AutoDepth = int(value) })
				parseInt64(meta["lease_task_type"], func(value int64) { lease.TaskType = engine.TaskType(value) })
				if rawPayload := meta["lease_payload"]; rawPayload != "" {
					var payload types.SignalPayload
					if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
						return out, fmt.Errorf("decode expired lease payload %q/%q: %w", execID, nodeName, err)
					}
					lease.Payload = &payload
				}
				out = append(out, lease)
				if len(out) == leaseIndexBatchLimit {
					break
				}
			}
			if len(out) == leaseIndexBatchLimit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *redisState) RevokeLease(ctx context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	if token == "" {
		return false, nil
	}
	ttl := s.getExecTTL(id)
	result, err := revokeLeaseLua.Run(ctx, s.rdb,
		[]string{nodeStatusKey(id, name), nodeMetaKey(id, name), leaseExpiryZSetKey(id)},
		string(token), int(ttl.Seconds()), leaseExpiryMember(id, name),
	).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease %q/%q: %w", id, name, err)
	}
	if result == 1 {
		if err := s.refreshTransientTTL(ctx, id, nodeStatusKey(id, name), nodeMetaKey(id, name), leaseExpiryZSetKey(id)); err != nil {
			return false, err
		}
	}
	return result == 1, nil
}

// parseInt64 pulls an int64 out of a redis-hash string field. Silent on parse
// failures — missing / malformed fields simply leave the callback unset.
func parseInt64(s string, cb func(int64)) {
	if s == "" {
		return
	}
	var v int64
	if _, err := fmt.Sscanf(s, "%d", &v); err == nil {
		cb(v)
	}
}

func (s *redisState) ClaimTaskLease(ctx context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	if s.transient {
		execStatus, err := s.rdb.Get(ctx, execKey(lease.Task.ExecutionID, "status")).Result()
		if err != nil && err != redis.Nil {
			return nil, false, fmt.Errorf("claim task lease %q/%q: get execution status: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		if types.IsTerminalExecutionStatus(types.ExecutionStatus(execStatus)) {
			return &engine.NodeSnapshot{
				ExecutionID:  lease.Task.ExecutionID,
				Name:         lease.Task.NodeName,
				NodeIdx:      lease.Task.NodeIdx,
				Status:       types.NodeStatusCanceled,
				ActivationID: lease.Task.ActivationID,
				AutoDepth:    lease.Task.AutoDepth,
			}, true, nil
		}
	}

	ttl := s.getExecTTL(lease.Task.ExecutionID)
	result, err := claimTaskLeaseLua.Run(ctx, s.rdb,
		[]string{
			nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName),
			nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName),
			leaseExpiryZSetKey(lease.Task.ExecutionID),
		},
		string(lease.LeaseToken), int(ttl.Seconds()), lease.Task.ActivationID, leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName),
	).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("claim task lease %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(result) != 2 {
		return nil, false, fmt.Errorf("claim task lease %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, result)
	}

	valid, _ := result[0].(int64)
	status, _ := result[1].(string)
	ns := &engine.NodeSnapshot{
		ExecutionID:  lease.Task.ExecutionID,
		Name:         lease.Task.NodeName,
		NodeIdx:      lease.Task.NodeIdx,
		Status:       types.NodeStatus(status),
		ActivationID: lease.Task.ActivationID,
		AutoDepth:    lease.Task.AutoDepth,
	}
	if valid != 1 {
		return ns, false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(lease.Task.ExecutionID),
	); err != nil {
		return nil, false, err
	}
	return ns, true, nil
}

// SuspendTaskLease atomically converts one committing lease into a suspended
// node while preserving the signal rendezvous semantics for ordinary and
// multi-signal waits. A stale claimant returns committed=false and cannot
// consume signals or overwrite a recovered lease.
func (s *redisState) SuspendTaskLease(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (*types.SignalPayload, bool, error) {
	if lease == nil || spec == nil {
		return nil, false, engine.ErrInvalidLeaseToken
	}
	outputJSON := ""
	if storeOutput {
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, false, fmt.Errorf("marshal suspend output %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		outputJSON = string(encoded)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, false, fmt.Errorf("marshal suspend spec %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	multi := 0
	if spec.Mode == types.ModeMultiSignal {
		multi = 1
	}
	oldWaiter := oldSignalName
	if oldWaiter == "" {
		oldWaiter = "__none__"
	}
	keys := []string{
		nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName),
		outputKey(lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(lease.Task.ExecutionID),
		suspendedNodesKey(lease.Task.ExecutionID),
		resumeLockKey(lease.Task.ExecutionID, lease.Task.NodeName),
		waiterKey(lease.Task.ExecutionID, oldWaiter),
		waiterSpecKey(lease.Task.ExecutionID, lease.Task.NodeName),
		signalBatchKey(lease.Task.ExecutionID, lease.Task.NodeName),
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, signalKey(lease.Task.ExecutionID, signalName))
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, waiterKey(lease.Task.ExecutionID, signalName))
	}
	store := 0
	if storeOutput {
		store = 1
	}
	args := []any{
		string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID,
		int(s.getExecTTL(lease.Task.ExecutionID).Seconds()), leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName),
		store, outputJSON, lease.Task.NodeName, multi, signalQuorum(spec), len(spec.Signals), string(specJSON),
	}
	for _, signalName := range spec.Signals {
		args = append(args, signalName)
	}
	result, err := suspendTaskLeaseLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("suspend task lease %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(result) != 4 {
		return nil, false, fmt.Errorf("suspend task lease %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, result)
	}
	if redisResultInt(result[0]) != 1 {
		return nil, false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID, keys...); err != nil {
		return nil, false, err
	}
	if err := s.extendExecTTL(ctx, lease.Task.ExecutionID, lease.Task.NodeName, s.suspendTTL(lease.Task.ExecutionID, spec)); err != nil {
		return nil, false, err
	}
	name := redisResultString(result[1])
	raw := redisResultString(result[2])
	if name == "" || raw == "" {
		return nil, true, nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, false, fmt.Errorf("decode suspend payload %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	payload := &types.SignalPayload{Triggered: types.SignalReceived, Name: name, Data: data}
	if allJSON := redisResultString(result[3]); allJSON != "" {
		var encodedAll map[string]string
		if err := json.Unmarshal([]byte(allJSON), &encodedAll); err != nil {
			return nil, false, fmt.Errorf("decode suspend payload set %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		payload.All = make(map[string]map[string]any, len(encodedAll))
		for signalName, signalJSON := range encodedAll {
			var signalData map[string]any
			if err := json.Unmarshal([]byte(signalJSON), &signalData); err != nil {
				return nil, false, fmt.Errorf("decode suspend signal %q/%q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, signalName, err)
			}
			payload.All[signalName] = signalData
		}
	}
	return payload, true, nil
}

// ---------------------------------------------------------------------------
// Scheduling counters
// ---------------------------------------------------------------------------

func (s *redisState) DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	activeFlag := 0
	if portActive {
		activeFlag = 1
	}
	ttl := int(s.getExecTTL(id).Seconds())
	vals, err := propagateLua.Run(ctx, s.rdb,
		[]string{inDegreeKey(id, nodeIdx), activeInputsKey(id, nodeIdx)},
		activeFlag, ttl,
	).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("propagate lua: %w", err)
	}
	if err := s.refreshTransientTTL(ctx, id, inDegreeKey(id, nodeIdx), activeInputsKey(id, nodeIdx)); err != nil {
		return 0, 0, err
	}
	return int(vals[0]), int(vals[1]), nil
}

func (s *redisState) CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	s.mu.RLock()
	g := s.graphs[id]
	s.mu.RUnlock()
	if g == nil {
		return false, false, nil
	}

	keys := make([]string, 0, 1+totalNodes)
	keys = append(keys, execKey(id, "status"))
	for _, nd := range g.Nodes {
		keys = append(keys, nodeStatusKey(id, nd.Name))
	}

	result, err := checkCompletionLua.Run(ctx, s.rdb, keys, s.ttlSec()).Int64()
	if err != nil {
		return false, false, fmt.Errorf("check completion lua: %w", err)
	}
	switch result {
	case 1:
		return true, false, nil
	case -1:
		return true, true, nil
	default:
		return false, false, nil
	}
}

// ---------------------------------------------------------------------------
// Suspend / signal
// ---------------------------------------------------------------------------

func (s *redisState) SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		return s.suspendOrConsumeMulti(ctx, id, name, spec)
	}

	// Track waiter keys registered in previous iterations so we can clean them
	// up if a later signal is found pre-delivered.
	var registeredWaiters []string

	// Check each awaited signal name.
	for _, sigName := range spec.Signals {
		result, err := suspendOrConsumeLua.Run(ctx, s.rdb,
			[]string{
				signalKey(id, sigName),
				nodeStatusKey(id, name),
				waiterKey(id, sigName),
				suspendedNodesKey(id),
				resumeLockKey(id, name),
			},
			name, s.ttlSec(),
		).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("suspend or consume lua: %w", err)
		}
		if result != nil {
			raw, ok := result.(string)
			if ok && raw != "" {
				// Signal found — clean up any waiter keys from previous iterations.
				if len(registeredWaiters) > 0 {
					pipe := s.rdb.Pipeline()
					for _, wk := range registeredWaiters {
						pipe.Del(ctx, wk)
					}
					pipe.SRem(ctx, suspendedNodesKey(id), name)
					_, _ = pipe.Exec(ctx)
				}
				var data map[string]any
				_ = json.Unmarshal([]byte(raw), &data)
				return &types.SignalPayload{Triggered: types.SignalReceived, Name: sigName, Data: data}, nil
			}
		}
		// This signal was not pre-delivered; a waiter key was registered.
		registeredWaiters = append(registeredWaiters, waiterKey(id, sigName))
	}
	// Node is parked — register timeout in ZSET if spec has a timeout.
	if spec.Timeout > 0 {
		member := timeoutMember(id, name)
		if err := s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: member,
		}).Err(); err != nil {
			return nil, fmt.Errorf("register suspend timeout %q/%q: %w", id, name, err)
		}
	}
	// Extend TTL to prevent key expiry during suspension.
	if err := s.extendExecTTL(ctx, id, name, s.suspendTTL(id, spec)); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *redisState) suspendOrConsumeMulti(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	ttl := s.suspendTTL(id, spec)
	batchKey := signalBatchKey(id, name)

	// Atomically collect every pre-delivered signal into the batch hash. The
	// collection (GET→HSET→DEL per signal) runs in one Lua transition so a crash
	// cannot delete a signal before the quorum check parks the node.
	keys := []string{batchKey}
	args := []any{int(ttl.Seconds())}
	for _, sigName := range spec.Signals {
		keys = append(keys, signalKey(id, sigName))
		args = append(args, sigName)
	}
	collected, err := suspendOrConsumeMultiCollectLua.Run(ctx, s.rdb, keys, args...).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("suspend or consume multi collect %q/%q: %w", id, name, err)
	}

	// Evaluate quorum using the batch hash populated atomically above. Use the
	// last collected signal as the payload's Data field when quorum is reached.
	if len(collected) > 0 {
		lastName := collected[len(collected)-2]
		lastRaw := collected[len(collected)-1]
		payload, ready, err := s.multiSignalPayload(ctx, id, name, lastName, lastRaw, spec)
		if err != nil {
			return nil, err
		}
		if ready {
			s.cleanupMultiSignal(ctx, id, name, spec)
			return payload, nil
		}
	}

	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshal multi-signal spec: %w", err)
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, resumeLockKey(id, name))
	pipe.Set(ctx, nodeStatusKey(id, name), string(types.NodeStatusSuspended), ttl)
	pipe.Set(ctx, waiterSpecKey(id, name), string(specJSON), ttl)
	pipe.Expire(ctx, batchKey, ttl)
	for _, sigName := range spec.Signals {
		pipe.Set(ctx, waiterKey(id, sigName), name, ttl)
	}
	pipe.SAdd(ctx, suspendedNodesKey(id), name)
	pipe.Expire(ctx, suspendedNodesKey(id), ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("park multi-signal waiter: %w", err)
	}
	if spec.Timeout > 0 {
		if err := s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, name),
		}).Err(); err != nil {
			return nil, fmt.Errorf("register multi-signal timeout %q/%q: %w", id, name, err)
		}
	}
	if err := s.extendExecTTL(ctx, id, name, ttl); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *redisState) loadWaiterSpec(ctx context.Context, id types.ExecutionID, nodeName string) (*types.SuspendSpec, error) {
	raw, err := s.rdb.Get(ctx, waiterSpecKey(id, nodeName)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load waiter spec %q/%q: %w", id, nodeName, err)
	}
	var spec types.SuspendSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("unmarshal waiter spec %q/%q: %w", id, nodeName, err)
	}
	return &spec, nil
}

func (s *redisState) addMultiSignal(ctx context.Context, id types.ExecutionID, nodeName string, signalName string, dataJSON string, spec *types.SuspendSpec) (*types.SignalPayload, bool, error) {
	if err := s.rdb.HSet(ctx, signalBatchKey(id, nodeName), signalName, dataJSON).Err(); err != nil {
		return nil, false, fmt.Errorf("add multi-signal %q/%q/%q: %w", id, nodeName, signalName, err)
	}
	_ = s.rdb.Expire(ctx, signalBatchKey(id, nodeName), s.suspendTTL(id, spec)).Err()
	return s.multiSignalPayload(ctx, id, nodeName, signalName, dataJSON, spec)
}

func (s *redisState) multiSignalPayload(ctx context.Context, id types.ExecutionID, nodeName string, signalName string, dataJSON string, spec *types.SuspendSpec) (*types.SignalPayload, bool, error) {
	rawAll, err := s.rdb.HGetAll(ctx, signalBatchKey(id, nodeName)).Result()
	if err != nil {
		return nil, false, fmt.Errorf("read multi-signal batch %q/%q: %w", id, nodeName, err)
	}
	if len(rawAll) < signalQuorum(spec) {
		return nil, false, nil
	}

	all := make(map[string]map[string]any, len(rawAll))
	for name, raw := range rawAll {
		var payload map[string]any
		_ = json.Unmarshal([]byte(raw), &payload)
		all[name] = payload
	}
	var data map[string]any
	_ = json.Unmarshal([]byte(dataJSON), &data)
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      signalName,
		Data:      data,
		All:       all,
	}, true, nil
}

func (s *redisState) cleanupMultiSignal(ctx context.Context, id types.ExecutionID, nodeName string, spec *types.SuspendSpec) {
	pipe := s.rdb.Pipeline()
	for _, sigName := range spec.Signals {
		pipe.Del(ctx, waiterKey(id, sigName))
	}
	pipe.Del(ctx, waiterSpecKey(id, nodeName), signalBatchKey(id, nodeName))
	pipe.SRem(ctx, suspendedNodesKey(id), nodeName)
	pipe.ZRem(ctx, timeoutZSetKey, timeoutMember(id, nodeName))
	_, _ = pipe.Exec(ctx)
}

func signalQuorum(spec *types.SuspendSpec) int {
	if spec == nil {
		return 1
	}
	if spec.Quorum > 0 {
		return spec.Quorum
	}
	if len(spec.Signals) > 0 {
		return len(spec.Signals)
	}
	return 1
}

func (s *redisState) DeliverSignal(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, *types.SignalPayload, error) {
	dataJSON, _ := json.Marshal(data)

	waiter, err := s.rdb.Get(ctx, waiterKey(id, signalName)).Result()
	if err != nil && err != redis.Nil {
		return "", nil, fmt.Errorf("get waiter: %w", err)
	}
	if waiter != "" {
		spec, specErr := s.loadWaiterSpec(ctx, id, waiter)
		if specErr != nil {
			return "", nil, specErr
		}
		if spec != nil && spec.Mode == types.ModeMultiSignal {
			payload, ready, err := s.addMultiSignal(ctx, id, waiter, signalName, string(dataJSON), spec)
			if err != nil {
				return "", nil, err
			}
			if !ready {
				return "", nil, nil
			}
			s.cleanupMultiSignal(ctx, id, waiter, spec)
			return waiter, payload, nil
		}
	}

	result, err := signalOrStoreLua.Run(ctx, s.rdb,
		[]string{signalKey(id, signalName), waiterKey(id, signalName), suspendedNodesKey(id)},
		string(dataJSON), s.ttlSec(),
	).Result()
	if err != nil && err != redis.Nil {
		return "", nil, fmt.Errorf("signal or store lua: %w", err)
	}
	if result != nil {
		if nodeName, ok := result.(string); ok && nodeName != "" {
			// Node is being resumed — remove its timeout entry from the ZSET.
			s.cleanupOnResume(ctx, id, nodeName)
			return nodeName, nil, nil
		}
	}

	if s.db != nil && !s.transient {
		rec := &store.SignalRecord{
			ExecutionID: id,
			SignalName:  signalName,
			Payload:     dataJSON,
			CreatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "save_signal", func(ctx context.Context) error {
			return s.db.SaveSignal(ctx, rec)
		})
	}
	return "", nil, nil
}

// cleanupOnResume removes the timeout ZSET entry for a node that is being resumed.
func (s *redisState) cleanupOnResume(ctx context.Context, id types.ExecutionID, nodeName string) {
	s.rdb.ZRem(ctx, timeoutZSetKey, timeoutMember(id, nodeName))
}

// PeekResumeTarget returns the node name suspended and waiting for signalName,
// or "" when no waiter exists. It does not consume the signal.
func (s *redisState) PeekResumeTarget(ctx context.Context, id types.ExecutionID, signalName string) (string, error) {
	waiter, err := s.rdb.Get(ctx, waiterKey(id, signalName)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("peek waiter %q/%q: %w", id, signalName, err)
	}
	return waiter, nil
}

// DeliverSignalWithOutbox atomically consumes a signal and writes the resume
// delivery intent to the outbox in one Lua transition. See
// deliverSignalWithOutboxLua for the single/multi-signal semantics.
func (s *redisState) DeliverSignalWithOutbox(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any, intent engine.ResumeIntent) (string, *types.SignalPayload, bool, error) {
	dataJSON, _ := json.Marshal(data)
	ttl := s.ttlSec()
	nowMs := time.Now().UTC().UnixMilli()
	timeoutMemberStr := ""
	if intent.NodeName != "" {
		timeoutMemberStr = timeoutMember(id, intent.NodeName)
	}

	var (
		spec      *types.SuspendSpec
		specErr   error
		entryID   string
		entryBody string
	)
	if intent.NodeName != "" {
		spec, specErr = s.loadWaiterSpec(ctx, id, intent.NodeName)
		if specErr != nil {
			return "", nil, false, fmt.Errorf("load waiter spec for signal %q/%q: %w", id, intent.NodeName, specErr)
		}
		payload := &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: data}
		task := engine.Task{
			ExecutionID:  id,
			NodeName:     intent.NodeName,
			NodeIdx:      intent.NodeIdx,
			Type:         engine.TaskTypeNodeResume,
			Payload:      payload,
			ActivationID: intent.ActivationID,
			AutoDepth:    intent.AutoDepth,
		}
		entryID = fmt.Sprintf("resume/%s/%s/%d/signal/%s", id, intent.NodeName, intent.ActivationID, signalName)
		body, err := marshalRedisOutboxEntry(entryID, task, time.Now().UTC())
		if err != nil {
			return "", nil, false, fmt.Errorf("marshal resume outbox %q/%q: %w", id, intent.NodeName, err)
		}
		entryBody = body
	}

	multi := 0
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		multi = 1
	}
	quorum := signalQuorum(spec)

	keys := []string{
		signalKey(id, signalName),
		waiterKey(id, signalName),
		suspendedNodesKey(id),
		signalBatchKey(id, intent.NodeName),
		waiterSpecKey(id, intent.NodeName),
		resumeLockKey(id, intent.NodeName),
		outboxReadyKey(id),
		outboxBodyKey(id),
		timeoutZSetKey,
	}
	if multi == 1 {
		for _, sig := range spec.Signals {
			keys = append(keys, waiterKey(id, sig))
		}
	}
	args := []any{
		string(dataJSON), int(ttl), intent.NodeName,
		entryID, entryBody, nowMs, multi, quorum, signalName, timeoutMemberStr,
	}

	result, err := deliverSignalWithOutboxLua.Run(ctx, s.rdb, keys, args...).Result()
	if err != nil && err != redis.Nil {
		return "", nil, false, fmt.Errorf("deliver signal with outbox %q/%q: %w", id, signalName, err)
	}
	nodeName, _ := result.(string)
	if nodeName == "" {
		return "", nil, false, nil
	}
	return nodeName, nil, true, nil
}

func (s *redisState) AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	result, err := resumeNodeLua.Run(ctx, s.rdb,
		[]string{resumeLockKey(id, name)},
		s.ttlSec(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("resume lock lua: %w", err)
	}
	return result == 1, nil
}

func (s *redisState) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error) {
	result, err := revokeSignalLua.Run(ctx, s.rdb,
		[]string{signalKey(id, signalName), waiterKey(id, signalName)},
		fmt.Sprintf("xflow:exec:{%s}:node:", id),
		":resume_lock",
	).Int64()
	if err != nil {
		return false, fmt.Errorf("revoke signal lua: %w", err)
	}
	if result == 1 && s.db != nil && !s.transient {
		s.auditWrite(ctx, "revoke_signal", func(ctx context.Context) error {
			_, err := s.db.RevokeSignal(ctx, id, signalName)
			return err
		})
	}
	return result == 1, nil
}

func (s *redisState) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	result, err := resuspendAtomicLua.Run(ctx, s.rdb,
		[]string{
			resumeLockKey(id, nodeName),
			waiterKey(id, oldSignalName),
			signalKey(id, newSignalName),
			waiterKey(id, newSignalName),
			suspendedNodesKey(id),
		},
		nodeName, s.ttlSec(),
	).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("resuspend atomic lua: %w", err)
	}
	if result != nil {
		raw, ok := result.(string)
		if ok && raw != "" {
			var data map[string]any
			_ = json.Unmarshal([]byte(raw), &data)
			return &types.SignalPayload{Triggered: types.SignalReceived, Name: newSignalName, Data: data}, nil
		}
	}
	// Node is re-parked — register timeout in ZSET if spec has a timeout.
	if spec.Timeout > 0 {
		// Remove any old timeout entry first (signal name may have changed).
		if err := s.rdb.ZRem(ctx, timeoutZSetKey, timeoutMember(id, nodeName)).Err(); err != nil && err != redis.Nil {
			return nil, fmt.Errorf("clear resuspend timeout %q/%q: %w", id, nodeName, err)
		}
		if err := s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, nodeName),
		}).Err(); err != nil {
			return nil, fmt.Errorf("register resuspend timeout %q/%q: %w", id, nodeName, err)
		}
	}
	// Extend TTL to prevent key expiry during suspension.
	if err := s.extendExecTTL(ctx, id, nodeName, s.suspendTTL(id, spec)); err != nil {
		return nil, err
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Cancel support
// ---------------------------------------------------------------------------

func (s *redisState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	return s.rdb.SMembers(ctx, suspendedNodesKey(id)).Result()
}

// cleanupOnCancel removes timeout ZSET entries for all suspended nodes of a
// canceled execution and deletes the suspended_nodes SET itself.
func (s *redisState) cleanupOnCancel(ctx context.Context, id types.ExecutionID) {
	nodes, err := s.rdb.SMembers(ctx, suspendedNodesKey(id)).Result()
	pipe := s.rdb.Pipeline()
	if err == nil {
		for _, name := range nodes {
			pipe.ZRem(ctx, timeoutZSetKey, timeoutMember(id, name))
		}
	}
	// These indexes are execution-scoped. Once cancellation is authoritative,
	// no worker may recover work from them; removing them prevents stale leases
	// or undelivered outbox tasks from reviving a canceled execution.
	pipe.Del(ctx,
		suspendedNodesKey(id),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
		remainingNodesKey(id),
		failedNodesKey(id),
	)
	_, _ = pipe.Exec(ctx)
}

// ---------------------------------------------------------------------------
// Output store
// ---------------------------------------------------------------------------

func (s *redisState) PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	b, _ := json.Marshal(data)
	if err := s.rdb.Set(ctx, outputKey(id, name), string(b), s.getExecTTL(id)).Err(); err != nil {
		return err
	}
	return s.refreshTransientTTL(ctx, id, outputKey(id, name))
}

func (s *redisState) GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	raw, err := s.rdb.Get(ctx, outputKey(id, name)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

// ---------------------------------------------------------------------------
// TTL renewal
// ---------------------------------------------------------------------------

// extendExecTTL renews the TTL on all keys related to an execution and the
// specific suspended node. Called when a node is parked (suspended) to prevent
// keys from expiring while waiting for a signal. A failure here is surfaced
// rather than swallowed: if the TTL is not extended the execution/node keys may
// expire while a node is suspended, causing the eventual resume to silently
// target missing state.
func (s *redisState) extendExecTTL(ctx context.Context, id types.ExecutionID, nodeName string, ttl time.Duration) error {
	pipe := s.rdb.Pipeline()
	prefix := fmt.Sprintf("xflow:exec:{%s}", id)
	pipe.Expire(ctx, prefix+":status", ttl)
	pipe.Expire(ctx, prefix+":params", ttl)
	pipe.Expire(ctx, prefix+":runtime", ttl)
	pipe.Expire(ctx, prefix+":trace_id", ttl)
	pipe.Expire(ctx, prefix+":span_id", ttl)
	pipe.Expire(ctx, prefix+":graph", ttl)
	pipe.Expire(ctx, prefix+":node:"+nodeName+":status", ttl)
	pipe.Expire(ctx, prefix+":output:"+nodeName, ttl)
	pipe.Expire(ctx, prefix+":suspended_nodes", ttl)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("extend exec ttl %q/%q: %w", id, nodeName, err)
	}
	return nil
}

// suspendTTL returns the TTL to use when a node is suspended.
// It picks the larger of the execution's base TTL and spec.Timeout + 1 hour.
func (s *redisState) suspendTTL(id types.ExecutionID, spec *types.SuspendSpec) time.Duration {
	// Start with the per-execution override if set, otherwise use the adapter default.
	s.ttlMu.RLock()
	ttl := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if ttl == 0 {
		ttl = s.execTTL
	}

	if spec != nil && spec.Timeout > 0 {
		candidate := spec.Timeout + 1*time.Hour
		if candidate > ttl {
			ttl = candidate
		}
	}
	return ttl
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// doneChannel returns the Pub/Sub channel name for execution completion events.
func doneChannel(id types.ExecutionID) string {
	return fmt.Sprintf("xflow:exec:{%s}:done", id)
}

func (s *redisState) PublishExecutionEvent(ctx context.Context, event engine.ExecutionEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}
	return s.rdb.Publish(ctx, doneChannel(event.ExecutionID), data).Err()
}

func (s *redisState) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan engine.ExecutionEvent, error) {
	pubsub := s.rdb.Subscribe(ctx, doneChannel(id))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe execution events %q: %w", id, err)
	}
	out := make(chan engine.ExecutionEvent, 8)
	go func() {
		defer close(out)
		defer func() { _ = pubsub.Close() }()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var event engine.ExecutionEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					continue
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// ---------------------------------------------------------------------------
// Sub-execution support
// ---------------------------------------------------------------------------

func (s *redisState) CreateSubExecution(ctx context.Context, sub *engine.SubExecution) error {
	key := fmt.Sprintf("xflow:exec:{%s}:subs:%s", sub.ParentExecID, sub.ParentNode)
	data, _ := json.Marshal(sub)
	if err := s.rdb.HSet(ctx, key, string(sub.ChildExecID), data).Err(); err != nil {
		return err
	}
	return s.refreshTransientTTL(ctx, sub.ParentExecID, key)
}

func (s *redisState) CompleteSubExecution(ctx context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	key := fmt.Sprintf("xflow:exec:{%s}:subs:%s", parentExecID, parentNode)

	sub := &engine.SubExecution{
		ParentExecID: parentExecID,
		ParentNode:   parentNode,
		ChildExecID:  childExecID,
		Status:       status,
		Result:       result,
	}
	data, _ := json.Marshal(sub)
	if err := s.rdb.HSet(ctx, key, string(childExecID), data).Err(); err != nil {
		return false, err
	}
	if err := s.refreshTransientTTL(ctx, parentExecID, key); err != nil {
		return false, err
	}

	// Check if all sub-executions are done.
	all, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return false, err
	}
	for _, v := range all {
		var entry engine.SubExecution
		if err := json.Unmarshal([]byte(v), &entry); err != nil {
			continue
		}
		if entry.Status == types.ExecutionStatusRunning {
			return false, nil
		}
	}
	return true, nil
}

func (s *redisState) GetSubExecutionResults(ctx context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	key := fmt.Sprintf("xflow:exec:{%s}:subs:%s", parentExecID, parentNode)
	all, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, 0, len(all))
	for _, v := range all {
		var entry engine.SubExecution
		if err := json.Unmarshal([]byte(v), &entry); err != nil {
			continue
		}
		if entry.Result != nil {
			results = append(results, entry.Result)
		}
	}
	return results, nil
}
