package rstate

import "github.com/redis/go-redis/v9"

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
redis.call('EXPIRE', KEYS[1], ttl)
local ai = tonumber(redis.call('GET', KEYS[2]) or '0')
return {newVal, ai}
`)

// suspendOrConsumeLua atomically checks for an existing signal or parks the node.
// KEYS[1] = signal key, KEYS[2] = node status key, KEYS[3] = waiter key,
// KEYS[4] = suspended_nodes SET, KEYS[5] = resume_lock key, KEYS[6] = timeout ZSET
// ARGV[1] = node name, ARGV[2] = ttl seconds, ARGV[3] = timeout score (0 = no timeout),
// ARGV[4] = timeout member
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
local timeoutScore = tonumber(ARGV[3])
if timeoutScore > 0 then
    redis.call('ZADD', KEYS[6], timeoutScore, ARGV[4])
    redis.call('EXPIRE', KEYS[6], tonumber(ARGV[2]))
end
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
// KEYS[9] = timeout ZSET, KEYS[10] = node meta hash (live activation source),
// KEYS[11..] = multi waiter keys (cleanup on quorum).
// ARGV[1] = signal data JSON, ARGV[2] = ttl seconds, ARGV[3] = node name,
// ARGV[4] = (unused, empty), ARGV[5] = outbox entry body JSON (activation_id absent),
// ARGV[6] = now ms, ARGV[7] = multi flag, ARGV[8] = quorum,
// ARGV[9] = signal name, ARGV[10] = timeout member,
// ARGV[11] = execution id, ARGV[12] = node name, ARGV[13] = signal name (for entryID).
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
local nodeMetaKey = KEYS[10]
local dataJSON = ARGV[1]
local ttl = tonumber(ARGV[2])
local nodeName = ARGV[3]
local entryBody = ARGV[5]
local nowMs = tonumber(ARGV[6])
local multi = tonumber(ARGV[7]) == 1
local quorum = tonumber(ARGV[8])
local signalName = ARGV[9]
local timeoutMember = ARGV[10]
local execID = ARGV[11]
local nodeNameForID = ARGV[12]
local signalNameForID = ARGV[13]

local waiter = redis.call('GET', waiterKey)
if not waiter then
    redis.call('SET', signalKey, dataJSON, 'EX', ttl)
    return ''
end

-- Read LIVE activation_id from node meta inside the transaction, closing the
-- TOCTOU window where a concurrent re-suspend under a new activation could
-- leave a stale activation in the resume entry.
local liveActivation = tonumber(redis.call('HGET', nodeMetaKey, 'activation_id') or '0')
local liveActivationStr = string.format('%d', liveActivation)

-- Build the authoritative entryID from the live activation.
local entryID = 'resume/' .. execID .. '/' .. nodeNameForID .. '/' .. liveActivationStr .. '/signal/' .. signalNameForID

-- Stamp activation_id into the entry body exactly once. Go marshaled the body
-- with ActivationID=0 (omitempty drops it), so we insert the live value as a
-- fresh top-level key right after the opening '{'.
local stampActivation = function(body)
    if liveActivation == 0 then
        return body
    end
    return '{' .. '"activation_id":' .. liveActivationStr .. ',' .. body:sub(2)
end

-- Also patch the placeholder "id" field inside the body to carry the real entryID.
local patchID = function(body)
    -- The marshal produced "id":"resume/.../0/signal/..." — replace the first
    -- occurrence of the placeholder ID with the live one using plain string.find
    -- (gsub treats '-' as a pattern metachar, which breaks on execution IDs).
    local placeholder = '"id":"resume/' .. execID .. '/' .. nodeNameForID .. '/0/signal/' .. signalNameForID .. '"'
    local replacement = '"id":"' .. entryID .. '"'
    local startPos, endPos = body:find(placeholder, 1, true)
    if startPos then
        return body:sub(1, startPos - 1) .. replacement .. body:sub(endPos + 1)
    end
    return body
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
    -- Build the All map as a raw JSON object from batch values WITHOUT decoding
    -- individual signal payloads through cjson, preserving exact data fidelity
    -- (avoids empty-object {} → [] mutation and int64>2^53 precision loss).
    local parts = {}
    for i = 1, #values, 2 do
        table.insert(parts, cjson.encode(values[i]) .. ':' .. values[i + 1])
    end
    local allJSON = '{' .. table.concat(parts, ',') .. '}'
    -- Inject All into the entry body. The entry is known-structure JSON from
    -- marshalRedisOutboxEntry; payload is at .task.payload. We find the payload
    -- object's opening brace after the "payload": key and insert the All field
    -- right after it: {"triggered":...,"All":{...},...}
    local payloadPos = entryBody:find('"payload":')
    local finalBody
    if payloadPos then
        local bracePos = entryBody:find('{', payloadPos + 10)
        if bracePos then
            finalBody = entryBody:sub(1, bracePos) .. '"All":' .. allJSON .. ',' .. entryBody:sub(bracePos + 1)
        end
    end
    if not finalBody then
        -- Fallback for unexpected structure: cjson round-trip (correctness).
        local entry = cjson.decode(entryBody)
        entry.task.payload.All = cjson.decode(allJSON)
        finalBody = cjson.encode(entry)
    end
    -- Patch ID and stamp activation into the final body after All splice.
    finalBody = patchID(finalBody)
    finalBody = stampActivation(finalBody)
    for i = 11, #KEYS do
        redis.call('DEL', KEYS[i])
    end
    redis.call('DEL', waiterSpecKey, batchKey, resumeLockKey)
    redis.call('SREM', suspendedKey, nodeName)
    if timeoutMember ~= '' then
        redis.call('ZREM', timeoutKey, timeoutMember)
    end
    writeOutbox(finalBody)
    return nodeName
end

-- Guard FIRST, before tearing down any waiter state: if the Go-side pre-peek
-- missed the waiter (TOCTOU — the waiter became visible only after Go built the
-- call), entryBody is empty and we cannot construct a valid resume outbox entry.
-- Previously the waiter / spec / suspended-set / timeout state was deleted
-- BEFORE this check, so on the empty-body path the node was stranded suspended
-- with no waiter — no future signal could ever resume it, and only an exec-TTL
-- expiry would clear it. Store the signal and leave the entire waiter state
-- intact so a subsequent delivery (whose Go peek now succeeds) or the suspend
-- timeout drives the resume. Do NOT remove the timeout member here.
if entryBody == '' then
    redis.call('SET', signalKey, dataJSON, 'EX', ttl)
    return ''
end
-- Committing the resume: consume the waiter/signal state atomically with the
-- outbox write. Only reached when we have a valid entry body to deliver.
redis.call('DEL', signalKey, waiterKey, resumeLockKey, waiterSpecKey)
redis.call('SREM', suspendedKey, nodeName)
if timeoutMember ~= '' then
    redis.call('ZREM', timeoutKey, timeoutMember)
end
local finalBody = patchID(entryBody)
finalBody = stampActivation(finalBody)
writeOutbox(finalBody)
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
// KEYS[1] = status key, KEYS[2] = error key (optional; omitted when errMsg is empty)
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
if ARGV[2] ~= '' and #KEYS >= 2 then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', ttl)
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
// ARGV[1] = new status, ARGV[2] = output JSON (or ""), ARGV[3] = ttl seconds, ARGV[4] = activation id, ARGV[5] = private output (0|1),
// ARGV[6] = started_at milliseconds (0 = none), ARGV[7] = finished_at milliseconds (0 = none)
// Returns {written, effectivePrivate}. effectivePrivate is a strict 0|1
// decision made at the same linearization point as the snapshot write.
var upsertNodeLua = redis.NewScript(`
local ttl = tonumber(ARGV[3])
-- Calculate privacy before any stale/terminal fence. A stale private recovery
-- snapshot still has to redact an older SQL projection. Once private, it is
-- monotonic for this node output lifetime.
local existingPrivate = redis.call('HGET', KEYS[3], 'private_output') == '1'
local requestedPrivate = tonumber(ARGV[5] or '0') == 1
local effectivePrivate = (existingPrivate or requestedPrivate) and 1 or 0
if requestedPrivate then
    redis.call('HSET', KEYS[3], 'private_output', '1')
end
-- A later public upsert must not let the existing private marker expire before
-- the refreshed runtime output. This also covers a stale public snapshot.
if effectivePrivate == 1 then
    redis.call('EXPIRE', KEYS[3], ttl)
end
local existing = redis.call('GET', KEYS[1])
if existing == 'success' or existing == 'failed' or existing == 'skipped' or existing == 'canceled' or existing == 'continued' or existing == 'pinned' then
    local oldActivation = tonumber(redis.call('HGET', KEYS[3], 'activation_id') or '0')
    local newActivation = tonumber(ARGV[4] or '0')
    if newActivation <= oldActivation then
        return {0, effectivePrivate}
    end
end
if existing == 'committing' or existing == 'waiting' then
    if ARGV[1] == 'running' then
        return {0, effectivePrivate}
    end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
-- Timing is write-once: HSETNX only fills a missing field, so a retry keeps
-- the first started_at and a duplicate terminal/cancel write keeps the first
-- finished_at. This runs after the fence above, so a rejected stale snapshot
-- cannot stamp either fact.
if tonumber(ARGV[6] or '0') > 0 then
    redis.call('HSETNX', KEYS[3], 'started_at_ms', ARGV[6])
end
if tonumber(ARGV[7] or '0') > 0 then
    redis.call('HSETNX', KEYS[3], 'finished_at_ms', ARGV[7])
end
if ARGV[2] ~= '' then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', ttl)
end
-- Keep normal accepted-write metadata expiration behavior, even where no
-- privacy marker exists (EXPIRE on a missing hash is harmless).
redis.call('EXPIRE', KEYS[3], ttl)
return {1, effectivePrivate}
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
if status == 'success' or status == 'failed' or status == 'skipped' or status == 'canceled' or status == 'continued' or status == 'pinned' then
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
// output JSON, node name, multi flag, quorum, signal count, spec JSON, names,
// trailing private-output bit.
// Returns {committed, signal name, signal payload JSON, multi-payload JSON}.
var suspendTaskLeaseLua = redis.NewScript(`
local terminal = function(value)
    return value == 'success' or value == 'failed' or value == 'skipped' or value == 'canceled' or value == 'continued' or value == 'pinned'
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
if tonumber(ARGV[#ARGV] or '0') == 1 then
    redis.call('HSET', KEYS[2], 'private_output', '1')
end
redis.call('SET', KEYS[1], 'suspended', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_id', '', 'lease_token', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '', 'lease_generation', '0')
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
// ARGV[12] = queued task unit index
// ARGV[13] = queued task generation
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

if status == 'success' or status == 'failed' or status == 'skipped' or status == 'canceled' or status == 'continued' or status == 'pinned' then
	if taskActivation <= 0 or prevActivation >= taskActivation then
		return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
	end
end

if status == 'committing' or status == 'waiting' then
	return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
end

if status == 'running' and prevLeaseToken ~= '' then
	local prevDeadlineMs = tonumber(redis.call('HGET', KEYS[2], 'lease_deadline_ms') or '0')
	if prevDeadlineMs <= 0 and prevIssuedAtMs > 0 and prevLeaseTTLms > 0 then
		-- Compatibility with leases written before lease_deadline_ms was
		-- introduced; mirror ListExpiredLeases' fallback so both index paths
		-- judge expiry on the same basis.
		prevDeadlineMs = prevIssuedAtMs + prevLeaseTTLms
	end
	local leaseValid = false
	if prevDeadlineMs > 0 then
		leaseValid = nowMs < prevDeadlineMs
	elseif prevIssuedAtMs == 0 or prevLeaseTTLms <= 0 then
		-- No deadline and no issued/ttl: treat as acquirable (stale meta).
		leaseValid = false
	else
		leaseValid = nowMs < (prevIssuedAtMs + prevLeaseTTLms)
	end
	if leaseValid then
		return {0, status, prevAttempt, prevActivation, prevAutoDepth, prevLeaseToken, prevIssuedAtMs, prevLeaseTTLms}
	end
end

-- Attempt counts retries within a single activation. A cyclic re-entry carries
-- a higher taskActivation than the node's previous activation; that is a fresh
-- execution of the node, not a retry, so the counter restarts at 1. Carrying it
-- across activations would let a looping node exhaust MaxAttempts and be
-- misclassified as permanently failed.
local nextAttempt = 1
if prevActivation == taskActivation then
	nextAttempt = prevAttempt + 1
end
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
	'lease_generation', tonumber(ARGV[13] or '0'),
	'node_idx', tonumber(ARGV[11] or '0'),
	'unit_idx', tonumber(ARGV[12] or '-1'))
-- The first lease this node ever receives fixes started_at; a retry and a
-- later cyclic activation re-enter here and must not move it.
if nowMs > 0 then
    redis.call('HSETNX', KEYS[2], 'started_at_ms', nowMs)
end
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
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '', 'lease_generation', '0')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
redis.call('ZREM', KEYS[3], ARGV[3])
return 1
`)

// repairLeaseIndexLua conditionally ZAdds a corrected score for an existing
// lease-index member, but ONLY when the node is still in a non-terminal state
// and its lease token has not changed since the caller observed it. This
// prevents a bare Go-side ZAdd from racing with a concurrent commitNodeLua
// that terminalized the node and ZREMed the member: without this fence the
// sweeper would re-add a member for an already-finished node.
// KEYS[1] = node status key, KEYS[2] = node meta hash, KEYS[3] = lease expiry ZSET
// ARGV[1] = corrected deadline ms (float score), ARGV[2] = member, ARGV[3] = expected lease token
// Returns 1 (repaired) or 0 (skipped — node terminal or token stale).
var repairLeaseIndexLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if not status then
    return 0
end
if status == 'success' or status == 'failed' or status == 'skipped' or status == 'canceled' or status == 'continued' or status == 'pinned' then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
if token == '' or token ~= ARGV[3] then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 0
end
redis.call('ZADD', KEYS[3], tonumber(ARGV[1]), ARGV[2])
return 1
`)

// revokeSignalLua atomically removes a signal that has not yet been consumed.
// KEYS[1] = signal key, KEYS[2] = waiter key (stores node name waiting for this signal)
// KEYS[3] = resume lock key (pre-built by the caller to satisfy Cluster key declaration)
// Returns 1 (revoked) or 0 (signal not found or already consumed/resumed).
var revokeSignalLua = redis.NewScript(`
local signal = redis.call('GET', KEYS[1])
if not signal then return 0 end
local nodeName = redis.call('GET', KEYS[2])
if nodeName then
    if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
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
// The early-return guard checks terminal states AND canceling/timeout to prevent
// a concurrent completion from overwriting a cancel/timeout already in progress.
var checkCompletionLua = redis.NewScript(`
local execStatus = redis.call('GET', KEYS[1])
if execStatus == 'success' or execStatus == 'failed' or execStatus == 'canceled' or execStatus == 'timeout' or execStatus == 'canceling' then
    return 0
end
local anyFailed = false
for i = 2, #KEYS do
    local ns = redis.call('GET', KEYS[i])
    if ns == 'failed' then
        anyFailed = true
    elseif ns ~= 'success' and ns ~= 'skipped' and ns ~= 'canceled' and ns ~= 'continued' and ns ~= 'pinned' then
        return 0
    end
end
local final = anyFailed and 'failed' or 'success'
redis.call('SET', KEYS[1], final, 'EX', tonumber(ARGV[1]))
return anyFailed and -1 or 1
`)

// concurrencyIndexTouchLua adds a workflow tag to its namespace's index and
// extends the index's expiry to cover it, never shortening what is there.
//
// The extend-only rule is shared with the entry-activation index
// (prepareEntryActivationTransitionLua's touch_activation) and exists for the
// same reason: two writers register here with different lifetimes — an
// admission, which knows the queue wait it has to keep the line discoverable
// for, and a release, which knows nothing about the waiters already in line —
// and a plain EXPIRE from the shorter one would cut the index back under a
// deadline that is still live.
//
// Unlike that index, this one is never deliberately permanent: every writer
// registers with an expiry, so a PTTL of -1 means the SADD above just created
// the key and it still needs one. Hence `<` rather than the entry-activation
// form's "skip -1": skipping it here would leave a freshly created index with
// no expiry at all — the opposite of the leak this script is closing.
//
// The index is a single key, so this script is Redis Cluster safe even though
// the keys it indexes are not: the SADD and the expiry can never be split by a
// concurrent registration.
//
// KEYS[1] = namespace workflow index, ARGV[1] = workflow tag,
// ARGV[2] = how long the index has to survive, in milliseconds
var concurrencyIndexTouchLua = redis.NewScript(`
redis.call('SADD', KEYS[1], ARGV[1])
local want = tonumber(ARGV[2])
local current = redis.call('PTTL', KEYS[1])
if current < want then
    redis.call('PEXPIRE', KEYS[1], want)
end
return 1
`)

// workflowAdmitLua charges one workflow slot, refusing when the workflow is at
// its limit. This is the whole limit: concurrent submissions from different
// replicas serialize on the slot ZSET, so the check-and-charge cannot race.
//
// KEYS[1] = workflow slot ZSET, KEYS[2] = queue ZSET,
// KEYS[3] = queue sequence counter, KEYS[4] = capacity string,
// KEYS[5] = queue deadline ZSET
// ARGV[1] = slot member (the execution ID), ARGV[2] = limit,
// ARGV[3] = admission score (unix seconds), ARGV[4] = TTL seconds for the keys,
// ARGV[5] = 1 queue mode / 0 reject mode, ARGV[6] = max_queued (0 unbounded),
// ARGV[7] = queue deadline (unix milliseconds, 0 = no bound),
// ARGV[8] = queue wait (milliseconds) this submission may spend in the line
// Returns {code, position, added, promoted...}: code 1 admitted, 2 queued,
// 0 refused at the limit, 3 refused because the queue is full. position is the
// 1-based place in the line for a queued submission (0 otherwise), added is 1
// when this call is what put the member in the slot set or the line, and the
// rest are the execution IDs this call promoted into free slots.
//
// The decision is idempotent on the member. A member already in the slot set or
// the line was charged by an earlier attempt of the SAME admission — the
// entry-seed path derives its execution ID from the admission key, so a
// redelivered batch asks for the same member — and this call reports what that
// attempt decided instead of charging a second slot. Without this, a seed whose
// response was lost would leave the workflow one short of its limit forever, and
// a redelivery of a batch whose execution already finished would charge a slot
// for it with nothing left to release. added is what tells the two apart: only a
// charge this call made may be given back (see the seed path's duplicate
// handling), because only then is it certain the member was not already
// accounted for.
//
// The namespace workflow index is NOT written here, and that is the point: it
// deliberately carries no {...} tag (it aggregates every workflow in the
// namespace), so it can never share a slot with this script's keys. Redis
// Cluster refuses a script whose keys hash to different slots, so keeping the
// index out is what makes this script runnable there at all. The caller
// registers the tag beforehand — before the charge, not after, so a slot that
// is charged is always already discoverable by the sweeper.
//
// Admission promotes when the line is non-empty and capacity is free. That
// combination is not a normal state — a release fills capacity as it frees it —
// but it is reachable: a re-published definition can raise the limit, and the
// execution that declared the new number finds the waiters already there. Using
// that free capacity for the newcomer instead would jump the line, so the
// waiter goes first and the newcomer takes what is left, which is exactly the
// order this loop puts them in.
var workflowAdmitLua = redis.NewScript(`
local member = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local mode = tonumber(ARGV[5])
-- The line's own keys have to outlive the longest deadline they hold, and that
-- is this submission's wait: expiring the line on the slot TTL alone takes the
-- entry away before its deadline can be reached whenever a queue_timeout
-- exceeds that TTL, and the parked execution then reads queued with nothing
-- left to expire it. The execution's own keys are lengthened the same way
-- (createExecution), so the two halves of one wait now share an outer bound.
local waitMs = tonumber(ARGV[8]) or 0
local lineTTL = ttl * 1000 + waitMs
-- Never SHORTEN that bound either. A longer wait admitted earlier into this
-- same line still holds a deadline further out than this submission's own, and
-- lowering the keys' expiry to this one's would put that deadline back at risk.
local current = redis.call('PTTL', KEYS[2])
if current > 0 and current > lineTTL then
    lineTTL = current
end
redis.call('SET', KEYS[4], limit, 'PX', lineTTL)
-- Idempotency first, before the capacity check: a member that is already here
-- was charged by an earlier attempt of the same admission, and re-deciding it
-- would either charge twice or refuse a submission that already has its slot.
if redis.call('ZSCORE', KEYS[1], member) then
    return {1, 0, 0}
end
local rank = redis.call('ZRANK', KEYS[2], member)
if rank then
    return {2, rank + 1, 0}
end
local holders = redis.call('ZCARD', KEYS[1])
local waiting = redis.call('ZCARD', KEYS[2])
if holders < limit and waiting == 0 then
    redis.call('ZADD', KEYS[1], now, member)
    redis.call('EXPIRE', KEYS[1], ttl)
    return {1, 0, 1}
end
if mode == 0 then
    return {0, 0, 0}
end
local maxQueued = tonumber(ARGV[6])
if maxQueued > 0 and waiting >= maxQueued then
    return {3, 0, 0}
end
local seq = redis.call('INCR', KEYS[3])
redis.call('PEXPIRE', KEYS[3], lineTTL)
redis.call('ZADD', KEYS[2], seq, member)
redis.call('PEXPIRE', KEYS[2], lineTTL)
local deadline = tonumber(ARGV[7])
if deadline > 0 then
    redis.call('ZADD', KEYS[5], deadline, member)
    redis.call('PEXPIRE', KEYS[5], lineTTL)
end
local position = redis.call('ZCARD', KEYS[2])
local promoted = {}
while redis.call('ZCARD', KEYS[1]) < limit do
    local head = redis.call('ZPOPMIN', KEYS[2])
    if #head == 0 then
        break
    end
    redis.call('ZADD', KEYS[1], now, head[1])
    redis.call('ZREM', KEYS[5], head[1])
    table.insert(promoted, head[1])
end
redis.call('EXPIRE', KEYS[1], ttl)
local out = {2, position, 1}
for _, id in ipairs(promoted) do
    table.insert(out, id)
end
return out
`)

// workflowReleaseLua frees the slot one execution holds. It is idempotent: a
// member that is already gone releases nothing and returns 0, which is what
// lets every terminal writer call it unconditionally.
//
// KEYS[1] = workflow slot ZSET, KEYS[2] = queue ZSET,
// KEYS[3] = queue deadline ZSET, KEYS[4] = capacity string
// ARGV[1] = slot member (the execution ID), ARGV[2] = promotion score
// (unix seconds), ARGV[3] = TTL seconds for the set
// Returns {drained, wasQueued, promoted...}: drained is 1 when the workflow's
// concurrency state was emptied and deleted, wasQueued is 1 when the member was
// still waiting in the line (so its parked intents belong to an execution that
// never ran), and the rest are the execution IDs promoted into the freed
// capacity.
//
// The promotion is here, in the same script as the release, because the two are
// one decision: capacity is free only after the release, and a promotion
// computed outside this script could hand the same slot to two executions. The
// promoted execution's own status and outbox are in its own hash slot and
// cannot be touched from here — the caller finishes the promotion, and the
// reconciler repairs the window if it does not (see completePromotions).
//
// An emptied state is deleted here, at the same linearization point as the
// removal, so no admit can find a set that is about to disappear. Its entry in
// the namespace workflow index is removed by the caller instead — the index key
// cannot share this script's slot (see workflowAdmitLua), so the two cannot be
// joined atomically. The caller's removal is therefore best effort, and the
// sweep drops a tag whose set is gone: a stale tag costs one read, while a lost
// tag would hide a leaked slot.
var workflowReleaseLua = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
local wasQueued = redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
local promoted = {}
if removed == 1 then
    local limit = tonumber(redis.call('GET', KEYS[4]) or '0')
    while limit > 0 and redis.call('ZCARD', KEYS[1]) < limit do
        local head = redis.call('ZPOPMIN', KEYS[2])
        if #head == 0 then
            break
        end
        redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), head[1])
        redis.call('ZREM', KEYS[3], head[1])
        table.insert(promoted, head[1])
    end
end
local out = {0, wasQueued}
if redis.call('ZCARD', KEYS[1]) == 0 then
    redis.call('DEL', KEYS[1])
    if redis.call('ZCARD', KEYS[2]) == 0 then
        redis.call('DEL', KEYS[2])
        redis.call('DEL', KEYS[3])
        redis.call('DEL', KEYS[4])
        out[1] = 1
    end
else
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
end
for _, id in ipairs(promoted) do
    table.insert(out, id)
end
return out
`)

// promoteQueuedExecutionLua finishes one promotion: the execution leaves the
// queued status and the intents it parked become due.
//
// The status is what the CAS is on, and the two halves are in this one script
// because they belong together. An execution whose status still reads queued
// while its intents are due would start work holding no slot; one that reads
// running while its intents stay parked would never start at all. Both are
// reachable through a crash between the release script's promotion and this
// call, which is why the call is idempotent and the reconciler re-drives it.
//
// Losing the CAS is the third outcome, and it is where the dropped count comes
// from. The execution is no longer queued, so this promotion will never start
// it — but the release that promoted it had already taken it out of the line,
// which means a cancel racing this call releases a holder (not a waiter) and
// that release never drops the parked intents itself. A cancel that runs to its
// terminal write does clear them, through cleanupOnCancel; the window before
// that write lands does not, and neither does any status that leaves queued
// without that cleanup. Through that window the intents sit scored at a
// sentinel nothing reads, so this branch drops them at the instant the CAS
// finds the execution unstartable. The sentinel is what identifies them: a
// promotion that DID happen moved every held intent to a due score, so intents
// still all at the held score belong to an execution that never started,
// whatever it is now (cancelled, timed out, or a status write that failed).
// Doing it here rather than in the caller is what makes the decision atomic
// with the CAS that found the execution unstartable.
//
// KEYS[1] = execution status key, KEYS[2] = execution outbox ready ZSET,
// KEYS[3] = execution outbox body hash, KEYS[4] = execution updated_at key
// ARGV[1] = the queued status, ARGV[2] = the running status,
// ARGV[3] = status TTL seconds, ARGV[4] = held score (the parked sentinel),
// ARGV[5] = score to release the parked intents to (also the promotion instant)
// Returns {started, released, dropped}.
var promoteQueuedExecutionLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    local held = redis.call('ZCOUNT', KEYS[2], ARGV[4], ARGV[4])
    if held > 0 and held == redis.call('ZCARD', KEYS[2]) then
        redis.call('DEL', KEYS[2], KEYS[3])
        return {0, 0, 1}
    end
    return {0, 0, 0}
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
-- Leaving the queue is an accepted status transition, so it advances the
-- execution clock exactly like the pause/resume and terminal transitions do:
-- inspect reports updated_at as the latest accepted transition.
redis.call('SET', KEYS[4], ARGV[5], 'EX', tonumber(ARGV[3]))
local due = redis.call('ZRANGEBYSCORE', KEYS[2], ARGV[4], ARGV[4])
for _, member in ipairs(due) do
    redis.call('ZADD', KEYS[2], tonumber(ARGV[5]), member)
end
return {1, #due, 0}
`)

// workflowFillLua hands free capacity to the head of the line and reports what
// it promoted.
//
// It exists for the repairs no release performs: a sweep that released a leaked
// slot, or a limit that was raised while waiters were already queued. Without
// it, capacity would be free and the line would still not advance, because
// promotion happens on release and a sweep is not a release.
//
// KEYS[1] = workflow slot ZSET, KEYS[2] = queue ZSET,
// KEYS[3] = queue deadline ZSET, KEYS[4] = capacity string
// ARGV[1] = promotion score (unix seconds), ARGV[2] = TTL seconds
var workflowFillLua = redis.NewScript(`
local limit = tonumber(redis.call('GET', KEYS[4]) or '0')
local filled = {}
if limit > 0 then
    while redis.call('ZCARD', KEYS[1]) < limit do
        local head = redis.call('ZPOPMIN', KEYS[2])
        if #head == 0 then
            break
        end
        redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), head[1])
        redis.call('ZREM', KEYS[3], head[1])
        table.insert(filled, head[1])
    end
end
if #filled > 0 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return filled
`)

// workflowQueueExpireLua removes the executions whose queue time has run out,
// and reports them so the caller can drive each into the timeout terminal
// state.
//
// Removing them here — from the line and from the deadline index at once, in
// the workflow's own hash slot — is what stops the same execution being expired
// twice by two replicas sweeping concurrently. The terminal write that follows
// is a separate transition (the execution's keys are in another slot), so an
// expiry whose status write is lost is retried by the next sweep: the execution
// has left the line, and the reconcile pass finds it holding a slot while
// still reading queued.
//
// A deadline without a line entry is dropped rather than reported. The member
// was promoted, cancelled or expired by someone else, and the deadline is all
// that is left of it.
//
// KEYS[1] = queue ZSET, KEYS[2] = queue deadline ZSET
// ARGV[1] = now (unix milliseconds), ARGV[2] = batch limit
var workflowQueueExpireLua = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local expired = {}
for _, id in ipairs(due) do
    redis.call('ZREM', KEYS[2], id)
    if redis.call('ZSCORE', KEYS[1], id) then
        redis.call('ZREM', KEYS[1], id)
        table.insert(expired, id)
    end
end
return expired
`)

// ---------------------------------------------------------------------------
// Store implements engine.StateStore.
// ---------------------------------------------------------------------------
