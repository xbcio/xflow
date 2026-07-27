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

var _ engine.DurableLeaseSuspender = (*Store)(nil)

// suspendTaskLeaseWithOutboxLua makes the suspend transition and every
// immediately-known continuation intent visible together. The script consumes
// a pre-delivered signal only after it has recorded that resume task in the
// execution outbox; timer and timeout tasks are likewise recorded before the
// parent lease is cleared.
//
// KEYS: status, meta, output, lease index, suspended set, resume lock,
// old waiter, waiter spec, signal batch, outbox ready, outbox body, then
// signal/waiter key pairs. All keys share the execution hash tag.
var suspendTaskLeaseWithOutboxLua = redis.NewScript(`
local terminal = function(value)
    return value == 'success' or value == 'failed' or value == 'skipped' or value == 'canceled' or value == 'continued'
end
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
if tonumber(ARGV[7]) == 1 then
    redis.call('SET', KEYS[3], ARGV[8], 'EX', ttl)
end
redis.call('SET', KEYS[1], 'suspended', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_id', '', 'lease_token', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0', 'lease_deadline_ms', '0', 'lease_task_type', '0', 'lease_payload', '')
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('ZREM', KEYS[4], ARGV[6])
redis.call('DEL', KEYS[6], KEYS[7], KEYS[8], KEYS[9])

local addOutbox = function(id, body, availableAt)
    if id ~= '' and redis.call('HSETNX', KEYS[11], id, body) == 1 then
        redis.call('ZADD', KEYS[10], availableAt, id)
        redis.call('EXPIRE', KEYS[10], ttl)
        redis.call('EXPIRE', KEYS[11], ttl)
    end
end

local multi = tonumber(ARGV[10]) == 1
local quorum = tonumber(ARGV[11])
local count = tonumber(ARGV[12])
local delayedCount = tonumber(ARGV[17])
local namesStart = 18 + delayedCount * 3
local selectedName = ''
local selectedPayload = ''
for i = 1, count do
    local signalKey = KEYS[11 + i]
    local waiterKey = KEYS[11 + count + i]
    local signalName = ARGV[namesStart + i - 1]
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
        redis.call('DEL', KEYS[11 + count + i])
    end
    redis.call('SREM', KEYS[5], ARGV[9])
end
-- scheduleSignal builds the resume outbox entry for a pre-delivered signal.
-- It assembles the SignalPayload JSON from raw stored pieces WITHOUT decoding
-- user signal data through cjson, preserving exact fidelity (avoids empty-object
-- {} -> [] mutation and int64>2^53 precision loss). raw is the stored signal
-- payload JSON; allJSON (multi-signal) is a pre-built raw JSON object or nil.
-- Field names match Go's types.SignalPayload (Triggered/Name/Data/All).
local scheduleSignal = function(name, raw, allJSON)
    local payloadJSON = '{"Triggered":0,"Name":' .. cjson.encode(name) .. ',"Data":' .. raw
    if allJSON then
        payloadJSON = payloadJSON .. ',"All":' .. allJSON
    end
    payloadJSON = payloadJSON .. '}'
    -- Splice "payload":<payloadJSON> into the task object of the entry template.
    -- SuspendResumeOutboxEntry marshals with a nil payload, so omitempty drops the
    -- "payload" key entirely; we insert it right after the task object's brace:
    -- {"id":...,"task":{"payload":{...},"execution_id":...}}
    local entryBody = ARGV[15]
    local finalBody
    local taskPos = entryBody:find('"task":')
    if taskPos then
        local bracePos = entryBody:find('{', taskPos + 7)
        if bracePos then
            finalBody = entryBody:sub(1, bracePos) .. '"payload":' .. payloadJSON .. ',' .. entryBody:sub(bracePos + 1)
        end
    end
    if not finalBody then
        -- Fallback for unexpected structure: cjson round-trip (correctness over fidelity).
        local entry = cjson.decode(entryBody)
        local payload = {Triggered = 0, Name = name, Data = cjson.decode(raw)}
        if allJSON then
            payload.All = cjson.decode(allJSON)
        end
        entry.task.payload = payload
        finalBody = cjson.encode(entry)
    end
    addOutbox(ARGV[14], finalBody, tonumber(ARGV[16]))
end
if multi then
    local values = redis.call('HGETALL', KEYS[9])
    if #values / 2 >= quorum then
        -- Build the All map as raw JSON WITHOUT decoding individual signal
        -- payloads through cjson, preserving exact fidelity (see scheduleSignal).
        local parts = {}
        for i = 1, #values, 2 do
            table.insert(parts, cjson.encode(values[i]) .. ':' .. values[i + 1])
        end
        local allJSON = '{' .. table.concat(parts, ',') .. '}'
        clearWaiters()
        redis.call('DEL', KEYS[8], KEYS[9])
        scheduleSignal(selectedName, selectedPayload, allJSON)
        return 1
    end
    redis.call('SET', KEYS[8], ARGV[13], 'EX', ttl)
    redis.call('EXPIRE', KEYS[9], ttl)
elseif selectedName ~= '' then
    clearWaiters()
    scheduleSignal(selectedName, selectedPayload, nil)
    return 1
end
for i = 1, count do
    local signalName = ARGV[namesStart + i - 1]
    redis.call('SET', KEYS[11 + count + i], ARGV[9], 'EX', ttl)
end
redis.call('SADD', KEYS[5], ARGV[9])
redis.call('EXPIRE', KEYS[5], ttl)
for i = 1, delayedCount do
    local position = 18 + (i - 1) * 3
    addOutbox(ARGV[position], ARGV[position + 1], tonumber(ARGV[position + 2]))
end
return 1
`)

// SuspendTaskLeaseWithOutbox atomically parks one claimed lease and persists
// the resume delivery required by an already-present signal, timer, or timeout.
func (s *Store) SuspendTaskLeaseWithOutbox(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (bool, error) {
	if lease == nil || spec == nil {
		return false, engine.ErrInvalidLeaseToken
	}
	outputJSON := ""
	if storeOutput {
		encoded, err := json.Marshal(output)
		if err != nil {
			return false, fmt.Errorf("marshal suspend output %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		outputJSON = string(encoded)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return false, fmt.Errorf("marshal suspend spec %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	now := time.Now().UTC()
	signalEntry := engine.SuspendResumeOutboxEntry(lease, "signal", nil, now)
	signalBody, err := marshalRedisOutboxEntry(signalEntry.ID, signalEntry.Task, signalEntry.AvailableAt)
	if err != nil {
		return false, fmt.Errorf("marshal suspend signal outbox %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	delayedEntries := engine.SuspendOutboxEntries(lease, spec, nil, now)

	multi := 0
	if spec.Mode == types.ModeMultiSignal {
		multi = 1
	}
	oldWaiter := oldSignalName
	if oldWaiter == "" {
		oldWaiter = "__none__"
	}
	t := namespace.FromContext(ctx)
	keys := []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		outputKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
		suspendedNodesKey(t, lease.Task.ExecutionID),
		resumeLockKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		waiterKey(t, lease.Task.ExecutionID, oldWaiter),
		waiterSpecKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		signalBatchKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		outboxReadyKey(t, lease.Task.ExecutionID),
		outboxBodyKey(t, lease.Task.ExecutionID),
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, signalKey(t, lease.Task.ExecutionID, signalName))
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, waiterKey(t, lease.Task.ExecutionID, signalName))
	}
	store := 0
	if storeOutput {
		store = 1
	}
	args := []any{
		string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID,
		int(s.getExecTTL(lease.Task.ExecutionID).Seconds()), leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName),
		store, outputJSON, lease.Task.NodeName, multi, signalQuorum(spec), len(spec.Signals), string(specJSON),
		signalEntry.ID, signalBody, now.UnixMilli(), len(delayedEntries),
	}
	for _, entry := range delayedEntries {
		body, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return false, fmt.Errorf("marshal suspend outbox %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		availableAt := now.UnixMilli()
		if !entry.AvailableAt.IsZero() {
			availableAt = entry.AvailableAt.UTC().UnixMilli()
		}
		args = append(args, entry.ID, body, availableAt)
	}
	for _, signalName := range spec.Signals {
		args = append(args, signalName)
	}
	result, err := suspendTaskLeaseWithOutboxLua.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("suspend task lease with outbox %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID, keys...); err != nil {
		return false, err
	}
	if err := s.extendExecTTL(ctx, lease.Task.ExecutionID, lease.Task.NodeName, spec, s.suspendTTL(lease.Task.ExecutionID, spec)); err != nil {
		return false, err
	}
	return true, nil
}
