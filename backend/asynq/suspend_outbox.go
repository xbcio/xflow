package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.DurableLeaseSuspender = (*redisState)(nil)

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
local scheduleSignal = function(name, raw, all)
    local entry = cjson.decode(ARGV[15])
    local payload = {Triggered = 0, Name = name, Data = cjson.decode(raw)}
    if all then
        payload.All = all
    end
    entry.task.payload = payload
    addOutbox(ARGV[14], cjson.encode(entry), tonumber(ARGV[16]))
end
if multi then
    local values = redis.call('HGETALL', KEYS[9])
    if #values / 2 >= quorum then
        local all = {}
        for i = 1, #values, 2 do
            all[values[i]] = cjson.decode(values[i + 1])
        end
        clearWaiters()
        redis.call('DEL', KEYS[8], KEYS[9])
        scheduleSignal(selectedName, selectedPayload, all)
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
func (s *redisState) SuspendTaskLeaseWithOutbox(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (bool, error) {
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
		outboxReadyKey(lease.Task.ExecutionID),
		outboxBodyKey(lease.Task.ExecutionID),
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
	s.extendExecTTL(ctx, lease.Task.ExecutionID, lease.Task.NodeName, s.suspendTTL(lease.Task.ExecutionID, spec))
	return true, nil
}
