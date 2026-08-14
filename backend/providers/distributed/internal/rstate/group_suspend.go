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

// Compile-time interface satisfaction checks.
var _ engine.GroupSuspender = (*Store)(nil)
var _ engine.GroupResumer = (*Store)(nil)
var _ engine.GroupSuspendReader = (*Store)(nil)
var _ engine.GroupCanceler = (*Store)(nil)

// ---------------------------------------------------------------------------
// Key helper
// ---------------------------------------------------------------------------

// groupSuspendKey stores the durable GroupSuspendState JSON for a suspended
// group unit. It is set by SuspendGroup and cleared on resume or cancel.
func groupSuspendKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return execKey(t, id, fmt.Sprintf("group:%d:suspend", unitIdx))
}

// ---------------------------------------------------------------------------
// Lua scripts
// ---------------------------------------------------------------------------

// suspendGroupLua atomically transitions a running group unit to suspended.
// KEYS: 1=group:status 2=group:meta 3=group:suspend
// ARGV: 1=expected_lease_token 2=ttl_s 3=suspend_state_json
//
// Returns 1=success (fenced transition applied), 0=fenced (token mismatch or
// wrong status).
var suspendGroupLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' then
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token') or ''
if token == '' or token ~= ARGV[1] then
    return 0
end
local ttl = tonumber(ARGV[2])
redis.call('SET', KEYS[1], 'suspended', 'EX', ttl)
redis.call('SET', KEYS[3], ARGV[3], 'EX', ttl)
redis.call('HDEL', KEYS[2], 'lease_token', 'lease_id')
redis.call('HSET', KEYS[2], 'committed_lease_token', ARGV[1])
redis.call('EXPIRE', KEYS[2], ttl)
return 1
`)

// resumeGroupLua delivers a signal to a suspended group unit. If quorum is
// satisfied, it transitions the unit to pending and writes an outbox entry for
// TaskTypeGroupResume dispatch. Otherwise it updates the suspend state JSON
// with the newly delivered signal.
//
// KEYS: 1=group:status 2=group:suspend 3=outbox:ready 4=outbox:body
// ARGV: 1=signal_name 2=signal_data_json 3=ttl_s 4=outbox_id 5=outbox_body_json
//
// Returns {1, 0} = resumed (quorum met), {0, pending} = partial delivery,
// {-1} = not suspended or signal not in WaitSignals.
var resumeGroupLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'suspended' then
    return {-1}
end
local raw = redis.call('GET', KEYS[2])
if not raw then
    return {-1}
end
local state = cjson.decode(raw)
-- Validate signal name is in WaitSignals.
local found = false
for _, s in ipairs(state.spec.wait_signals) do
    if s == ARGV[1] then
        found = true
        break
    end
end
if not found then
    return {-1}
end
-- Append to DeliveredSignals.
local signal = {name = ARGV[1]}
if ARGV[2] ~= '' and ARGV[2] ~= '{}' then
    signal.data = cjson.decode(ARGV[2])
end
if type(state.delivered_signals) ~= 'table' then
    state.delivered_signals = {}
end
table.insert(state.delivered_signals, signal)
-- Determine quorum.
local quorum = state.spec.quorum or 0
if quorum <= 0 then quorum = 1 end
local delivered = #state.delivered_signals
local ttl = tonumber(ARGV[3])
if delivered >= quorum then
    -- Quorum satisfied: transition to pending, delete suspend state, write outbox.
    redis.call('SET', KEYS[1], 'pending', 'EX', ttl)
    redis.call('DEL', KEYS[2])
    if redis.call('HSETNX', KEYS[4], ARGV[4], ARGV[5]) == 1 then
        redis.call('ZADD', KEYS[3], 0, ARGV[4])
    end
    redis.call('EXPIRE', KEYS[3], ttl)
    redis.call('EXPIRE', KEYS[4], ttl)
    return {1, 0}
end
-- Partial: update suspend state.
redis.call('SET', KEYS[2], cjson.encode(state), 'EX', ttl)
return {0, quorum - delivered}
`)

// cancelSuspendedGroupLua transitions a suspended group unit to done and
// decrements the execution remaining counter. If remaining reaches zero the
// execution is finalized as failed, recording ARGV[2] as the execution-level
// reason: cancellation terminalizes the group unit itself, so no member node
// ever commits a failure and this is the only carrier for the reason.
//
// KEYS: 1=group:status 2=group:suspend 3=remaining 4=failed 5=exec:status
//
//	6=exec:error
//
// ARGV: 1=ttl_s 2=exec_error
//
// Returns 1=success, 0=not suspended.
var cancelSuspendedGroupLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'suspended' then
    return 0
end
local ttl = tonumber(ARGV[1])
redis.call('SET', KEYS[1], 'done', 'EX', ttl)
redis.call('DEL', KEYS[2])
local remaining = redis.call('DECR', KEYS[3])
redis.call('EXPIRE', KEYS[3], ttl)
redis.call('INCR', KEYS[4])
redis.call('EXPIRE', KEYS[4], ttl)
if remaining <= 0 then
    local curExec = redis.call('GET', KEYS[5])
    if curExec ~= 'success' and curExec ~= 'failed' and curExec ~= 'canceled' and curExec ~= 'timeout' then
        redis.call('SET', KEYS[5], 'failed', 'EX', ttl)
        if ARGV[2] ~= '' then
            redis.call('SET', KEYS[6], ARGV[2], 'EX', ttl)
        end
    end
end
return 1
`)

// ---------------------------------------------------------------------------
// Go wrappers
// ---------------------------------------------------------------------------

// SuspendGroup atomically transitions a running group unit to suspended state.
func (s *Store) SuspendGroup(ctx context.Context, req engine.GroupSuspendRequest) (engine.GroupSuspendResult, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(req.ExecutionID)

	// Build the durable suspend state.
	state := engine.GroupSuspendState{
		Spec:           req.SuspendSpec,
		SignalJournal:  req.SignalJournal,
		EntryInput:     req.EntryInput,
		IdempotencyKey: req.IdempotencyKey,
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return engine.GroupSuspendResult{}, fmt.Errorf("marshal group suspend state %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
	}

	res, err := suspendGroupLua.Run(ctx, s.rdb, []string{
		groupUnitStatusKey(t, req.ExecutionID, req.GroupUnitIdx),
		groupUnitMetaKey(t, req.ExecutionID, req.GroupUnitIdx),
		groupSuspendKey(t, req.ExecutionID, req.GroupUnitIdx),
	}, string(req.LeaseToken), int(ttl.Seconds()), string(stateJSON)).Int64()
	if err != nil {
		return engine.GroupSuspendResult{}, fmt.Errorf("suspend group %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
	}
	if res == 0 {
		return engine.GroupSuspendResult{}, nil
	}
	return engine.GroupSuspendResult{Committed: true}, nil
}

// ResumeGroup delivers a signal to a suspended group unit and, if quorum is
// satisfied, produces a TaskTypeGroupResume outbox entry for re-dispatch.
func (s *Store) ResumeGroup(ctx context.Context, req engine.GroupResumeRequest) (engine.GroupResumeResult, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(req.ExecutionID)

	// Build signal data JSON.
	signalDataJSON := "{}"
	if req.SignalData != nil {
		encoded, err := json.Marshal(req.SignalData)
		if err != nil {
			return engine.GroupResumeResult{}, fmt.Errorf("marshal signal data for resume %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
		}
		signalDataJSON = string(encoded)
	}

	// Build outbox entry.
	outboxID := fmt.Sprintf("group-resume/%s/%d", req.ExecutionID, req.GroupUnitIdx)
	outboxBody, err := marshalRedisOutboxEntry(outboxID, engine.Task{
		ExecutionID: req.ExecutionID,
		UnitIdx:     req.GroupUnitIdx,
		Type:        engine.TaskTypeGroupResume,
	}, time.Time{})
	if err != nil {
		return engine.GroupResumeResult{}, fmt.Errorf("marshal outbox for resume %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
	}

	res, err := resumeGroupLua.Run(ctx, s.rdb, []string{
		groupUnitStatusKey(t, req.ExecutionID, req.GroupUnitIdx),
		groupSuspendKey(t, req.ExecutionID, req.GroupUnitIdx),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
	}, req.SignalName, signalDataJSON, int(ttl.Seconds()), outboxID, outboxBody).Slice()
	if err != nil {
		return engine.GroupResumeResult{}, fmt.Errorf("resume group %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
	}
	if len(res) == 0 {
		return engine.GroupResumeResult{}, nil
	}
	code := redisResultInt(res[0])
	switch code {
	case -1:
		// Not suspended or signal not accepted.
		return engine.GroupResumeResult{}, nil
	case 1:
		return engine.GroupResumeResult{Resumed: true}, nil
	case 0:
		pending := 0
		if len(res) >= 2 {
			pending = int(redisResultInt(res[1]))
		}
		return engine.GroupResumeResult{Resumed: false, Pending: pending}, nil
	default:
		return engine.GroupResumeResult{}, fmt.Errorf("resume group %q/#%d: unexpected code %d", req.ExecutionID, req.GroupUnitIdx, code)
	}
}

// GetGroupSuspendState reads the current suspend state of a group unit.
func (s *Store) GetGroupSuspendState(ctx context.Context, execID types.ExecutionID, unitIdx int) (*engine.GroupSuspendState, error) {
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, groupSuspendKey(t, execID, unitIdx)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get group suspend state %q/#%d: %w", execID, unitIdx, err)
	}
	var state engine.GroupSuspendState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, fmt.Errorf("unmarshal group suspend state %q/#%d: %w", execID, unitIdx, err)
	}
	return &state, nil
}

// CancelSuspendedGroup transitions a suspended group unit to done and cleans
// up suspend state. It decrements the execution's remaining counter which may
// finalize the execution as failed, in which case the Lua records
// engine.CanceledSuspendedGroupError as the execution-level reason so the
// readback matches the local backend.
func (s *Store) CancelSuspendedGroup(ctx context.Context, execID types.ExecutionID, unitIdx int) error {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(execID)

	res, err := cancelSuspendedGroupLua.Run(ctx, s.rdb, []string{
		groupUnitStatusKey(t, execID, unitIdx),
		groupSuspendKey(t, execID, unitIdx),
		remainingNodesKey(t, execID),
		failedNodesKey(t, execID),
		execKey(t, execID, "status"),
		execKey(t, execID, "error"),
	}, int(ttl.Seconds()), engine.CanceledSuspendedGroupError).Int64()
	if err != nil {
		return fmt.Errorf("cancel suspended group %q/#%d: %w", execID, unitIdx, err)
	}
	if res == 1 {
		s.evictExecutionCaches(execID)
	}
	return nil
}
