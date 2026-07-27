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

// Compile-time interface satisfaction check.
var _ engine.GroupStateStore = (*Store)(nil)

// ---------------------------------------------------------------------------
// Lua scripts for group unit lifecycle.
// All KEYS share the same {id} hash tag (execution ID) for Redis Cluster
// safety — this guarantees co-location on one slot and prevents CROSSSLOT.
// ---------------------------------------------------------------------------

// acquireGroupLeaseLua atomically pins a group unit to a runner. It mirrors
// acquireTaskLeaseLua but keys on the group unit's status/meta. Returns
// {1} on acquire, {0} when the unit is already running/done or fenced.
// KEYS: 1=group:status 2=group:meta 3=leases(zset)
// ARGV: 1=lease_id 2=lease_token 3=attempt 4=ttl_s 5=deadline_ms 6=lease_member
var acquireGroupLeaseLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status == 'running' or status == 'done' then
    return {0}
end
local ttl = tonumber(ARGV[4])
redis.call('SET', KEYS[1], 'running', 'EX', ttl)
redis.call('HSET', KEYS[2], 'lease_id', ARGV[1], 'lease_token', ARGV[2], 'attempt', ARGV[3])
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('ZADD', KEYS[3], tonumber(ARGV[5]), ARGV[6])
redis.call('EXPIRE', KEYS[3], ttl)
return {1}
`)

// renewGroupLeaseLua extends the deadline only when token still matches.
// KEYS: 1=group:meta 2=leases(zset)  ARGV: 1=token 2=deadline_ms 3=ttl_s 4=member
var renewGroupLeaseLua = redis.NewScript(`
local token = redis.call('HGET', KEYS[1], 'lease_token') or ''
if token == '' or token ~= ARGV[1] then
    return {0}
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[4])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return {1}
`)

// commitGroupLua terminalizes exactly ONE unit and decrements the unit-based
// remaining counter — the group analogue of commitNodeLua. Boundary outputs
// are written by THIS script, after the fence check and before any other
// state mutation (F2 fix): a prior version wrote them via a separate,
// unchecked s.rdb.Set call before this script ran at all, so a stale/failed
// commit attempt could still clobber output, and a crash between that write
// and the fenced transition could leave a half-committed state. Output writes
// now share the exact same fence (token/attempt/status/execution-active) as
// the terminal transition and downstream fan-in, in one atomic step.
//
// KEYS: 1=exec:status 2=exec:error 3=remaining 4=failed 5=group:status
//
//	6=group:meta 7=leases(zset) 8=outbox:ready 9=outbox:body
//	10.. = per exit: output key (1 key each), followed by
//	       per downstream unit: inDegree, active, schedule (3 keys each)
//
// Redis Cluster: all keys share the same {id} hash tag (execution ID) so they
// map to one slot. The namespace prefix is brace-less and does not participate
// in slot computation (see keys.go header comment). If any downstream key were
// ever to lose its {id} tag, Redis would return CROSSSLOT rather than silently
// mis-computing.
//
// ARGV: 1=lease_token 2=attempt 3=ttl_s 4=lease_member 5=outcome(success|failed)
//
//	6=fatal(0|1) 7=allowCycles(0|1) 8=error 9=exitCount 10=downstreamCount
//	11.. = per exit: encoded output data (1 arg each), followed by
//	       per unit: arrivalCount, activeCount, mergeMode,
//	       executeID, executeBody, skipID, skipBody (7 args each)
//
// Returns {code, done, finalStatus}: code 0=stale 1=accepted 2=duplicate_terminal
//
//	3=execution_inactive
var commitGroupLua = redis.NewScript(`
local status = redis.call('GET', KEYS[5])
if status == 'done' then
    local committed = redis.call('HGET', KEYS[6], 'committed_lease_token') or ''
    if committed ~= '' and committed == ARGV[1] then
        return {2, 0, ''}
    end
    return {0, 0, ''}
end
local execStatus = redis.call('GET', KEYS[1])
if execStatus == 'success' or execStatus == 'failed' or execStatus == 'canceled' or execStatus == 'timeout' then
    return {3, 0, execStatus}
end
if status ~= 'running' then
    return {0, 0, ''}
end
local token = redis.call('HGET', KEYS[6], 'lease_token') or ''
local attempt = tonumber(redis.call('HGET', KEYS[6], 'attempt') or '0')
if token == '' or token ~= ARGV[1] or attempt ~= tonumber(ARGV[2]) then
    return {0, 0, ''}
end
local ttl = tonumber(ARGV[3])
-- F2: boundary outputs are written here, strictly after the fence check
-- above and before any other mutation, so a stale/duplicate/failed commit
-- attempt can never reach this point and cannot alter output.
local exitCount = tonumber(ARGV[9] or '0')
local keypos = 10
for i = 1, exitCount do
    redis.call('SET', KEYS[keypos], ARGV[10 + i], 'EX', ttl)
    keypos = keypos + 1
end
redis.call('SET', KEYS[5], 'done', 'EX', ttl)
redis.call('HSET', KEYS[6], 'lease_id', '', 'lease_token', '', 'committed_lease_token', ARGV[1])
redis.call('EXPIRE', KEYS[6], ttl)
redis.call('ZREM', KEYS[7], ARGV[4])
local done = 0
local finalStatus = ''
if tonumber(ARGV[7]) == 0 then
    local remaining = redis.call('DECR', KEYS[3])
    redis.call('EXPIRE', KEYS[3], ttl)
    if ARGV[5] == 'failed' then
        redis.call('INCR', KEYS[4]); redis.call('EXPIRE', KEYS[4], ttl)
    end
    if tonumber(ARGV[6]) == 1 or remaining <= 0 then
        local curExec = redis.call('GET', KEYS[1])
        if curExec == 'canceling' or curExec == 'canceled' or curExec == 'timeout' or curExec == 'success' or curExec == 'failed' then
            done = 0
        else
            finalStatus = 'success'
            if tonumber(ARGV[6]) == 1 or tonumber(redis.call('GET', KEYS[4]) or '0') > 0 then
                finalStatus = 'failed'
            end
            redis.call('SET', KEYS[1], finalStatus, 'EX', ttl)
            if finalStatus == 'failed' and ARGV[8] ~= '' then
                redis.call('SET', KEYS[2], ARGV[8], 'EX', ttl)
            end
            done = 1
        end
    end
end
if done == 0 and tonumber(ARGV[6]) == 0 and redis.call('GET', KEYS[1]) ~= 'canceling' then
    local n = tonumber(ARGV[10] or '0')
    local argpos = 10 + exitCount + 1
    for i = 1, n do
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
        redis.call('EXPIRE', inDegreeKey, ttl)
        redis.call('EXPIRE', activeKey, ttl)
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
                redis.call('EXPIRE', scheduleKey, ttl)
                local outboxID = executeID
                local outboxBody = executeBody
                if nextAction == 'skip' then
                    outboxID = skipID
                    outboxBody = skipBody
                end
                if redis.call('HSETNX', KEYS[9], outboxID, outboxBody) == 1 then
                    redis.call('ZADD', KEYS[8], tonumber(ARGV[2]), outboxID)
                end
            end
        end
        keypos = keypos + 3
        argpos = argpos + 7
    end
    redis.call('EXPIRE', KEYS[8], ttl); redis.call('EXPIRE', KEYS[9], ttl)
end
return {1, done, finalStatus}
`)

// ---------------------------------------------------------------------------
// Go wrappers implementing engine.GroupStateStore on *Store.
// ---------------------------------------------------------------------------

func (s *Store) AcquireGroupLease(ctx context.Context, lease *engine.GroupLease) (bool, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(lease.ExecutionID)
	// deadline derived from IssuedAt+TTL, consistent with TaskLease.
	deadlineMs := lease.IssuedAt.Add(lease.TTL).UnixMilli()
	res, err := acquireGroupLeaseLua.Run(ctx, s.rdb, []string{
		groupUnitStatusKey(t, lease.ExecutionID, lease.GroupUnitIdx),
		groupUnitMetaKey(t, lease.ExecutionID, lease.GroupUnitIdx),
		leaseExpiryZSetKey(t, lease.ExecutionID),
	}, string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt,
		int(ttl.Seconds()), deadlineMs,
		groupLeaseMember(lease.ExecutionID, lease.GroupUnitIdx)).Slice()
	if err != nil {
		return false, fmt.Errorf("acquire group lease %q/#%d: %w", lease.ExecutionID, lease.GroupUnitIdx, err)
	}
	return len(res) == 1 && redisResultInt(res[0]) == 1, nil
}

func (s *Store) RenewGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int, token engine.LeaseToken, deadline time.Time) (bool, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(id)
	res, err := renewGroupLeaseLua.Run(ctx, s.rdb, []string{
		groupUnitMetaKey(t, id, unitIdx),
		leaseExpiryZSetKey(t, id),
	}, string(token), deadline.UnixMilli(), int(ttl.Seconds()), groupLeaseMember(id, unitIdx)).Slice()
	if err != nil {
		return false, fmt.Errorf("renew group lease %q/#%d: %w", id, unitIdx, err)
	}
	return len(res) == 1 && redisResultInt(res[0]) == 1, nil
}

func (s *Store) CommitGroup(ctx context.Context, req engine.GroupCommitRequest) (engine.GroupCommitResult, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(req.ExecutionID)
	allowCycles := 0 // Milestone A: group is never cyclic (AllowCycles and group are mutually exclusive)
	fatal := 0
	if req.Fatal {
		fatal = 1
	}
	keys := []string{
		execKey(t, req.ExecutionID, "status"),
		execKey(t, req.ExecutionID, "error"),
		remainingNodesKey(t, req.ExecutionID),
		failedNodesKey(t, req.ExecutionID),
		groupUnitStatusKey(t, req.ExecutionID, req.GroupUnitIdx),
		groupUnitMetaKey(t, req.ExecutionID, req.GroupUnitIdx),
		leaseExpiryZSetKey(t, req.ExecutionID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
	}
	args := []any{
		string(req.LeaseToken), req.Attempt, int(ttl.Seconds()),
		groupLeaseMember(req.ExecutionID, req.GroupUnitIdx),
		string(req.Outcome), fatal, allowCycles, req.Error, len(req.Exits), len(req.Downstream),
	}
	// F2: boundary outputs are passed as KEYS/ARGV to commitGroupLua so they
	// are written under the exact same fence as the terminal transition — no
	// separate, unchecked Set call before the script runs. Encoding errors are
	// caught here (fail before any Redis command runs); Redis command errors
	// are surfaced by the single Run() call below.
	exitArgs := make([]any, 0, len(req.Exits))
	for _, ex := range req.Exits {
		encoded, err := json.Marshal(ex.Data)
		if err != nil {
			return engine.GroupCommitResult{}, fmt.Errorf("marshal group exit %q/%q: %w", req.ExecutionID, ex.NodeName, err)
		}
		keys = append(keys, outputKey(t, req.ExecutionID, ex.NodeName))
		exitArgs = append(exitArgs, string(encoded))
	}
	args = append(args, exitArgs...)
	// Downstream unit arrivals: each arrival appends 3 counting keys + 7 args
	// (execute/skip body), structurally identical to AdvanceNode wrapper
	// (state_commit.go:258-289) but keyed by UnitIdx.
	outboxIDs := make([]string, 0, len(req.Downstream)*2)
	for _, arrival := range req.Downstream {
		keys = append(keys,
			inDegreeKey(t, req.ExecutionID, arrival.UnitIdx),
			activeInputsKey(t, req.ExecutionID, arrival.UnitIdx),
			scheduleKey(t, req.ExecutionID, arrival.UnitIdx),
		)
		execType := arrival.ExecTaskType
		if execType == 0 {
			execType = engine.TaskTypeNodeExec
		}
		executeID := redisExecuteOutboxID(req.ExecutionID, arrival.NodeName, 0)
		skipID := redisSkipOutboxID(req.ExecutionID, arrival.NodeName, 0)
		executeJSON, err := marshalRedisOutboxEntry(executeID, engine.Task{
			ExecutionID: req.ExecutionID,
			NodeName:    arrival.NodeName,
			NodeIdx:     arrival.NodeIdx,
			UnitIdx:     arrival.UnitIdx,
			Type:        execType,
		}, time.Time{})
		if err != nil {
			return engine.GroupCommitResult{}, err
		}
		skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
			ExecutionID: req.ExecutionID,
			NodeName:    arrival.NodeName,
			NodeIdx:     arrival.NodeIdx,
			UnitIdx:     arrival.UnitIdx,
			Type:        engine.TaskTypeNodeSkip,
		}, time.Time{})
		if err != nil {
			return engine.GroupCommitResult{}, err
		}
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON)
		outboxIDs = append(outboxIDs, executeID, skipID)
	}
	res, err := commitGroupLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.GroupCommitResult{}, fmt.Errorf("commit group %q/#%d: %w", req.ExecutionID, req.GroupUnitIdx, err)
	}
	if len(res) != 3 {
		return engine.GroupCommitResult{}, fmt.Errorf("commit group %q/#%d: unexpected result %v", req.ExecutionID, req.GroupUnitIdx, res)
	}
	out := engine.GroupCommitResult{}
	switch code := redisResultInt(res[0]); code {
	case 0:
		out.Outcome = engine.CommitOutcomeStaleToken
	case 1:
		out.Applied = true
		out.Outcome = engine.CommitOutcomeAccepted
		out.ExecutionDone = redisResultInt(res[1]) == 1
		out.ExecutionStatus = types.ExecutionStatus(redisResultString(res[2]))
		if !out.ExecutionDone && !req.Fatal {
			out.OutboxIDs = outboxIDs
		}
	case 2:
		out.Outcome = engine.CommitOutcomeDuplicateTerminal
	case 3:
		out.Outcome = engine.CommitOutcomeExecutionInactive
	default:
		return engine.GroupCommitResult{}, fmt.Errorf("commit group %q/#%d: unknown outcome %d", req.ExecutionID, req.GroupUnitIdx, code)
	}
	if out.ExecutionDone {
		s.evictExecutionCaches(req.ExecutionID)
	}
	return out, nil
}
