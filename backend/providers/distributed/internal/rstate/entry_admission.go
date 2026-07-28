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

// Compile-time interface satisfaction.
var _ engine.EntryAdmissionStore = (*Store)(nil)

// seedExecutionFromEntryLua atomically admits an entry-unit (single node or
// group node) result. It performs all steps in one transition:
//  1. check admission key occupancy (first-writer-wins)
//  2. create execution (status, graph, remaining, failed, in-degree)
//  3. mark entry unit as done
//  4. write boundary outputs
//  5. apply downstream fan-in and write outbox intents
//  6. check completion (remaining=0 → finalize)
//
// All KEYS share the same {execID} hash tag because the execution ID is
// deterministic from the admission key (DeterministicExecutionID).
//
// KEYS: 1=admission 2=exec:status 3=exec:graph 4=remaining 5=failed
//
//	6=group:status 7=group:meta 8=outbox:ready 9=outbox:body
//	10..10+exitCount-1 = output keys
//	10+exitCount..end = per downstream: inDegree, active, schedule (3 each)
//
// ARGV: 1=resultHash 2=ttl_s 3=graphJSON 4=outcome(success|failed)
//
//	5=exitCount 6=downstreamCount
//	7..7+exitCount-1 = encoded exit data
//	7+exitCount..end = per downstream: arrivalCount, activeCount, mergeMode,
//	                   executeID, executeBody, skipID, skipBody (7 each)
//
// Returns {code, finalStatus}: code 1=accepted, 2=duplicate, 3=conflict
var seedExecutionFromEntryLua = redis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing and existing ~= '' then
    if existing == ARGV[1] then
        return {2, ''}
    end
    return {3, ''}
end
local ttl = tonumber(ARGV[2])
-- Step 1: Write admission key (stores the result hash).
redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
-- Step 2: Create execution.
redis.call('SET', KEYS[2], 'running', 'EX', ttl)
redis.call('SET', KEYS[3], ARGV[3], 'EX', ttl)
-- Step 3: remaining/failed counters are pre-seeded via TxPipeline (see Go wrapper).
-- Step 4: Mark group unit as done.
redis.call('SET', KEYS[6], 'done', 'EX', ttl)
redis.call('HSET', KEYS[7], 'committed_lease_token', 'seed-triggered')
redis.call('EXPIRE', KEYS[7], ttl)
-- Step 5: Boundary outputs.
local exitCount = tonumber(ARGV[5] or '0')
local keypos = 10
for i = 1, exitCount do
    redis.call('SET', KEYS[keypos], ARGV[6 + i], 'EX', ttl)
    keypos = keypos + 1
end
-- Step 6: Decrement remaining (group unit is done).
local remaining = redis.call('DECR', KEYS[4])
redis.call('EXPIRE', KEYS[4], ttl)
if ARGV[4] == 'failed' then
    redis.call('INCR', KEYS[5]); redis.call('EXPIRE', KEYS[5], ttl)
end
local finalStatus = ''
if remaining <= 0 then
    finalStatus = 'success'
    if tonumber(redis.call('GET', KEYS[5]) or '0') > 0 then
        finalStatus = 'failed'
    end
    redis.call('SET', KEYS[2], finalStatus, 'EX', ttl)
end
-- Step 7: Downstream fan-in (same logic as commitGroupLua).
if remaining > 0 then
    local n = tonumber(ARGV[6] or '0')
    local argpos = 6 + exitCount + 1
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
        local rem = redis.call('DECRBY', inDegreeKey, arrivals)
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
            elseif rem <= 0 then
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
                    redis.call('ZADD', KEYS[8], 0, outboxID)
                end
            end
        end
        keypos = keypos + 3
        argpos = argpos + 7
    end
    redis.call('EXPIRE', KEYS[8], ttl); redis.call('EXPIRE', KEYS[9], ttl)
end
return {1, finalStatus}
`)

// SeedExecutionFromEntry implements engine.EntryAdmissionStore using a
// two-phase approach: a TxPipeline seeds the structural keys (remaining,
// failed, in-degree) and the Lua script atomically occupies the admission key,
// creates the execution status, marks the entry unit done, writes outputs,
// decrements remaining, and applies downstream fan-in. Both phases are
// deterministic and idempotent — the Lua short-circuits on existing admission key.
func (s *Store) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	t := req.Namespace
	if t == "" {
		t = namespace.FromContext(ctx)
	}
	ttl := s.execTTL

	// Phase 1: Pre-seed structural keys (idempotent SET NX patterns won't
	// overwrite). This uses a pipeline for the keys the Lua script reads but
	// does not create itself (remaining, failed, in-degree).
	if req.Graph != nil && !req.Graph.AllowCycles() {
		pipe := s.rdb.TxPipeline()
		pipe.SetNX(ctx, remainingNodesKey(t, execID), req.Graph.UnitCount(), ttl)
		pipe.SetNX(ctx, failedNodesKey(t, execID), 0, ttl)
		for i := 0; i < req.Graph.UnitCount(); i++ {
			d := req.Graph.UnitInDegreeAt(i)
			if d > 0 {
				pipe.SetNX(ctx, inDegreeKey(t, execID, i), d, ttl)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed counters for %q: %w", execID, err)
		}
	}

	// Serialize graph.
	graphJSON, err := json.Marshal(req.Graph)
	if err != nil {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("marshal graph for admission %q: %w", req.AdmissionKey, err)
	}

	// Build KEYS.
	keys := []string{
		admissionKey(t, execID),                              // 1
		execKey(t, execID, "status"),                         // 2
		execKey(t, execID, "graph"),                          // 3
		remainingNodesKey(t, execID),                         // 4
		failedNodesKey(t, execID),                            // 5
		groupUnitStatusKey(t, execID, req.EntryUnitIdx),      // 6
		groupUnitMetaKey(t, execID, req.EntryUnitIdx),        // 7
		outboxReadyKey(t, execID),                            // 8
		outboxBodyKey(t, execID),                             // 9
	}

	// Build ARGV.
	args := []any{
		string(req.ResultHash),  // 1
		int(ttl.Seconds()),      // 2
		string(graphJSON),       // 3
		string(req.Outcome),     // 4
		len(req.Exits),          // 5
		len(req.Downstream),     // 6
	}

	// Exit outputs (keys + args).
	for _, ex := range req.Exits {
		encoded, err := json.Marshal(ex.Data)
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("marshal exit %q: %w", ex.NodeName, err)
		}
		keys = append(keys, outputKey(t, execID, ex.NodeName))
		args = append(args, string(encoded))
	}

	// Downstream arrivals (keys + args).
	for _, arrival := range req.Downstream {
		keys = append(keys,
			inDegreeKey(t, execID, arrival.UnitIdx),
			activeInputsKey(t, execID, arrival.UnitIdx),
			scheduleKey(t, execID, arrival.UnitIdx),
		)
		execType := arrival.ExecTaskType
		if execType == 0 {
			execType = engine.TaskTypeNodeExec
		}
		executeID := redisExecuteOutboxID(execID, arrival.NodeName, 0)
		skipID := redisSkipOutboxID(execID, arrival.NodeName, 0)
		executeJSON, err := marshalRedisOutboxEntry(executeID, engine.Task{
			ExecutionID: execID,
			NodeName:    arrival.NodeName,
			NodeIdx:     arrival.NodeIdx,
			UnitIdx:     arrival.UnitIdx,
			Type:        execType,
		}, time.Time{})
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, err
		}
		skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
			ExecutionID: execID,
			NodeName:    arrival.NodeName,
			NodeIdx:     arrival.NodeIdx,
			UnitIdx:     arrival.UnitIdx,
			Type:        engine.TaskTypeNodeSkip,
		}, time.Time{})
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, err
		}
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON)
	}

	// Run Lua.
	res, err := seedExecutionFromEntryLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed triggered group %q: %w", req.AdmissionKey, err)
	}
	if len(res) < 1 {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed triggered group %q: empty response", req.AdmissionKey)
	}

	code := redisResultInt(res[0])
	switch code {
	case 1: // accepted
		return engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateAccepted,
			ExecutionID: execID,
			Duplicate:   false,
		}, nil
	case 2: // duplicate (same hash)
		return engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateAccepted,
			ExecutionID: execID,
			Duplicate:   true,
		}, nil
	case 3: // conflict (different hash)
		return engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateConflict,
			ExecutionID: execID,
			Duplicate:   false,
		}, nil
	default:
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed triggered group %q: unknown code %d", req.AdmissionKey, code)
	}
}

// admissionKey stores the result hash for a given deterministic execution ID.
// The key lives in the same {execID} hash slot as all other execution keys.
func admissionKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "admission")
}
