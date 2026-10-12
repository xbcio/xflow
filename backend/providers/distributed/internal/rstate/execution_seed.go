package rstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Compile-time interface satisfaction.
var _ engine.ExecutionSeeder = (*Store)(nil)

// seedExecutionLua creates an execution around units whose outcome is already
// decided, in one transition:
//  1. create the execution (status, graph, concurrency back-reference)
//  2. subtract the resolutions from remaining
//  3. record every resolved unit — status, port, output, and the "seed"
//     scheduling marker that stops any later arrival from scheduling it
//  4. apply every resolved unit's arrivals (same fan-in as advanceNodeLua)
//  5. write the caller's own intents
//  6. finalize when nothing is left to run
//
// The counters are pre-seeded by the Go wrapper's TxPipeline (remaining, failed,
// in-degree), because they are derived from the graph; the script only moves
// them.
//
// Steps 3 and 4 are separate passes over the same blocks, and that is the point
// of the layout below: every resolved unit is recorded before ANY arrival is
// applied. An arrival from one resolved unit reaches another resolved unit's
// counters, so applying them as they are recorded would let the first unit's
// arrival schedule the second, and the seeded execution would re-run work whose
// result it was built on. The pass order is the same guarantee the memory
// backend gets from marking every node before calling its fan-in.
//
// KEYS: 1=exec:status 2=exec:graph 3=remaining 4=failed
//
//	5=concurrency back-reference (exec:conc_wf)
//	6=outbox:ready 7=outbox:body
//	8.. = per resolved unit: node status, node meta, output, scheduling
//	      marker (4 keys each), then that unit's arrivals: in-degree, active
//	      inputs, scheduling marker (3 keys each)
//	then one scheduling-marker key per entry that carries a skip intent,
//	in entry order
//
// ARGV: 1=ttl_s 2=graphJSON 3=the status the execution starts at
//
//	4=outbox readiness score in unix milliseconds (the parked sentinel for a
//	  queued admission, 0 for work that is due now)
//	5=the workflow's concurrency tag (empty when the workflow declares none)
//	6=1 when this graph counts units (acyclic and present), 0 otherwise
//	7=resolved-unit count 8=entry count
//	9.. = per resolved unit: status, port, output, private bit, node index,
//	      arrival count (6 args each), then that unit's arrivals: arrival
//	      count, active count, merge mode, execute ID, execute body, skip ID,
//	      skip body, node name (8 args each)
//	then per entry: entry ID, entry body, scheduling-marker key number (0 when
//	this entry carries no skip intent; 3 each)
//
// Returns {code, finalStatus, skips}: code is always 1 (the script creates the
// execution, so there is no admission key to collide on), finalStatus is the
// terminal status when the seed resolved the whole graph and empty when the
// execution is left to run, and skips is the flat {name, count, ...} list of
// the units this seed resolved as skip rather than execute.
//
// A skip entry's scheduling-marker key is written in the same transition as its
// intent, exactly as createExecution fences a root skip: the intent and the
// marker that makes it a legal resolution are one state, and a delivery of the
// former without the latter would be refused as rogue.
var seedExecutionLua = redis.NewScript(`
local ttl = tonumber(ARGV[1])
local counters = tonumber(ARGV[6] or '0') == 1
local outboxScore = tonumber(ARGV[4] or '0')
-- Step 1: create the execution, at the status its admission landed on.
redis.call('SET', KEYS[1], ARGV[3], 'EX', ttl)
redis.call('SET', KEYS[2], ARGV[2], 'EX', ttl)
-- The workflow this execution contends for, so the release that follows its
-- terminal status can find the slot set (or the place in line) to give back.
if ARGV[5] ~= '' then
    redis.call('SET', KEYS[5], ARGV[5], 'EX', ttl)
end
-- Step 2: subtract the resolutions. Every seeded unit is resolved, which is what
-- makes remaining the number of units this execution still has to run.
local unitCount = tonumber(ARGV[7] or '0')
local remaining = 0
if counters then
    remaining = redis.call('DECRBY', KEYS[3], unitCount)
    redis.call('EXPIRE', KEYS[3], ttl)
end
-- Step 3: record every resolved unit. No arrival is applied yet -- see the pass
-- order argument in the script's Go-side doc comment.
local positions = {}
local keypos = 8
local argpos = 9
for i = 1, unitCount do
    local status = ARGV[argpos]
    local port = ARGV[argpos + 1]
    local output = ARGV[argpos + 2]
    local privateBit = tonumber(ARGV[argpos + 3] or '0') == 1
    local nodeIdx = ARGV[argpos + 4]
    local arrivalCount = tonumber(ARGV[argpos + 5] or '0')
    redis.call('SET', KEYS[keypos], status, 'EX', ttl)
    local metaKey = KEYS[keypos + 1]
    redis.call('HSET', metaKey, 'port', port, 'node_idx', nodeIdx, 'committed_lease_token', 'seed')
    if privateBit then
        redis.call('HSET', metaKey, 'private_output', '1')
    end
    redis.call('EXPIRE', metaKey, ttl)
    -- The output is written even for a private unit: privacy gates what the
    -- public projections show (the marker does that), not what a downstream
    -- expression resolves.
    redis.call('SET', KEYS[keypos + 2], output, 'EX', ttl)
    -- The decision for this unit is made and nothing may make it again. "seed"
    -- is a third value rather than an invented one: "skip" would claim the unit
    -- was skipped as a branch decision and "execute" that it is due.
    redis.call('HSET', KEYS[keypos + 3], 'action', 'seed')
    redis.call('EXPIRE', KEYS[keypos + 3], ttl)
    positions[i] = {keypos, argpos, arrivalCount}
    keypos = keypos + 4 + 3 * arrivalCount
    argpos = argpos + 6 + 8 * arrivalCount
end
-- Step 4: apply the arrivals, which schedules whatever the resolutions made
-- ready. This is the transition a commit's fan-in runs, and it is what decides
-- execute vs skip for each successor.
local skips = {}
local apply = (not counters) or remaining > 0
if apply then
    for i = 1, unitCount do
        local base = positions[i]
        local kpos = base[1] + 4
        local apos = base[2] + 6
        for j = 1, base[3] do
            local inDegreeKey = KEYS[kpos]
            local activeKey = KEYS[kpos + 1]
            local scheduleKey = KEYS[kpos + 2]
            local arrivals = tonumber(ARGV[apos])
            local activeArrivals = tonumber(ARGV[apos + 1])
            local mergeMode = ARGV[apos + 2]
            local executeID = ARGV[apos + 3]
            local executeBody = ARGV[apos + 4]
            local skipID = ARGV[apos + 5]
            local skipBody = ARGV[apos + 6]
            local arrivalName = ARGV[apos + 7] or ''
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
                        table.insert(skips, arrivalName)
                        table.insert(skips, 1)
                    end
                    if redis.call('HSETNX', KEYS[7], outboxID, outboxBody) == 1 then
                        redis.call('ZADD', KEYS[6], outboxScore, outboxID)
                    end
                end
            end
            kpos = kpos + 3
            apos = apos + 8
        end
    end
    -- Step 5: the caller's own intents, for units no arrival can reach.
    local entryCount = tonumber(ARGV[8] or '0')
    local skipKeyPos = 8 + 4 * unitCount
    for i = 1, unitCount do
        skipKeyPos = skipKeyPos + 3 * positions[i][3]
    end
    for i = 1, entryCount do
        local entryID = ARGV[argpos]
        local entryBody = ARGV[argpos + 1]
        local markerNum = tonumber(ARGV[argpos + 2] or '0')
        argpos = argpos + 3
        if markerNum > 0 then
            local scheduleKey = KEYS[skipKeyPos + markerNum - 1]
            redis.call('HSET', scheduleKey, 'action', 'skip')
            redis.call('EXPIRE', scheduleKey, ttl)
        end
        if redis.call('HSETNX', KEYS[7], entryID, entryBody) == 1 then
            redis.call('ZADD', KEYS[6], outboxScore, entryID)
        end
    end
end
redis.call('EXPIRE', KEYS[6], ttl)
redis.call('EXPIRE', KEYS[7], ttl)
-- Step 6: an execution with nothing left to run completes here rather than
-- sitting at zero remaining with nothing queued.
local finalStatus = ''
if counters and remaining <= 0 then
    finalStatus = 'success'
    if tonumber(redis.call('GET', KEYS[4]) or '0') > 0 then
        finalStatus = 'failed'
    end
    redis.call('SET', KEYS[1], finalStatus, 'EX', ttl)
end
return {1, finalStatus, skips}
`)

// SeedExecution implements engine.ExecutionSeeder: an execution created around
// units whose outcome is already decided, admitted and scheduled in one
// critical section.
//
// The slot decision is a separate transition from the seed for the same reason
// the entry-admission path's is (see SeedExecutionFromEntryWithConcurrency): the
// slot set is tagged by workflow while every key the seed writes is tagged by
// the execution ID, and Redis Cluster refuses a script that names both. Charge
// first, then seed, so a refused admission has written nothing when it returns
// and a seeded execution is never running without the slot it was admitted on.
//
// The charges this call makes are not given back by its error handling. An
// error from the seed Lua cannot be told apart from a seed that applied and
// whose reply was lost, and releasing the slot of a running execution would
// over-admit — the one failure the limit exists to prevent — while a leaked
// slot only costs capacity until the reconciler sweeps it. This is the same
// trade the entry-admission seed makes.
//
// The seed is not idempotent, and deliberately so: it is the only creation path
// with no natural key to collide on. CreateExecution is keyed by the execution
// ID it is given, the entry seed by its admission key, and a rerun by nothing
// at all — a rerun of the same node twice is two executions, which is exactly
// what the caller asked for. A retry that reaches the store twice therefore
// subtracts the resolutions twice; the engine derives a fresh execution ID per
// call, so the retry that matters (a lost reply to the client) creates a second
// execution rather than corrupting the first.
func (s *Store) SeedExecution(ctx context.Context, req engine.SeedExecutionRequest) (engine.SeedExecutionResponse, error) {
	if req.Execution == nil {
		return engine.SeedExecutionResponse{}, errors.New("seed execution: no execution snapshot")
	}
	for _, unit := range req.Units {
		if unit.Status != types.NodeStatusSuccess && unit.Status != types.NodeStatusSkipped {
			return engine.SeedExecutionResponse{}, fmt.Errorf(
				"seed execution %q: unit %q has status %q, want %q or %q",
				req.Execution.ID, unit.NodeName, unit.Status,
				types.NodeStatusSuccess, types.NodeStatusSkipped)
		}
	}
	for _, entry := range req.Entries {
		if entry.ID == "" {
			return engine.SeedExecutionResponse{}, fmt.Errorf("seed execution %q: empty outbox entry ID", req.Execution.ID)
		}
	}

	execID := req.Execution.ID
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(ctx, execID)

	// Step 0: the slot decision, before anything is written.
	status := types.ExecutionStatusRunning
	outboxScore := int64(0)
	concTag := ""
	var admit workflowAdmitResult
	if req.Admission != nil {
		concTag = workflowDigestTag(t, req.Admission.Workflow)
		// Same wait as the submission path registers: a seed that ends up in the
		// line has to stay discoverable to the expiry sweep for as long as it may
		// wait, or its deadline passes with nothing left to notice it.
		s.registerConcurrencyWorkflow(ctx, t, concTag, concurrencyIndexWait(*req.Admission))
		var err error
		admit, err = s.admitWorkflowSlot(ctx, t, execID, *req.Admission)
		if err != nil {
			return engine.SeedExecutionResponse{}, fmt.Errorf("admit workflow %q for %q: %w", req.Admission.Workflow, execID, err)
		}
		if refused := concurrencyRefusal(admit.code); refused != nil {
			return engine.SeedExecutionResponse{}, refused
		}
		if admit.code == concurrencyAdmitQueued {
			status = types.ExecutionStatusQueued
			outboxScore = queuedOutboxHold.UnixMilli()
		}
	}

	// A seeded execution of a transient graph declares that its payloads must
	// not be projected to SQL, and that promise has to be made before the Lua
	// creates the execution: from that instant another replica can lease a node
	// and commit its output, so a marker written afterwards leaves a window in
	// which the first commit reads "durable" and projects. The graph is the
	// source of truth, not a context hint (see the same argument on the
	// entry-admission path).
	if req.Execution.Graph != nil && req.Execution.Graph.Transient() {
		markTTL := req.Execution.Graph.TransientTTL()
		if markTTL <= 0 {
			markTTL = s.transientTTL
		}
		if markTTL <= 0 {
			markTTL = ttl
		}
		ttl = markTTL
		pipe := s.rdb.TxPipeline()
		s.markExecutionTransient(ctx, pipe, execID,
			req.Execution.Graph.TransientTTL(), req.Execution.Graph.TransientCompletionTTL(), markTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			return engine.SeedExecutionResponse{}, fmt.Errorf("mark transient execution %q: %w", execID, err)
		}
	}

	// The queue wait is added last, after every other retention rule has had its
	// say, and recorded as the execution's retention so a later writer cannot
	// shorten it back: a parked execution's keys are written once and then sit
	// untouched until promotion, so a retention that does not cover the wait
	// expires them mid-queue and leaves the line with a member naming nothing.
	if req.Admission != nil && status == types.ExecutionStatusQueued && req.Admission.QueueTimeout > 0 && ttl > 0 {
		ttl += req.Admission.QueueTimeout
		s.ttlMu.Lock()
		s.execTTLs[execID] = ttl
		s.ttlMu.Unlock()
	}

	// Persist the retention record for the same reason createExecution does:
	// GetExecutionRetention treats its absence as "window unknown", so a creation
	// path that skips the record degrades every later no-evidence drop on this
	// execution to unattributed.
	retention := s.recordedRetention(ttl)
	if s.transient && s.transientTTL > 0 && s.transientTTL < retention {
		retention = s.transientTTL
	}
	if ms := retention.Milliseconds(); ms > 0 {
		if recordTTL := s.retentionRecordTTL(retention); recordTTL > 0 {
			if err := s.rdb.Set(ctx, retentionRecordKey(t, execID), strconv.FormatInt(ms, 10), recordTTL).Err(); err != nil {
				return engine.SeedExecutionResponse{}, fmt.Errorf("persist retention for %q: %w", execID, err)
			}
		}
	}

	// Prime the verdict when the graph is NOT transient: this call is
	// authoritative for the execution, and the SQL projection below would
	// otherwise ask isTransient, find no marker -- none is written for a durable
	// execution -- and fall through to the SQL confirmation against a row that
	// does not exist yet, misreading every seeded execution as transient.
	if !s.transient && (req.Execution.Graph == nil || !req.Execution.Graph.Transient()) {
		s.rememberTransient(execID, transientMark{durableConfirmed: true})
	}
	if !s.isTransient(ctx, execID) {
		_ = s.registerNamespace(ctx, t)
	}

	// Phase 1: pre-seed the counters the script reads but does not create
	// (remaining, failed, in-degree). SetNX so a re-seed of the same ID -- which
	// no production path performs -- cannot reset counters already in motion.
	counters := req.Execution.Graph != nil && !req.Execution.Graph.AllowCycles()
	if counters {
		pipe := s.rdb.TxPipeline()
		pipe.SetNX(ctx, remainingNodesKey(t, execID), req.Execution.Graph.UnitCount(), ttl)
		pipe.SetNX(ctx, failedNodesKey(t, execID), 0, ttl)
		for i := 0; i < req.Execution.Graph.UnitCount(); i++ {
			d := req.Execution.Graph.UnitInDegreeAt(i)
			if d > 0 {
				pipe.SetNX(ctx, inDegreeKey(t, execID, i), d, ttl)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return engine.SeedExecutionResponse{}, fmt.Errorf("seed counters for %q: %w", execID, err)
		}
	}

	graphJSON, err := json.Marshal(req.Execution.Graph)
	if err != nil {
		return engine.SeedExecutionResponse{}, fmt.Errorf("marshal graph for %q: %w", execID, err)
	}

	countersFlag := 0
	if counters {
		countersFlag = 1
	}
	keys := []string{
		execKey(t, execID, "status"),  // 1
		execKey(t, execID, "graph"),   // 2
		remainingNodesKey(t, execID),  // 3
		failedNodesKey(t, execID),     // 4
		concurrencyTagKey(t, execID),  // 5
		outboxReadyKey(t, execID),     // 6
		outboxBodyKey(t, execID),      // 7
	}
	args := []any{
		int(ttl.Seconds()), // 1
		string(graphJSON),  // 2
		string(status),     // 3
		outboxScore,        // 4
		concTag,            // 5
		countersFlag,       // 6
		len(req.Units),     // 7
		len(req.Entries),   // 8
	}

	// Resolved units: the resolution and its arrivals, in one block per unit (see
	// the ARGV layout above -- the key list is walked the same way).
	for _, unit := range req.Units {
		encoded, err := s.encodeOutputValue(unit.Output)
		if err != nil {
			return engine.SeedExecutionResponse{}, fmt.Errorf("marshal output %q/%q: %w", execID, unit.NodeName, err)
		}
		private := 0
		if unit.PrivateOutput {
			private = 1
		}
		keys = append(keys,
			nodeStatusKey(t, execID, unit.NodeName),
			nodeMetaKey(t, execID, unit.NodeName),
			outputKey(t, execID, unit.NodeName),
			scheduleKey(t, execID, unit.UnitIdx),
		)
		args = append(args, string(unit.Status), unit.Port, encoded, private, unit.NodeIdx, len(unit.Downstream))
		for _, arrival := range unit.Downstream {
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
				return engine.SeedExecutionResponse{}, err
			}
			skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
				ExecutionID: execID,
				NodeName:    arrival.NodeName,
				NodeIdx:     arrival.NodeIdx,
				UnitIdx:     arrival.UnitIdx,
				Type:        engine.TaskTypeNodeSkip,
			}, time.Time{})
			if err != nil {
				return engine.SeedExecutionResponse{}, err
			}
			args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode,
				executeID, executeJSON, skipID, skipJSON, arrival.NodeName)
		}
	}

	// Entries, then the scheduling-marker keys a skip entry is fenced by: one key
	// per skip entry, in entry order, numbered from 1 in the entry's own slot.
	skipKeys := 0
	for _, entry := range req.Entries {
		markerNum := 0
		if entry.Task.Type == engine.TaskTypeNodeSkip {
			skipKeys++
			markerNum = skipKeys
			keys = append(keys, scheduleKey(t, execID, entry.Task.UnitIdx))
		}
		encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return engine.SeedExecutionResponse{}, fmt.Errorf("seed execution %q outbox %q: %w", execID, entry.ID, err)
		}
		args = append(args, entry.ID, encoded, markerNum)
	}

	res, err := seedExecutionLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.SeedExecutionResponse{}, fmt.Errorf("seed execution %q: %w", execID, err)
	}
	if len(res) < 1 {
		return engine.SeedExecutionResponse{}, fmt.Errorf("seed execution %q: empty response", execID)
	}
	finalStatus := status
	if len(res) >= 2 {
		if fs := redisResultString(res[1]); fs != "" {
			finalStatus = types.ExecutionStatus(fs)
		}
	}
	var skipped []engine.SkippedUnit
	if len(res) >= 3 {
		skipped = skippedUnitsFromLua(res[2])
	}

	s.markOutboxReadyIndex(ctx, t, execID)
	s.markExecutionTerminalBestEffort(ctx, execID, finalStatus)
	s.releaseWorkflowSlotAfterTerminal(ctx, execID, finalStatus)
	if req.Admission != nil {
		s.completePromotions(ctx, t, concTag, admit.promoted)
	}
	s.projectSeededRerun(ctx, execID, finalStatus, req)

	resp := engine.SeedExecutionResponse{Skipped: skipped}
	// Queued describes where the execution ended up, not where it was admitted:
	// a seed that resolved its whole graph has already left the line by the
	// release above, and reporting it as still waiting would hand the caller a
	// position that does not exist.
	if status == types.ExecutionStatusQueued && !types.IsTerminalExecutionStatus(finalStatus) {
		resp.Queued = true
		resp.QueuePosition = int(admit.position)
	}
	return resp, nil
}

// projectSeededRerun creates the SQL audit row for an execution the seed Lua
// created. Best effort by contract (STORAGE-CONTRACT.md): Redis is
// authoritative and a failed projection must never fail a seed Redis already
// accepted.
//
// It is the entry-admission projection's sibling and exists for the same
// reason: this path never calls CreateExecution, so without it a rerun's
// execution has no SQL row at all and is invisible to the audit trail once its
// Redis keys expire. The row is created with the status the Lua landed on, so
// an execution the seed already completed is not first written as running and
// then left there.
func (s *Store) projectSeededRerun(ctx context.Context, execID types.ExecutionID, status types.ExecutionStatus, req engine.SeedExecutionRequest) {
	if s.db == nil || s.isTransient(ctx, execID) {
		return
	}
	now := time.Now()
	rec := &store.ExecutionRecord{
		ExecutionID: execID,
		Namespace:   string(namespace.FromContext(ctx)),
		Status:      status,
		CreatedAt:   now,
		UpdatedAt:   now,
		// workflow_def is NOT NULL in the schema (db/xflow_schema.sql) and a seed
		// carries no WorkflowDef -- only a compiled graph. Empty JSON keeps the
		// insert valid instead of failing the whole projection.
		WorkflowDef: emptyJSONObject,
	}
	if req.Execution.Graph != nil {
		rec.WorkflowName = req.Execution.Graph.Name()
	}
	rec.TraceID = req.Execution.TraceID
	rec.SpanID = req.Execution.SpanID
	// An unencodable field drops only that field, never the row: this projection
	// is the execution's last surviving record once its Redis keys expire, so
	// losing it over a params value that cannot be marshalled is a worse trade
	// than a row without the field. Both JSON columns are nullable.
	if req.Execution.Params != nil {
		paramsJSON, err := json.Marshal(req.Execution.Params)
		if err != nil {
			s.logProjectionEncodeFailure(execID, "params", err)
		} else {
			rec.Params = paramsJSON
		}
	}
	if req.Execution.Runtime != nil {
		runtimeJSON, err := json.Marshal(req.Execution.Runtime)
		if err != nil {
			s.logProjectionEncodeFailure(execID, "runtime", err)
		} else {
			rec.Runtime = runtimeJSON
		}
	}
	s.auditWrite(ctx, "create_seeded_execution", func(ctx context.Context) error {
		return s.db.CreateExecution(ctx, rec)
	})
}
