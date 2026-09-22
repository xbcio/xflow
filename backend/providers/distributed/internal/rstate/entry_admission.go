package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
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
//	10.. = per exit: output key + node meta key (2 keys each)
//	10+2*exitCount..end = per downstream: inDegree, active, schedule (3 each)
//
// ARGV: 1=resultHash 2=ttl_s 3=graphJSON 4=outcome(success|failed)
//
//	5=exitCount 6=downstreamCount
//	7.. = per exit: encoded exit data + private-output bit (2 args each)
//	7+2*exitCount..end = per downstream: arrivalCount, activeCount, mergeMode,
//	                   executeID, executeBody, skipID, skipBody, nodeName (8 each)
//
// Returns {code, finalStatus, skips}: code 1=accepted, 2=duplicate, 3=conflict;
// skips is the flat {name, count, ...} list of the downstream units this
// admission resolved as skip rather than execute. The Go wrapper reads the
// first two and treats a two-element reply as "no skips", which is both a
// script revision predating this element and an admission that genuinely
// skipped nothing — the same shape, decoded the same way, so the wider reply
// cannot fail an admission Redis has already accepted.
//
// nodeName rides in a slot the skip branch alone reads: an admission request is
// assembled fresh by the caller and handed straight to the script, never
// decoded out of a durable body written by an older revision, so widening the
// per-downstream slot cannot desynchronize a replay.
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
    local dataPos = 7 + (i - 1) * 2
    redis.call('SET', KEYS[keypos], ARGV[dataPos], 'EX', ttl)
    if tonumber(ARGV[dataPos + 1]) == 1 then
        redis.call('HSET', KEYS[keypos + 1], 'private_output', '1')
    end
    -- Never let a refreshed runtime output outlive a prior private marker.
    -- EXPIRE is a no-op when this exit has never had metadata.
    redis.call('EXPIRE', KEYS[keypos + 1], ttl)
    keypos = keypos + 2
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
-- Downstream units resolved as skip rather than execute, as a flat
-- {name, count, ...} list. Same semantics and same observation as the other two
-- skip sites — see advanceNodeLua's doc comment for what a skip means (durable
-- skip intent, position advances, downstream never sees the data, no error).
local skips = {}
if remaining > 0 then
    local n = tonumber(ARGV[6] or '0')
    local argpos = 7 + 2 * exitCount
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
        local arrivalName = ARGV[argpos + 7] or ''
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
                if redis.call('HSETNX', KEYS[9], outboxID, outboxBody) == 1 then
                    redis.call('ZADD', KEYS[8], 0, outboxID)
                end
            end
        end
        keypos = keypos + 3
        argpos = argpos + 8
    end
    redis.call('EXPIRE', KEYS[8], ttl); redis.call('EXPIRE', KEYS[9], ttl)
end
return {1, finalStatus, skips}
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

	// A transient workflow admitted here declares that its payloads must not be
	// projected to SQL. The seed path is where that promise is easiest to lose:
	// it never calls CreateExecution, which is the only other place a
	// per-execution transient marker is written, so without this the marker
	// simply does not exist and every isTransient() guard downstream --
	// projectSeededExecution's included -- resolves "durable" and writes the
	// row. The exposure is not hypothetical: a Kafka trigger group is admitted
	// exclusively through this path, and a workflow is marked transient
	// precisely when it carries raw third-party traffic.
	//
	// The graph is the source of truth here, not a context hint. Submit and
	// Invoke derive the hint from the compiled graph (engine.attachTransientHint)
	// but a trigger group's admission does not pass through either, so there is
	// no hint on this context to read.
	//
	// Written BEFORE the Lua, in its own transaction: the Lua creates the
	// execution, and from that instant another replica can lease a node and
	// commit its output. A marker written afterwards leaves exactly that window
	// open, which is long enough to project a payload.
	if req.Graph != nil && req.Graph.Transient() {
		markTTL := req.Graph.TransientTTL()
		if markTTL <= 0 {
			markTTL = s.transientTTL
		}
		if markTTL <= 0 {
			markTTL = ttl
		}
		ttl = markTTL
		pipe := s.rdb.TxPipeline()
		s.markExecutionTransient(ctx, pipe, execID,
			req.Graph.TransientTTL(), req.Graph.TransientCompletionTTL(), markTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			// Fail the admission rather than proceeding unmarked. Redis is
			// authoritative and this write is cheap; admitting the batch anyway
			// would persist the very payloads the workflow declared ephemeral,
			// and the broker will redeliver it once the error propagates.
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("mark transient execution %q: %w", execID, err)
		}
	}

	// Prime the verdict when the graph is NOT transient: admission is
	// authoritative for this execution (it is the code that decided), and the
	// projection below would otherwise ask isTransient, find no marker -- none
	// is written for a durable execution -- and fall through to the SQL
	// confirmation against a row that admission has not created yet, misreading
	// every seeded execution as transient.
	// Store-wide transient mode writes no per-execution marker, so it must not
	// be primed as durable either.
	if !s.transient && (req.Graph == nil || !req.Graph.Transient()) {
		s.rememberTransient(execID, transientMark{durableConfirmed: true})
	}

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
		admissionKey(t, execID),                         // 1
		execKey(t, execID, "status"),                    // 2
		execKey(t, execID, "graph"),                     // 3
		remainingNodesKey(t, execID),                    // 4
		failedNodesKey(t, execID),                       // 5
		groupUnitStatusKey(t, execID, req.EntryUnitIdx), // 6
		groupUnitMetaKey(t, execID, req.EntryUnitIdx),   // 7
		outboxReadyKey(t, execID),                       // 8
		outboxBodyKey(t, execID),                        // 9
	}

	// Build ARGV.
	args := []any{
		string(req.ResultHash), // 1
		int(ttl.Seconds()),     // 2
		string(graphJSON),      // 3
		string(req.Outcome),    // 4
		len(req.Exits),         // 5
		len(req.Downstream),    // 6
	}

	// Exit outputs (keys + args).
	for _, ex := range req.Exits {
		// Same codec as CommitNode and CommitGroup: this writes the same
		// output:<name> key, so a value stored here has to be readable by the
		// same decodeOutputValue that serves every other path.
		encoded, err := s.encodeOutputValue(ex.Data)
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("marshal exit %q: %w", ex.NodeName, err)
		}
		keys = append(keys,
			outputKey(t, execID, ex.NodeName),
			nodeMetaKey(t, execID, ex.NodeName),
		)
		private := 0
		if ex.PrivateOutput {
			private = 1
		}
		args = append(args, string(encoded), private)
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
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON, arrival.NodeName)
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
	// finalStatus is non-empty only when the seed itself completed the whole
	// execution (remaining hit 0 inside the Lua). Otherwise the execution is
	// still running and later commits carry it to a terminal state.
	finalStatus := types.ExecutionStatusRunning
	if len(res) >= 2 {
		if fs := redisResultString(res[1]); fs != "" {
			finalStatus = types.ExecutionStatus(fs)
		}
	}
	// Element 3 is the skipped-unit list, absent from an older script revision
	// — which then reports no skips rather than failing an admission Redis has
	// already accepted.
	var skipped []engine.SkippedUnit
	if len(res) >= 3 {
		skipped = skippedUnitsFromLua(res[2])
	}
	switch code {
	case 1: // accepted
		// The execution is created INSIDE seedExecutionFromEntryLua — this path
		// never calls CreateExecution, so without this projection a
		// trigger-group-seeded execution has no SQL row at all and is invisible
		// to the audit trail once its Redis keys expire.
		s.markOutboxReadyIndex(ctx, t, execID)
		s.projectSeededExecution(ctx, execID, finalStatus, req)
		return engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateAccepted,
			ExecutionID: execID,
			Duplicate:   false,
			Skipped:     skipped,
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

// emptyJSONObject satisfies the schema's NOT NULL JSON columns for records that
// genuinely have no such data.
var emptyJSONObject = []byte(`{}`)

// projectSeededExecution creates the SQL audit row for an execution that was
// created inside seedExecutionFromEntryLua. Best effort by contract
// (STORAGE-CONTRACT.md): Redis is authoritative and a failed projection must
// never fail an admission that Redis already accepted.
//
// The row is created with the status the Lua landed on, so an execution the
// seed already completed is not first written as running and then left there.
// The error text is the engine-supplied reason only — never boundary-exit data,
// which is node output and routinely carries credentials.
func (s *Store) projectSeededExecution(ctx context.Context, execID types.ExecutionID, status types.ExecutionStatus, req engine.SeedExecutionFromEntryRequest) {
	if s.db == nil || s.isTransient(ctx, execID) {
		return
	}
	now := time.Now()
	rec := &store.ExecutionRecord{
		ExecutionID: execID,
		Namespace:   string(namespace.FromContext(ctx)),
		Status:      status,
		TraceID:     req.TraceID,
		SpanID:      req.SpanID,
		CreatedAt:   now,
		UpdatedAt:   now,
		// workflow_def is NOT NULL in the schema (db/xflow_schema.sql) and a
		// seed carries no WorkflowDef — only a compiled graph. Empty JSON keeps
		// the insert valid instead of failing the whole projection.
		WorkflowDef: emptyJSONObject,
	}
	if status == types.ExecutionStatusFailed {
		rec.Error = req.Error
	}
	if req.Graph != nil {
		rec.WorkflowName = req.Graph.Name()
	}
	// An unencodable field drops only that field, never the row: this
	// projection is the execution's last surviving record once its Redis keys
	// expire, so losing it over a params value that cannot be marshalled is a
	// worse trade than a row without the field. Both JSON columns are nullable.
	if req.Params != nil {
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			s.logProjectionEncodeFailure(execID, "params", err)
		} else {
			rec.Params = paramsJSON
		}
	}
	if req.Runtime != nil {
		runtimeJSON, err := json.Marshal(req.Runtime)
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

// logProjectionEncodeFailure reports a request field that could not be encoded
// for the SQL projection. It is deliberately NOT routed through auditWrite: the
// audit observer and its ok/failed counters answer "did the SQL projection
// diverge from Redis?", and an encode failure is not that — counting it as one
// reports a phantom audit-store outage to whoever is watching the counters. The
// row is still projected, without the field.
func (s *Store) logProjectionEncodeFailure(id types.ExecutionID, field string, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Error("seed_projection_encode_failed",
		"execution_id", string(id), "field", field, "err", err)
}

// admissionKey stores the result hash for a given deterministic execution ID.
// The key lives in the same {execID} hash slot as all other execution keys.
func admissionKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "admission")
}
