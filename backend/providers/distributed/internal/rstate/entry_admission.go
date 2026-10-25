package rstate

import (
	"context"
	"encoding/json"
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
var _ engine.EntryAdmissionStore = (*Store)(nil)
var _ engine.ConcurrencyLimitedEntryAdmissionStore = (*Store)(nil)

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
//	10=concurrency back-reference (exec:conc_wf) 11=exec:workflow_id
//	12=exec:workflow_version
//	13.. = per exit: output key + node meta key (2 keys each)
//	13+2*exitCount..end = per downstream: inDegree, active, schedule (3 each),
//	then ... and last 2 = the execution's own clock keys (created_at_ms,
//	updated_at_ms)
//
// ARGV: 1=resultHash 2=ttl_s 3=graphJSON 4=outcome(success|failed)
//
//	5=exitCount 6=downstreamCount
//	7=the status the execution starts at (running, or queued for an admission
//	  that took a place in the workflow's line instead of a slot)
//	8=outbox readiness score in unix milliseconds (the parked sentinel for a
//	  queued admission, 0 for work that is due now)
//	9=the workflow's concurrency tag, storing which slot set this execution has
//	  to leave when it finishes (empty when the workflow declares no limit)
//	10=the execution's workflow id, 11=its workflow version (empty when the
//	  admission carries no identity)
//	12.. = per exit: encoded exit data + private-output bit (2 args each)
//	12+2*exitCount.. = per downstream: arrivalCount, activeCount, mergeMode,
//	                   executeID, executeBody, skipID, skipBody, nodeName (8 each)
//	then rootSkipCount, then per unselected root: skipID, skipBody (2 each),
//	and finally the execution's creation time in unix milliseconds (created_at
//	and updated_at start equal, exactly as createExecution writes them)
//
// The concurrency key is always declared, even by a caller that passes an empty
// tag: it carries the execution's own hash tag, so naming it keeps every key in
// one slot whether or not the workflow limits concurrency.
//
// The unselected-root tail (engine.UnselectedRootSkips) follows the downstream
// keys: one schedule key per root. Appending it at the end leaves every earlier
// slot where it was. A missing count reads as zero.
//
// Returns {code, finalStatus, skips}: code 1=accepted, 2=duplicate, 3=conflict;
// finalStatus is the status the Lua left the execution at, empty when the
// admission started it and something else will finish it; skips is the flat
// {name, count, ...} list of the downstream units this admission resolved as
// skip rather than execute. The Go wrapper reads the first two and treats a
// two-element reply as "no skips", which is both a script revision predating
// this element and an admission that genuinely skipped nothing — the same
// shape, decoded the same way, so the wider reply cannot fail an admission
// Redis has already accepted.
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
local outboxScore = tonumber(ARGV[8] or '0')
-- Step 1: Write admission key (stores the result hash).
redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
-- Step 2: Create execution, at the status its admission landed on.
redis.call('SET', KEYS[2], ARGV[7], 'EX', ttl)
redis.call('SET', KEYS[3], ARGV[3], 'EX', ttl)
-- Step 2a: the execution's own clock, written with its creation. Inspect
-- exposes these as created_at/updated_at, and an admission-created execution
-- must report the same lifetime as one created through CreateExecution.
local createdAtMs = ARGV[#ARGV]
redis.call('SET', KEYS[#KEYS - 1], createdAtMs, 'EX', ttl)
redis.call('SET', KEYS[#KEYS], createdAtMs, 'EX', ttl)
-- Step 2b: The workflow this execution contends for, so the release that follows
-- its terminal status can find the slot set (or the place in line) to give back.
if ARGV[9] ~= '' then
    redis.call('SET', KEYS[10], ARGV[9], 'EX', ttl)
end
-- Step 2c: The workflow identity captured for this execution, with the same
-- retention as its status. Empty values write nothing and read back empty,
-- which is the documented unattributed state.
if ARGV[10] ~= '' then
    redis.call('SET', KEYS[11], ARGV[10], 'EX', ttl)
end
if ARGV[11] ~= '' then
    redis.call('SET', KEYS[12], ARGV[11], 'EX', ttl)
end
-- Step 3: remaining/failed counters are pre-seeded via TxPipeline (see Go wrapper).
-- Step 4: Mark group unit as done.
redis.call('SET', KEYS[6], 'done', 'EX', ttl)
redis.call('HSET', KEYS[7], 'committed_lease_token', 'seed-triggered')
redis.call('EXPIRE', KEYS[7], ttl)
-- Step 5: Boundary outputs.
local exitCount = tonumber(ARGV[5] or '0')
local keypos = 13
for i = 1, exitCount do
    local dataPos = 12 + (i - 1) * 2
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
    local argpos = 12 + 2 * exitCount
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
                    redis.call('ZADD', KEYS[8], outboxScore, outboxID)
                end
            end
        end
        keypos = keypos + 3
        argpos = argpos + 8
    end
    redis.call('EXPIRE', KEYS[8], ttl); redis.call('EXPIRE', KEYS[9], ttl)
end
-- Step 8: Resolve the roots this admission did not seed as skipped, so they
-- neither hold remaining open nor block a fan-in shared with the seeded entry.
-- They are not reported in skips: that list is the downstream fan-in decision.
if remaining > 0 then
    local downstreamCount = tonumber(ARGV[6] or '0')
    local rootArg = 12 + 2 * exitCount + 8 * downstreamCount
    local rootKey = 13 + 2 * exitCount + 3 * downstreamCount
    local rootCount = tonumber(ARGV[rootArg] or '0')
    for i = 1, rootCount do
        local scheduleKey = KEYS[rootKey + i - 1]
        local skipID = ARGV[rootArg + 1 + (i - 1) * 2]
        local skipBody = ARGV[rootArg + 2 + (i - 1) * 2]
        if redis.call('HSETNX', scheduleKey, 'action', 'skip') == 1 then
            redis.call('EXPIRE', scheduleKey, ttl)
            if redis.call('HSETNX', KEYS[9], skipID, skipBody) == 1 then
                redis.call('ZADD', KEYS[8], outboxScore, skipID)
            end
        end
    end
    if rootCount > 0 then
        redis.call('EXPIRE', KEYS[8], ttl); redis.call('EXPIRE', KEYS[9], ttl)
    end
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
	return s.seedExecutionFromEntry(ctx, req, nil)
}

// SeedExecutionFromEntryWithConcurrency implements
// engine.ConcurrencyLimitedEntryAdmissionStore: the same admission, judged
// against the seeded graph's concurrency policy before anything is written.
//
// The slot decision is a SEPARATE transition from the seed, because the slot set
// is tagged by workflow while every key the seed writes is tagged by the
// deterministic execution ID, and Redis Cluster refuses a script that names both.
// The order — charge first, then seed — is what makes the pair sound: a refused
// admission has written nothing when it returns, and an execution can never be
// created running without the slot it was admitted on. The window between the
// two is real and is closed by the reconciler rather than by atomicity, exactly
// as it is on the submission path (see concurrency_reconcile.go).
//
// A charge this call made is therefore never given back by this call's error
// handling. An error from the seed Lua cannot be told apart from a seed that
// applied and whose reply was lost, and releasing the slot of a running
// execution would over-admit — the one failure the limit exists to prevent —
// while leaving a leaked slot only costs capacity until the next sweep, which
// finds a member naming no live execution and clears it. The two replies where
// the store does know better — duplicate and conflict — give back exactly the
// charge this call added (see below).
func (s *Store) SeedExecutionFromEntryWithConcurrency(ctx context.Context, req engine.SeedExecutionFromEntryRequest, admission engine.ConcurrencyAdmission) (engine.SeedExecutionFromEntryResponse, error) {
	return s.seedExecutionFromEntry(ctx, req, &admission)
}

// seedExecutionFromEntry is the seed body both entry points share; admission is
// nil when the seeded graph declares no concurrency limit, which is the plain
// admission with no slot consulted at all.
func (s *Store) seedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest, admission *engine.ConcurrencyAdmission) (engine.SeedExecutionFromEntryResponse, error) {
	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	t := req.Namespace
	if t == "" {
		t = namespace.FromContext(ctx)
	}
	ttl := s.execTTL

	// Step 0: the slot decision. It runs before anything is written because a
	// refusal has to leave nothing behind — no execution, no admission, not even
	// the transient marker below — so that the entry-unit result still belongs to
	// the caller and can be retried once a slot frees or the backlog drains.
	// A refusal is an answer the caller acts on, not an error: the broker is told
	// to hold the offset, which is only possible if the call reads as a decision.
	status := types.ExecutionStatusRunning
	outboxScore := int64(0)
	concTag := ""
	var admit workflowAdmitResult
	if admission != nil {
		concTag = workflowDigestTag(t, admission.Workflow)
		// Same wait as the submission path registers: a seed that ends up in the
		// line has to stay discoverable to the expiry sweep for as long as it may
		// wait, or its deadline passes with nothing left to notice it.
		s.registerConcurrencyWorkflow(ctx, t, concTag, concurrencyIndexWait(*admission))
		var err error
		admit, err = s.admitWorkflowSlot(ctx, t, execID, *admission)
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("admit workflow %q for %q: %w", admission.Workflow, req.AdmissionKey, err)
		}
		if refused := concurrencyRefusal(admit.code); refused != nil {
			return engine.SeedAdmissionRefusal(refused)
		}
		if admit.code == concurrencyAdmitQueued {
			// The execution is created queued and its intents are scored into the
			// future, exactly as a queued submission's are, so it cannot run until
			// a release promotes it.
			status = types.ExecutionStatusQueued
			outboxScore = queuedOutboxHold.UnixMilli()
		}
	}

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

	// The queue wait is added last, after every other retention rule has had its
	// say, so it lengthens whatever retention the execution actually got — a
	// transient execution's short TTL included. A parked execution's keys are
	// written once and then sit untouched until promotion, so a retention that
	// does not cover the wait expires them mid-queue and leaves the line with a
	// member naming nothing. This mirrors createExecution's own rule for a queued
	// submission, down to recording the extension as the execution's retention so
	// a later writer does not shorten it back.
	if admission != nil && status == types.ExecutionStatusQueued && admission.QueueTimeout > 0 && ttl > 0 {
		ttl += admission.QueueTimeout
		s.ttlMu.Lock()
		s.execTTLs[execID] = ttl
		s.ttlMu.Unlock()
	}

	// The explicit durable marker, written before the Lua creates the execution
	// for the same reason the transient marker above is: from the instant the
	// execution exists another replica can lease a node. Without a marker that
	// replica asks SQL, whose row this path projects only after the Lua and
	// best-effort even then, and caches the projection lag as "transient" --
	// permanently skipping this execution's audit writes. Store-wide transient
	// mode writes no marker, matching createExecution.
	if !s.transient && (req.Graph == nil || !req.Graph.Transient()) {
		pipe := s.rdb.TxPipeline()
		s.markExecutionDurable(ctx, pipe, execID, ttl)
		if _, err := pipe.Exec(ctx); err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("mark durable execution %q: %w", execID, err)
		}
	}

	// The registration namespace is immutable metadata about the execution and is
	// written before the Lua for the same reason the retention record below is:
	// from the instant the execution exists another replica can dispatch its
	// sub-workflows, and dispatch resolves them by this value. It is the
	// admission's own namespace — the request's when supplied, the authenticated
	// ctx otherwise — never the queue ctx dispatch will later run under.
	//
	// SetNX, not Set: a duplicate admission finds the execution already there,
	// and rewriting this key would reset its TTL past the retention the original
	// creation granted. The value it would write is the same one.
	if err := s.rdb.SetNX(ctx, execKey(t, execID, "namespace"), string(t), ttl).Err(); err != nil {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("persist namespace for %q: %w", execID, err)
	}

	// The instant this admission creates the execution. It is read once and
	// serves both as the created_at the Lua stamps and as the base of the
	// deadline below, so the deadline is exactly created_at plus the graph's time
	// budget rather than a second clock reading a few writes later.
	nowMs := time.Now().UTC().UnixMilli()

	// The time budget's deadline, derived here because the entry-seed request
	// carries the graph but no stamped snapshot. It is written outside the Lua so
	// the script's key and argument positions do not move, before the Lua for the
	// reason the namespace above is: from the instant the execution exists another
	// replica reads it back. SetNX for the same reason as the namespace: a
	// duplicate admission must keep the deadline of the original, not push it out.
	// A graph with no budget writes nothing, which reads back as "no budget".
	if req.Graph != nil && req.Graph.Timeout() > 0 {
		deadlineMs := nowMs + req.Graph.Timeout().Milliseconds()
		if err := s.rdb.SetNX(ctx, execKey(t, execID, "deadline_ms"), deadlineMs, ttl).Err(); err != nil {
			return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("persist deadline for %q: %w", execID, err)
		}
	}

	// Persist the retention record for the same reason createExecution does:
	// GetExecutionRetention treats its absence as "window unknown", so a
	// creation path that skips the record would degrade every later no-evidence
	// drop on this execution to unattributed. The value is the retention the
	// seeded writes use, capped so it can never exceed what a terminal write
	// could have used (recordedRetention, and store-wide transient mode shortens
	// the execution's real terminal retention below the seed TTL): under-stating
	// the window is safe, over-stating it is the failure the record exists to
	// prevent. Written before the Lua creates the execution, like the transient
	// marker above; Redis is authoritative and the write is cheap, so a failure
	// fails the admission rather than admitting an execution whose loss signal
	// can never be proven.
	retention := s.recordedRetention(ttl)
	if s.transient && s.transientTTL > 0 && s.transientTTL < retention {
		retention = s.transientTTL
	}
	if ms := retention.Milliseconds(); ms > 0 {
		if recordTTL := s.retentionRecordTTL(retention); recordTTL > 0 {
			if err := s.rdb.Set(ctx, retentionRecordKey(t, execID), strconv.FormatInt(ms, 10), recordTTL).Err(); err != nil {
				return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("persist retention for %q: %w", execID, err)
			}
		}
	}

	// Prime the verdict when the graph is NOT transient: admission is
	// authoritative for this execution (it is the code that decided), and the
	// durable marker above is already in Redis, but priming keeps the projection
	// below from paying a Redis round-trip to re-learn what this call already
	// knows. Store-wide transient mode writes no per-execution marker, so it
	// must not be primed as durable either.
	if !s.transient && (req.Graph == nil || !req.Graph.Transient()) {
		s.rememberTransient(execID, transientMark{durableConfirmed: true})
	}

	// Register the namespace in the discovery registry before the execution
	// exists, as createExecution and SeedExecution do. The maintenance loops (the
	// deadline sweep among them) SCAN or list only registered namespaces, so a
	// non-default namespace whose executions are created solely by this path
	// would otherwise be invisible to them. Non-fatal for the same reason as
	// there: the next durable write retries it.
	if !s.isTransient(ctx, execID) {
		_ = s.registerNamespace(ctx, t)
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
		// The workflow this execution contends for, so its release can find the
		// slot set to leave. Declared whether or not there is a limit: it carries
		// the execution's own hash tag, so naming it keeps every key in one slot.
		concurrencyTagKey(t, execID), // 10
		// The workflow identity captured for this execution. Fixed slots in the
		// same {execID} slot as every other execution key, so the Lua can write
		// them atomically with the execution's creation.
		execKey(t, execID, "workflow_id"),      // 11
		execKey(t, execID, "workflow_version"), // 12
	}

	// Build ARGV.
	args := []any{
		string(req.ResultHash), // 1
		int(ttl.Seconds()),     // 2
		string(graphJSON),      // 3
		string(req.Outcome),    // 4
		len(req.Exits),         // 5
		len(req.Downstream),    // 6
		string(status),         // 7
		outboxScore,            // 8
		concTag,                // 9
		string(req.WorkflowID), // 10
		req.WorkflowVersion,    // 11
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

	// Unselected roots (keys + args), after every downstream slot.
	rootSkips := engine.UnselectedRootSkips(execID, req.Graph, req.EntryUnitIdx)
	args = append(args, len(rootSkips))
	for _, skip := range rootSkips {
		skipID := redisSkipOutboxID(execID, skip.NodeName, 0)
		skipJSON, err := marshalRedisOutboxEntry(skipID, skip, time.Time{})
		if err != nil {
			return engine.SeedExecutionFromEntryResponse{}, err
		}
		keys = append(keys, scheduleKey(t, execID, skip.UnitIdx))
		args = append(args, skipID, skipJSON)
	}

	// The execution's own clock, appended last: the script reads both keys and
	// the instant off the tail, so every positional slot above stays stable.
	keys = append(keys, execKey(t, execID, "created_at_ms"), execKey(t, execID, "updated_at_ms"))
	args = append(args, nowMs)

	// Run Lua.
	res, err := seedExecutionFromEntryLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed triggered group %q: %w", req.AdmissionKey, err)
	}
	if len(res) < 1 {
		return engine.SeedExecutionFromEntryResponse{}, fmt.Errorf("seed triggered group %q: empty response", req.AdmissionKey)
	}

	code := redisResultInt(res[0])
	// The status the execution is left at: what its admission started it as
	// (queued, or running), unless the Lua itself completed the whole execution
	// (remaining hit 0 inside it), in which case it names the terminal status.
	finalStatus := status
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
		// The seed terminalizes the execution inside the Lua when its graph was
		// fully resolved by the admission itself; record the terminal marker for
		// the same reason the commit paths do.
		s.markExecutionTerminalBestEffort(ctx, execID, finalStatus)
		s.releaseWorkflowSlotAfterTerminal(ctx, execID, finalStatus)
		s.projectSeededExecution(ctx, execID, finalStatus, req)
		resp := engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateAccepted,
			ExecutionID: execID,
			Skipped:     skipped,
		}
		// Queued describes where the execution ended up, not where it was
		// admitted: an admission that drove its execution straight to a terminal
		// status (a fatal entry-unit outcome with nothing downstream) has already
		// left the line by the release above, and reporting it as still waiting
		// would hand the caller a position that does not exist.
		if status == types.ExecutionStatusQueued && !types.IsTerminalExecutionStatus(finalStatus) {
			resp.Queued = true
			resp.QueuePosition = int(admit.position)
		}
		return resp, nil
	case 2, 3: // duplicate (same hash) / conflict (different hash)
		state := engine.AdmissionStateAccepted
		duplicate := true
		if code == 3 {
			state, duplicate = engine.AdmissionStateConflict, false
		}
		// A reply of duplicate or conflict means the admission key was already
		// occupied, so this call created nothing — but it may still have charged
		// a slot on the way in, and that charge has to be given back. The case is
		// a redelivery of a batch whose execution has already finished: its slot
		// was released when it terminated, so the membership test that makes
		// admission idempotent found nothing to short-circuit on and charged
		// again. The reservation is provably safe because added is only true when
		// the member was in NEITHER the slot set nor the line: it is this call's
		// own charge and nothing else's, so removing it cannot take a slot from
		// the execution the reply names. Without this the workflow would run one
		// short of its limit until the reconciler swept a slot that was never
		// the finished execution's to begin with.
		if admit.added {
			s.releaseWorkflowSlotBestEffort(ctx, execID, t, concTag)
		}
		// The position is reported for the execution the response names, so a
		// retry of a seed that is still waiting sees where it still stands. A
		// read failure leaves it at 0 rather than failing the reply: the caller's
		// offset commit hangs on the duplicate/conflict answer, and a position
		// that could not be read is not worth withholding it for.
		position := 0
		if pos, err := s.queuePositionFor(ctx, t, execID); err == nil {
			position = pos
		}
		return engine.SeedExecutionFromEntryResponse{
			State:         state,
			ExecutionID:   execID,
			Duplicate:     duplicate,
			Queued:        position > 0,
			QueuePosition: position,
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
	// Workflow identity: prefer the definition attached to the seeding context
	// (the control plane resolves it from the authoritative registry). A store
	// reached without one — or with a stored definition written by POST, which
	// carries no id — still has the request's own workflow id: the id the
	// admission key was derived from. That is an identity, not a guess, so it is
	// recorded rather than leaving a trigger-created execution unattributed.
	rec.WorkflowID = req.WorkflowID
	if def, ok := engine.WorkflowDefFromContext(ctx); ok && def.ID != "" {
		rec.WorkflowID = types.WorkflowID(def.ID)
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
