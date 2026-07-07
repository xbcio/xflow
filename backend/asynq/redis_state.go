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

// leaseExpiryZSetKey is the global lease-deadline index used by the sweeper.
// score = (IssuedAt + TTL).UnixMilli, member = leaseExpiryMember(execID, name).
// The {expiry} hash tag pins every member to the same cluster slot so
// ZRANGEBYSCORE / ZREM never cross slots.
const leaseExpiryZSetKey = "xflow:leases:{expiry}"

// leaseExpiryMember packs execID and node name into a ZSET member. Uses a
// vertical bar separator because execID / node name are UTF-8 identifiers and
// never contain '|' in practice.
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
if existing == 'committing' and ARGV[1] == 'running' then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
if ARGV[2] ~= '' then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
end
return 1
`)

// claimTaskLeaseLua fences task result commits by atomically checking the
// current node lease token and moving the node into the transient committing
// state. New leases cannot overwrite a committing node.
// KEYS[1] = node status key, KEYS[2] = node meta hash
// ARGV[1] = expected lease token, ARGV[2] = ttl seconds, ARGV[3] = expected activation id
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
    return {1, status}
end
local token = redis.call('HGET', KEYS[2], 'lease_token')
if not token or token == '' or token ~= ARGV[1] then
    return {0, status}
end
redis.call('SET', KEYS[1], 'committing', 'EX', tonumber(ARGV[2]))
redis.call('HSET', KEYS[2], 'lease_token', '')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
return {1, 'committing'}
`)

// Returns 1 (acquired) or 0 (already locked).
var resumeNodeLua = redis.NewScript(`
local locked = redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1]))
if not locked then return 0 end
return 1
`)

// resetNodeForRetryLua rolls a Running node back to Pending and clears its
// lease token so the engine can re-enqueue the task after a backoff. No-op if
// the node is not in 'running' state — keeps the operation idempotent against
// concurrent claim/timeouts/cancels.
// KEYS[1] = node status key, KEYS[2] = node meta hash
// ARGV[1] = ttl seconds
// Returns 1 (reset) or 0 (no-op).
var resetNodeForRetryLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' and status ~= 'committing' then
    return 0
end
redis.call('SET', KEYS[1], 'pending', 'EX', tonumber(ARGV[1]))
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
return 1
`)

// revokeLeaseLua is the atomic sweeper-side reclaim. It verifies the caller
// still owns the lease (token match) and the node is still Running before
// rolling the node back to Pending. A concurrent runner commit that already
// moved the node to Committing / terminal will have cleared the token, so
// the sweeper sees a mismatch and returns 0 — the lease-token fencing IS
// the race protection.
// KEYS[1] = node status key, KEYS[2] = node meta hash
// ARGV[1] = expected lease token, ARGV[2] = ttl seconds
// Returns 1 (revoked) or 0 (race lost — commit already ran or token stale).
var revokeLeaseLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'running' then
    return 0
end
local token = redis.call('HGET', KEYS[2], 'lease_token')
if not token or token == '' or token ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], 'pending', 'EX', tonumber(ARGV[2]))
redis.call('HSET', KEYS[2], 'lease_token', '', 'lease_id', '', 'lease_issued_at_ms', '0', 'lease_ttl_ms', '0')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
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

	// Audit-trail observability — Redis is system-of-record; the store/sqlstore
	// audit trail is best-effort. auditWrite routes failures through these
	// instead of silently dropping them.
	audit         AuditObserver
	auditCounters *auditCounters
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

	pipe := s.rdb.Pipeline()
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
	// Seed in-degree counters.
	for i, d := range e.Graph.InDegree {
		if d > 0 {
			pipe.Set(ctx, inDegreeKey(e.ID, i), d, ttl)
			keys = append(keys, inDegreeKey(e.ID, i))
		}
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
	pipe.Del(ctx, execKey(e.ID, "status"), execKey(e.ID, "graph"), execKey(e.ID, "error"), execKey(e.ID, "params"), execKey(e.ID, "runtime"), execKey(e.ID, "trace_id"), execKey(e.ID, "span_id"), executionKeySetKey(e.ID))
	if e.Graph != nil {
		for i := range e.Graph.InDegree {
			pipe.Del(ctx, inDegreeKey(e.ID, i), activeInputsKey(e.ID, i))
		}
	}
	_, _ = pipe.Exec(ctx)
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
	pipe := s.rdb.Pipeline()
	keys := []string{execKey(id, "status")}
	pipe.Set(ctx, execKey(id, "status"), string(status), ttl)
	if errMsg != "" {
		pipe.Set(ctx, execKey(id, "error"), errMsg, ttl)
		keys = append(keys, execKey(id, "error"))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("update execution status %q: %w", id, err)
	}
	// Clean up timeout ZSET entries when execution is canceled.
	if status == types.ExecutionStatusCanceled {
		s.cleanupOnCancel(ctx, id)
	}
	if types.IsTerminalExecutionStatus(status) {
		if err := s.shortenTransientCompletionTTL(ctx, id, keys...); err != nil {
			return err
		}
	} else if err := s.refreshTransientTTL(ctx, id, keys...); err != nil {
		return err
	}
	if s.db != nil && !s.transient {
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

func (s *redisState) UpsertNode(ctx context.Context, n *engine.NodeSnapshot) error {
	key := nodeStatusKey(n.ExecutionID, n.Name)
	outKey := outputKey(n.ExecutionID, n.Name)
	metaKey := nodeMetaKey(n.ExecutionID, n.Name)

	var outputJSON string
	if n.Output != nil {
		b, _ := json.Marshal(n.Output)
		outputJSON = string(b)
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
	if n.LeaseID != "" || n.LeaseToken != "" || n.Attempt != 0 || n.ActivationID != 0 || n.AutoDepth != 0 || !n.LeaseIssuedAt.IsZero() {
		meta := map[string]any{
			"lease_id":      string(n.LeaseID),
			"lease_token":   string(n.LeaseToken),
			"attempt":       n.Attempt,
			"activation_id": n.ActivationID,
			"auto_depth":    n.AutoDepth,
		}
		if !n.LeaseIssuedAt.IsZero() {
			meta["lease_issued_at_ms"] = n.LeaseIssuedAt.UnixMilli()
		}
		if n.LeaseTTL > 0 {
			meta["lease_ttl_ms"] = n.LeaseTTL.Milliseconds()
		}
		if err := s.rdb.HSet(ctx, metaKey, meta).Err(); err != nil {
			return fmt.Errorf("upsert node lease %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		_ = s.rdb.Expire(ctx, metaKey, ttl).Err()
		keys = append(keys, metaKey)
	}
	// Lease-expiry index: leases with a deadline live in a global ZSET so the
	// sweeper can find them without scanning every node key.
	member := leaseExpiryMember(n.ExecutionID, n.Name)
	if n.Status == types.NodeStatusRunning && n.LeaseToken != "" && !n.LeaseIssuedAt.IsZero() && n.LeaseTTL > 0 {
		expiryMs := float64(n.LeaseIssuedAt.Add(n.LeaseTTL).UnixMilli())
		if err := s.rdb.ZAdd(ctx, leaseExpiryZSetKey, redis.Z{Score: expiryMs, Member: member}).Err(); err != nil {
			return fmt.Errorf("index lease expiry %q/%q: %w", n.ExecutionID, n.Name, err)
		}
	} else if n.Status != types.NodeStatusRunning {
		// Any non-Running status means the lease no longer needs sweeping —
		// terminal, committing, suspended, or pending after retry.
		_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, member).Err()
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
	return ns, nil
}

func (s *redisState) ResetNodeForRetry(ctx context.Context, id types.ExecutionID, name string) error {
	ttl := s.getExecTTL(id)
	_, err := resetNodeForRetryLua.Run(ctx, s.rdb,
		[]string{nodeStatusKey(id, name), nodeMetaKey(id, name)},
		int(ttl.Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("reset node for retry %q/%q: %w", id, name, err)
	}
	// Drop from the lease-expiry index regardless of whether the Lua reset
	// took effect: either it succeeded (we cleared state) or a concurrent
	// commit already handled it (index will be pruned on the next ZADD).
	_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, leaseExpiryMember(id, name)).Err()
	if err := s.refreshTransientTTL(ctx, id, nodeStatusKey(id, name), nodeMetaKey(id, name)); err != nil {
		return err
	}
	return nil
}

// leaseIndexBatchLimit caps a single ListExpiredLeases scan. Small enough that
// the sweeper stays quick under heavy backlog; the sweeper re-polls until the
// list drains, so this is not a coverage cap, only a per-call bound.
const leaseIndexBatchLimit = 256

func (s *redisState) ListExpiredLeases(ctx context.Context, before time.Time) ([]engine.ExpiredLease, error) {
	max := fmt.Sprintf("%d", before.UnixMilli())
	members, err := s.rdb.ZRangeByScore(ctx, leaseExpiryZSetKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    max,
		Offset: 0,
		Count:  leaseIndexBatchLimit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list expired leases: %w", err)
	}
	if len(members) == 0 {
		return nil, nil
	}
	out := make([]engine.ExpiredLease, 0, len(members))
	for _, member := range members {
		execID, nodeName, ok := splitLeaseMember(member)
		if !ok {
			_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, member).Err()
			continue
		}
		status, err := s.rdb.Get(ctx, nodeStatusKey(execID, nodeName)).Result()
		if err == redis.Nil || err == nil && status != string(types.NodeStatusRunning) {
			// Node is no longer Running — someone already committed / reset.
			// Clean up the stale index entry and skip.
			_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, member).Err()
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
			_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, member).Err()
			continue
		}
		lease := engine.ExpiredLease{
			ExecutionID: execID,
			NodeName:    nodeName,
			LeaseID:     engine.LeaseID(meta["lease_id"]),
			LeaseToken:  engine.LeaseToken(meta["lease_token"]),
		}
		parseInt64(meta["lease_issued_at_ms"], func(ms int64) {
			lease.IssuedAt = time.UnixMilli(ms).UTC()
		})
		parseInt64(meta["lease_ttl_ms"], func(ms int64) {
			lease.TTL = time.Duration(ms) * time.Millisecond
		})
		parseInt64(meta["activation_id"], func(v int64) { lease.ActivationID = int(v) })
		parseInt64(meta["auto_depth"], func(v int64) { lease.AutoDepth = int(v) })
		out = append(out, lease)
	}
	return out, nil
}

func (s *redisState) RevokeLease(ctx context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	if token == "" {
		return false, nil
	}
	ttl := s.getExecTTL(id)
	result, err := revokeLeaseLua.Run(ctx, s.rdb,
		[]string{nodeStatusKey(id, name), nodeMetaKey(id, name)},
		string(token), int(ttl.Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease %q/%q: %w", id, name, err)
	}
	// Drop from the expiry index whether or not the Lua path won the race —
	// the index entry is now stale either way.
	_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, leaseExpiryMember(id, name)).Err()
	if result == 1 {
		if err := s.refreshTransientTTL(ctx, id, nodeStatusKey(id, name), nodeMetaKey(id, name)); err != nil {
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
	ttl := s.getExecTTL(lease.Task.ExecutionID)
	result, err := claimTaskLeaseLua.Run(ctx, s.rdb,
		[]string{nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName), nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName)},
		string(lease.LeaseToken), int(ttl.Seconds()), lease.Task.ActivationID,
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
	// Successful claim moved the node to Committing (or it was already
	// terminal). Either way the lease no longer needs sweeping.
	_ = s.rdb.ZRem(ctx, leaseExpiryZSetKey, leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName)).Err()
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID, nodeStatusKey(lease.Task.ExecutionID, lease.Task.NodeName), nodeMetaKey(lease.Task.ExecutionID, lease.Task.NodeName)); err != nil {
		return nil, false, err
	}
	return ns, true, nil
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
		s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: member,
		})
	}
	// Extend TTL to prevent key expiry during suspension.
	s.extendExecTTL(ctx, id, name, s.suspendTTL(id, spec))
	return nil, nil
}

func (s *redisState) suspendOrConsumeMulti(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	ttl := s.suspendTTL(id, spec)
	batchKey := signalBatchKey(id, name)

	for _, sigName := range spec.Signals {
		raw, err := s.rdb.Get(ctx, signalKey(id, sigName)).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("multi-signal get %q: %w", sigName, err)
		}
		if raw == "" {
			continue
		}
		if err := s.rdb.HSet(ctx, batchKey, sigName, raw).Err(); err != nil {
			return nil, fmt.Errorf("multi-signal collect %q: %w", sigName, err)
		}
		_ = s.rdb.Del(ctx, signalKey(id, sigName)).Err()
		payload, ready, err := s.multiSignalPayload(ctx, id, name, sigName, raw, spec)
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
		s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, name),
		})
	}
	s.extendExecTTL(ctx, id, name, ttl)
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
		s.rdb.ZRem(ctx, timeoutZSetKey, timeoutMember(id, nodeName))
		s.rdb.ZAdd(ctx, timeoutZSetKey, redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, nodeName),
		})
	}
	// Extend TTL to prevent key expiry during suspension.
	s.extendExecTTL(ctx, id, nodeName, s.suspendTTL(id, spec))
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
	if err != nil || len(nodes) == 0 {
		return
	}
	pipe := s.rdb.Pipeline()
	for _, name := range nodes {
		pipe.ZRem(ctx, timeoutZSetKey, timeoutMember(id, name))
	}
	pipe.Del(ctx, suspendedNodesKey(id))
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
// keys from expiring while waiting for a signal.
func (s *redisState) extendExecTTL(ctx context.Context, id types.ExecutionID, nodeName string, ttl time.Duration) {
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
	_, _ = pipe.Exec(ctx)
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

func isTerminalNode(status types.NodeStatus) bool { return types.IsTerminalNodeStatus(status) }

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
		defer pubsub.Close()
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

// mustJSON marshals v to a JSON string, returning "{}" on error.
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// splitPrefix splits "execID/rest" into (execID, rest).
func splitPrefix(key, prefix string) string {
	return strings.TrimPrefix(key, prefix)
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
