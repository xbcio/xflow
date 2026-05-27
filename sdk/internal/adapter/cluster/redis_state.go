package cluster

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
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

const defaultExecTTL = 24 * time.Hour

// execTTLCtxKey is the context key for per-execution TTL override.
type execTTLCtxKey struct{}

// WithExecTTLCtx attaches a per-execution TTL override to the context.
// The redisState.CreateExecution method reads this value and stores it
// for subsequent TTL renewal operations.
func WithExecTTLCtx(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, execTTLCtxKey{}, d)
}

// execTTLFromCtx extracts the per-execution TTL override from context.
func execTTLFromCtx(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(execTTLCtxKey{}).(time.Duration); ok {
		return v
	}
	return 0
}

// ---------------------------------------------------------------------------
// Redis key helpers — all keys use {id} as a hash tag for cluster compatibility
// ---------------------------------------------------------------------------

func execKey(id types.ExecutionID, suffix string) string {
	return fmt.Sprintf("xflow:exec:{%s}:%s", id, suffix)
}

func nodeStatusKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:status", id, name)
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
// KEYS[1] = signal key, KEYS[2] = node status key, KEYS[3] = waiter key, KEYS[4] = suspended_nodes SET
// ARGV[1] = node name, ARGV[2] = ttl seconds
// Returns signal payload JSON (consumed) or nil (suspended).
var suspendOrConsumeLua = redis.NewScript(`
local signal = redis.call('GET', KEYS[1])
if signal then
    redis.call('DEL', KEYS[1])
    return signal
end
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
// KEYS[1] = node status key, KEYS[2] = output key (optional, may be empty string)
// ARGV[1] = new status, ARGV[2] = output JSON (or ""), ARGV[3] = ttl seconds
// Returns 1 (written) or 0 (skipped, already terminal).
var upsertNodeLua = redis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing == 'success' or existing == 'failed' or existing == 'skipped' or existing == 'canceled' or existing == 'continued' then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
if ARGV[2] ~= '' then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
end
return 1
`)
// Returns 1 (acquired) or 0 (already locked).
var resumeNodeLua = redis.NewScript(`
local locked = redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1]))
if not locked then return 0 end
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
// redisState — implements core.StateBackend
// ---------------------------------------------------------------------------

type redisState struct {
	rdb     *redis.Client
	db      store.ClusterStore // may be nil
	execTTL time.Duration

	// in-memory graph cache for CheckCompletion (avoids Redis round-trips per node)
	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph

	// per-execution TTL overrides (set via SubmitOption)
	ttlMu    sync.RWMutex
	execTTLs map[types.ExecutionID]time.Duration
}

func newRedisState(rdb *redis.Client, db store.ClusterStore, execTTL time.Duration) *redisState {
	return &redisState{
		rdb:      rdb,
		db:       db,
		execTTL:  execTTL,
		graphs:   make(map[types.ExecutionID]*graph.Graph),
		execTTLs: make(map[types.ExecutionID]time.Duration),
	}
}

func (s *redisState) ttlSec() int { return int(s.execTTL.Seconds()) }

// getExecTTL returns the per-execution TTL override if set, otherwise the adapter default.
func (s *redisState) getExecTTL(id types.ExecutionID) time.Duration {
	s.ttlMu.RLock()
	ttl := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	return s.execTTL
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *redisState) CreateExecution(ctx context.Context, e *engine.ExecutionSnapshot) error {
	ttl := s.execTTL

	// Check for per-execution TTL override from context.
	if override := execTTLFromCtx(ctx); override > 0 {
		ttl = override
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = override
		s.ttlMu.Unlock()
	}

	// Serialize graph for persistence (allows recovery after worker restart).
	graphJSON, err := json.Marshal(e.Graph)
	if err != nil {
		return fmt.Errorf("marshal graph for %q: %w", e.ID, err)
	}

	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, execKey(e.ID, "status"), string(e.Status), ttl)
	pipe.Set(ctx, execKey(e.ID, "graph"), string(graphJSON), ttl)
	// Seed in-degree counters.
	for i, d := range e.Graph.InDegree {
		if d > 0 {
			pipe.Set(ctx, inDegreeKey(e.ID, i), d, ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create execution %q: %w", e.ID, err)
	}

	// Cache graph for CheckCompletion.
	s.mu.Lock()
	s.graphs[e.ID] = e.Graph
	s.mu.Unlock()

	// Dual-write to store.
	if s.db != nil {
		now := time.Now()
		rec := &store.ExecutionRecord{
			ExecutionID: e.ID,
			Status:      e.Status,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := s.db.CreateExecution(ctx, rec); err != nil {
			return fmt.Errorf("store create execution: %w", err)
		}
	}
	return nil
}

func (s *redisState) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error {
	ttl := s.getExecTTL(id)
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, execKey(id, "status"), string(status), ttl)
	if errMsg != "" {
		pipe.Set(ctx, execKey(id, "error"), errMsg, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("update execution status %q: %w", id, err)
	}
	// Clean up timeout ZSET entries when execution is canceled.
	if status == types.StatusCanceled {
		s.cleanupOnCancel(ctx, id)
	}
	if s.db != nil {
		if err := s.db.UpdateExecutionStatus(ctx, id, status, errMsg); err != nil {
			// Non-fatal: log but don't fail the execution.
			_ = err
		}
	}
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
	return &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.Status(val),
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

	var outputJSON string
	if n.Output != nil {
		b, _ := json.Marshal(n.Output)
		outputJSON = string(b)
	}

	ttl := s.getExecTTL(n.ExecutionID)
	_, err := upsertNodeLua.Run(ctx, s.rdb,
		[]string{key, outKey},
		n.Status, outputJSON, int(ttl.Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("upsert node %q/%q: %w", n.ExecutionID, n.Name, err)
	}

	if s.db != nil {
		var outBytes []byte
		if n.Output != nil {
			outBytes, _ = json.Marshal(n.Output)
		}
		rec := &store.NodeRecord{
			ExecutionID: n.ExecutionID,
			NodeName:    n.Name,
			Status:      n.Status,
			Output:      outBytes,
			Port:        n.Port,
			UpdatedAt:   time.Now(),
		}
		_ = s.db.UpsertNode(ctx, rec)
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
	return &engine.NodeSnapshot{ExecutionID: id, Name: name, Status: val}, nil
}

// ---------------------------------------------------------------------------
// Scheduling counters
// ---------------------------------------------------------------------------

func (s *redisState) DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	activeFlag := 0
	if portActive {
		activeFlag = 1
	}
	vals, err := propagateLua.Run(ctx, s.rdb,
		[]string{inDegreeKey(id, nodeIdx), activeInputsKey(id, nodeIdx)},
		activeFlag, s.ttlSec(),
	).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("propagate lua: %w", err)
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

func (s *redisState) SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
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
				return &node.SignalPayload{Triggered: node.SignalReceived, Name: sigName, Data: data}, nil
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

func (s *redisState) DeliverSignal(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, error) {
	dataJSON, _ := json.Marshal(data)
	result, err := signalOrStoreLua.Run(ctx, s.rdb,
		[]string{signalKey(id, signalName), waiterKey(id, signalName), suspendedNodesKey(id)},
		string(dataJSON), s.ttlSec(),
	).Result()
	if err != nil && err != redis.Nil {
		return "", fmt.Errorf("signal or store lua: %w", err)
	}
	if result != nil {
		if nodeName, ok := result.(string); ok && nodeName != "" {
			// Node is being resumed — remove its timeout entry from the ZSET.
			s.cleanupOnResume(ctx, id, nodeName)
			return nodeName, nil
		}
	}

	// Dual-write signal to store.
	if s.db != nil {
		_ = s.db.SaveSignal(ctx, &store.SignalRecord{
			ExecutionID: id,
			SignalName:  signalName,
			Payload:     dataJSON,
			CreatedAt:   time.Now(),
		})
	}
	return "", nil
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
	if result == 1 && s.db != nil {
		// Best-effort dual-write: mark signal as revoked in persistent store.
		_, _ = s.db.RevokeSignal(ctx, id, signalName)
	}
	return result == 1, nil
}

func (s *redisState) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
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
			return &node.SignalPayload{Triggered: node.SignalReceived, Name: newSignalName, Data: data}, nil
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
	return s.rdb.Set(ctx, outputKey(id, name), string(b), s.execTTL).Err()
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
	pipe.Expire(ctx, prefix+":graph", ttl)
	pipe.Expire(ctx, prefix+":node:"+nodeName+":status", ttl)
	pipe.Expire(ctx, prefix+":output:"+nodeName, ttl)
	pipe.Expire(ctx, prefix+":suspended_nodes", ttl)
	_, _ = pipe.Exec(ctx)
}

// suspendTTL returns the TTL to use when a node is suspended.
// It picks the larger of the execution's base TTL and spec.Timeout + 1 hour.
func (s *redisState) suspendTTL(id types.ExecutionID, spec *node.SuspendSpec) time.Duration {
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

func isTerminalNode(status string) bool {
	switch status {
	case "success", "failed", "skipped", "canceled", "continued":
		return true
	}
	return false
}

// doneChannel returns the Pub/Sub channel name for execution completion events.
func doneChannel(id types.ExecutionID) string {
	return fmt.Sprintf("xflow:exec:{%s}:done", id)
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
