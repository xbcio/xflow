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

func (s *Store) SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	if s.transient {
		// Store-wide transient mode disables suspend at the engine layer
		// (WithSuspendDisabled); this guard is defense-in-depth so a direct
		// StateStore caller cannot park a node the control plane has no way to
		// signal -- that mode rejects signal/revoke/inspect too.
		//
		// s.transient, not isTransient: this is a capability gate paired with the
		// engine option, and both are set from the same store-wide configuration.
		// The per-execution resolver fails closed to transient on a Redis read
		// error, which is the right call for a SQL projection (one lost audit row)
		// and the wrong one here (a Redis blip would turn a legitimate suspend
		// into ErrSuspendUnsupported).
		//
		// Per-workflow transient executions deliberately do NOT hit this branch.
		// They run on an otherwise durable store whose signal/revoke/inspect
		// endpoints all work, so parking a waiter is legitimate; the mode only
		// promises "no SQL projection, short TTL". engine.WithSuspendDisabled is
		// set solely from the store-wide mode (sdk/xflow/engine.go), and
		// attachTransientHint does not touch it, so such a suspend reaches the
		// code below. suspendTTL therefore has to resolve the per-execution
		// transient TTL rather than the store default.
		return nil, engine.ErrSuspendUnsupported
	}
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		return s.suspendOrConsumeMulti(ctx, id, name, spec)
	}

	// Track waiter keys registered in previous iterations so we can clean them
	// up if a later signal is found pre-delivered.
	var registeredWaiters []string
	t := namespace.FromContext(ctx)

	// Compute timeout score once; only the last signal's Lua call registers it
	// atomically with the suspend, ensuring no window where the node is parked
	// without its timeout entry.
	var timeoutScore float64
	timeoutMemberStr := ""
	if spec.Timeout > 0 {
		timeoutScore = float64(time.Now().Add(spec.Timeout).Unix())
		timeoutMemberStr = timeoutMember(id, name)
	}

	// Check each awaited signal name.
	for i, sigName := range spec.Signals {
		// Only register timeout atomically on the last signal iteration (all
		// prior signals were not pre-delivered, so the node will be parked).
		isLast := i == len(spec.Signals)-1
		var score float64
		if isLast {
			score = timeoutScore
		}
		result, err := suspendOrConsumeLua.Run(ctx, s.rdb,
			[]string{
				signalKey(t, id, sigName),
				nodeStatusKey(t, id, name),
				waiterKey(t, id, sigName),
				suspendedNodesKey(t, id),
				resumeLockKey(t, id, name),
				timeoutZSetKey(t, id),
			},
			name, s.ttlSec(), score, timeoutMemberStr,
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
					pipe.SRem(ctx, suspendedNodesKey(t, id), name)
					_, _ = pipe.Exec(ctx)
				}
				var data map[string]any
				if err := json.Unmarshal([]byte(raw), &data); err != nil {
					return nil, fmt.Errorf("unmarshal suspend signal %q/%q: %w", id, name, err)
				}
				return &types.SignalPayload{Triggered: types.SignalReceived, Name: sigName, Data: data}, nil
			}
		}
		// This signal was not pre-delivered; a waiter key was registered.
		registeredWaiters = append(registeredWaiters, waiterKey(t, id, sigName))
	}
	// Extend TTL to prevent key expiry during suspension.
	if err := s.extendExecTTL(ctx, id, name, spec, s.suspendTTL(ctx, id, spec)); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Store) suspendOrConsumeMulti(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	ttl := s.suspendTTL(ctx, id, spec)
	t := namespace.FromContext(ctx)
	batchKey := signalBatchKey(t, id, name)

	// Atomically collect every pre-delivered signal into the batch hash. The
	// collection (GET→HSET→DEL per signal) runs in one Lua transition so a crash
	// cannot delete a signal before the quorum check parks the node.
	keys := []string{batchKey}
	args := []any{int(ttl.Seconds())}
	for _, sigName := range spec.Signals {
		keys = append(keys, signalKey(t, id, sigName))
		args = append(args, sigName)
	}
	collected, err := suspendOrConsumeMultiCollectLua.Run(ctx, s.rdb, keys, args...).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("suspend or consume multi collect %q/%q: %w", id, name, err)
	}

	// Evaluate quorum using the batch hash populated atomically above. Use the
	// last collected signal as the payload's Data field when quorum is reached.
	if len(collected) > 0 {
		lastName := collected[len(collected)-2]
		lastRaw := collected[len(collected)-1]
		payload, ready, err := s.multiSignalPayload(ctx, id, name, lastName, lastRaw, spec)
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
	pipe.Del(ctx, resumeLockKey(t, id, name))
	pipe.Set(ctx, nodeStatusKey(t, id, name), string(types.NodeStatusSuspended), ttl)
	pipe.Set(ctx, waiterSpecKey(t, id, name), string(specJSON), ttl)
	pipe.Expire(ctx, batchKey, ttl)
	for _, sigName := range spec.Signals {
		pipe.Set(ctx, waiterKey(t, id, sigName), name, ttl)
	}
	pipe.SAdd(ctx, suspendedNodesKey(t, id), name)
	pipe.Expire(ctx, suspendedNodesKey(t, id), ttl)
	// Register timeout atomically with the park to avoid a window where the node
	// is parked without its timeout entry (#15).
	if spec.Timeout > 0 {
		pipe.ZAdd(ctx, timeoutZSetKey(t, id), redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, name),
		})
		pipe.Expire(ctx, timeoutZSetKey(t, id), ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("park multi-signal waiter: %w", err)
	}
	if err := s.extendExecTTL(ctx, id, name, spec, ttl); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Store) loadWaiterSpec(ctx context.Context, id types.ExecutionID, nodeName string) (*types.SuspendSpec, error) {
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, waiterSpecKey(t, id, nodeName)).Bytes()
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

func (s *Store) addMultiSignal(ctx context.Context, id types.ExecutionID, nodeName string, signalName string, dataJSON string, spec *types.SuspendSpec) (*types.SignalPayload, bool, error) {
	t := namespace.FromContext(ctx)
	if err := s.rdb.HSet(ctx, signalBatchKey(t, id, nodeName), signalName, dataJSON).Err(); err != nil {
		return nil, false, fmt.Errorf("add multi-signal %q/%q/%q: %w", id, nodeName, signalName, err)
	}
	_ = s.rdb.Expire(ctx, signalBatchKey(t, id, nodeName), s.suspendTTL(ctx, id, spec)).Err()
	return s.multiSignalPayload(ctx, id, nodeName, signalName, dataJSON, spec)
}

func (s *Store) multiSignalPayload(ctx context.Context, id types.ExecutionID, nodeName string, signalName string, dataJSON string, spec *types.SuspendSpec) (*types.SignalPayload, bool, error) {
	t := namespace.FromContext(ctx)
	rawAll, err := s.rdb.HGetAll(ctx, signalBatchKey(t, id, nodeName)).Result()
	if err != nil {
		return nil, false, fmt.Errorf("read multi-signal batch %q/%q: %w", id, nodeName, err)
	}
	if len(rawAll) < signalQuorum(spec) {
		return nil, false, nil
	}

	all := make(map[string]map[string]any, len(rawAll))
	for name, raw := range rawAll {
		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return nil, false, fmt.Errorf("unmarshal multi-signal %q/%q/%q: %w", id, nodeName, name, err)
		}
		all[name] = payload
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return nil, false, fmt.Errorf("unmarshal multi-signal payload %q/%q: %w", id, nodeName, err)
	}
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      signalName,
		Data:      data,
		All:       all,
	}, true, nil
}

func (s *Store) cleanupMultiSignal(ctx context.Context, id types.ExecutionID, nodeName string, spec *types.SuspendSpec) {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	for _, sigName := range spec.Signals {
		pipe.Del(ctx, waiterKey(t, id, sigName))
	}
	pipe.Del(ctx, waiterSpecKey(t, id, nodeName), signalBatchKey(t, id, nodeName))
	pipe.SRem(ctx, suspendedNodesKey(t, id), nodeName)
	pipe.ZRem(ctx, timeoutZSetKey(t, id), timeoutMember(id, nodeName))
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

func (s *Store) DeliverSignal(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, *types.SignalPayload, error) {
	dataJSON, _ := json.Marshal(data) // json.Marshal of map[string]any cannot fail
	t := namespace.FromContext(ctx)

	waiter, err := s.rdb.Get(ctx, waiterKey(t, id, signalName)).Result()
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
		[]string{signalKey(t, id, signalName), waiterKey(t, id, signalName), suspendedNodesKey(t, id)},
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

	if s.db != nil && !s.isTransient(ctx, id) {
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
func (s *Store) cleanupOnResume(ctx context.Context, id types.ExecutionID, nodeName string) {
	t := namespace.FromContext(ctx)
	s.rdb.ZRem(ctx, timeoutZSetKey(t, id), timeoutMember(id, nodeName))
}

// PeekResumeTarget returns the node name suspended and waiting for signalName,
// or "" when no waiter exists. It does not consume the signal.
func (s *Store) PeekResumeTarget(ctx context.Context, id types.ExecutionID, signalName string) (string, error) {
	t := namespace.FromContext(ctx)
	waiter, err := s.rdb.Get(ctx, waiterKey(t, id, signalName)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("peek waiter %q/%q: %w", id, signalName, err)
	}
	return waiter, nil
}

// DeliverSignalWithOutbox atomically consumes a signal and writes the resume
// delivery intent to the outbox in one Lua transition. See
// deliverSignalWithOutboxLua for the single/multi-signal semantics.
func (s *Store) DeliverSignalWithOutbox(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any, intent engine.ResumeIntent) (string, *types.SignalPayload, bool, error) {
	dataJSON, _ := json.Marshal(data) // json.Marshal of map[string]any cannot fail
	ttl := s.ttlSec()
	nowMs := time.Now().UTC().UnixMilli()
	timeoutMemberStr := ""
	if intent.NodeName != "" {
		timeoutMemberStr = timeoutMember(id, intent.NodeName)
	}

	var (
		spec    *types.SuspendSpec
		specErr error
		// entryBody is marshaled with ActivationID=0 so the "activation_id" field
		// is dropped (omitempty). The Lua script reads the LIVE activation from node
		// meta and stamps it into the body exactly once, closing the TOCTOU window
		// where a concurrent re-suspend could leave a stale activation in the entry.
		entryBody string
	)
	if intent.NodeName != "" {
		spec, specErr = s.loadWaiterSpec(ctx, id, intent.NodeName)
		if specErr != nil {
			return "", nil, false, fmt.Errorf("load waiter spec for signal %q/%q: %w", id, intent.NodeName, specErr)
		}
		payload := &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: data}
		task := engine.Task{
			ExecutionID:  id,
			NodeName:     intent.NodeName,
			NodeIdx:      intent.NodeIdx,
			UnitIdx:      intent.UnitIdx,
			Type:         engine.TaskTypeNodeResume,
			Payload:      payload,
			ActivationID: 0, // Lua reads live value from node meta
			AutoDepth:    intent.AutoDepth,
		}
		// Use a placeholder entryID for marshaling; Lua rebuilds the real entryID
		// from the live activation_id read inside the transaction.
		placeholderID := fmt.Sprintf("resume/%s/%s/0/signal/%s", id, intent.NodeName, signalName)
		body, err := marshalRedisOutboxEntry(placeholderID, task, time.Now().UTC())
		if err != nil {
			return "", nil, false, fmt.Errorf("marshal resume outbox %q/%q: %w", id, intent.NodeName, err)
		}
		entryBody = body
	}

	multi := 0
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		multi = 1
	}
	quorum := signalQuorum(spec)
	t := namespace.FromContext(ctx)

	keys := []string{
		signalKey(t, id, signalName),
		waiterKey(t, id, signalName),
		suspendedNodesKey(t, id),
		signalBatchKey(t, id, intent.NodeName),
		waiterSpecKey(t, id, intent.NodeName),
		resumeLockKey(t, id, intent.NodeName),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		timeoutZSetKey(t, id),
		nodeMetaKey(t, id, intent.NodeName), // KEYS[10]: live activation source
	}
	if multi == 1 {
		for _, sig := range spec.Signals {
			keys = append(keys, waiterKey(t, id, sig))
		}
	}
	args := []any{
		string(dataJSON), int(ttl), intent.NodeName,
		"", entryBody, nowMs, multi, quorum, signalName, timeoutMemberStr,
		string(id), intent.NodeName, signalName, // ARGV[11..13]: entryID components for Lua
	}

	result, err := deliverSignalWithOutboxLua.Run(ctx, s.rdb, keys, args...).Result()
	if err != nil && err != redis.Nil {
		return "", nil, false, fmt.Errorf("deliver signal with outbox %q/%q: %w", id, signalName, err)
	}
	nodeName, _ := result.(string)
	if nodeName == "" {
		return "", nil, false, nil
	}
	return nodeName, nil, true, nil
}

func (s *Store) AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	t := namespace.FromContext(ctx)
	result, err := resumeNodeLua.Run(ctx, s.rdb,
		[]string{resumeLockKey(t, id, name)},
		s.ttlSec(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("resume lock lua: %w", err)
	}
	return result == 1, nil
}

func (s *Store) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error) {
	t := namespace.FromContext(ctx)
	// Pre-read the waiter to build the resume_lock key for Cluster-safe KEYS
	// declaration. If the waiter is absent, pass a hash-tagged dummy key (the
	// script's `if nodeName then` guard prevents it from being accessed).
	waiter, err := s.rdb.Get(ctx, waiterKey(t, id, signalName)).Result()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("pre-read waiter for revoke %q/%q: %w", id, signalName, err)
	}
	lockKey := resumeLockKey(t, id, waiter)
	if waiter == "" {
		// No waiter; use a hash-tagged placeholder so all KEYS share the same slot.
		lockKey = execKey(t, id, "revoke_lock_sentinel")
	}
	result, err := revokeSignalLua.Run(ctx, s.rdb,
		[]string{signalKey(t, id, signalName), waiterKey(t, id, signalName), lockKey},
	).Int64()
	if err != nil {
		return false, fmt.Errorf("revoke signal lua: %w", err)
	}
	if result == 1 && s.db != nil && !s.isTransient(ctx, id) {
		s.auditWrite(ctx, "revoke_signal", func(ctx context.Context) error {
			_, err := s.db.RevokeSignal(ctx, id, signalName)
			return err
		})
	}
	return result == 1, nil
}

func (s *Store) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	if s.transient {
		// See SuspendOrConsume: transient mode never parks a waiter.
		return nil, engine.ErrSuspendUnsupported
	}
	t := namespace.FromContext(ctx)
	result, err := resuspendAtomicLua.Run(ctx, s.rdb,
		[]string{
			resumeLockKey(t, id, nodeName),
			waiterKey(t, id, oldSignalName),
			signalKey(t, id, newSignalName),
			waiterKey(t, id, newSignalName),
			suspendedNodesKey(t, id),
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
			if err := json.Unmarshal([]byte(raw), &data); err != nil {
				return nil, fmt.Errorf("unmarshal resuspend signal %q/%q: %w", id, nodeName, err)
			}
			return &types.SignalPayload{Triggered: types.SignalReceived, Name: newSignalName, Data: data}, nil
		}
	}
	// Node is re-parked — register timeout in ZSET if spec has a timeout.
	if spec.Timeout > 0 {
		// Remove any old timeout entry first (signal name may have changed).
		if err := s.rdb.ZRem(ctx, timeoutZSetKey(t, id), timeoutMember(id, nodeName)).Err(); err != nil && err != redis.Nil {
			return nil, fmt.Errorf("clear resuspend timeout %q/%q: %w", id, nodeName, err)
		}
		if err := s.rdb.ZAdd(ctx, timeoutZSetKey(t, id), redis.Z{
			Score:  float64(time.Now().Add(spec.Timeout).Unix()),
			Member: timeoutMember(id, nodeName),
		}).Err(); err != nil {
			return nil, fmt.Errorf("register resuspend timeout %q/%q: %w", id, nodeName, err)
		}
	}
	// Extend TTL to prevent key expiry during suspension.
	if err := s.extendExecTTL(ctx, id, nodeName, spec, s.suspendTTL(ctx, id, spec)); err != nil {
		return nil, err
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Cancel support
// ---------------------------------------------------------------------------

func (s *Store) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	t := namespace.FromContext(ctx)
	return s.rdb.SMembers(ctx, suspendedNodesKey(t, id)).Result()
}

// cancelSuspendedNodeLua atomically transitions a node from Suspended to
// Canceled. It fails closed (returns 0, mutating nothing) when the node is no
// longer 'suspended' — e.g. a concurrent resume already moved it to 'running'
// and issued a fresh lease — so the caller never clobbers a live lease. When it
// does cancel, it removes the node from the suspended set and drops any stale
// lease-expiry index membership in the SAME command.
var cancelSuspendedNodeLua = redis.NewScript(`
local status = redis.call('GET', KEYS[1])
if status ~= 'suspended' then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SREM', KEYS[2], ARGV[3])
redis.call('ZREM', KEYS[3], ARGV[4])
return 1
`)

// CancelSuspendedNode implements engine.SuspendedNodeCanceler with a fenced
// compare-and-set: it cancels the node ONLY IF it is still Suspended, closing
// the read-then-write TOCTOU window a bare UpsertNode(Canceled) leaves open.
// Node status/output/meta are otherwise retained so a post-cancel Inspect can
// still show where the execution stopped; the execution-scoped waiter/timeout
// indexes are wiped later by cleanupOnCancel when the execution goes Canceled.
func (s *Store) CancelSuspendedNode(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(ctx, id)
	res, err := cancelSuspendedNodeLua.Run(ctx, s.rdb,
		[]string{
			nodeStatusKey(t, id, name),
			suspendedNodesKey(t, id),
			leaseExpiryZSetKey(t, id),
		},
		string(types.NodeStatusCanceled), int(ttl.Seconds()), name, leaseExpiryMember(id, name),
	).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("cancel suspended node %q/%q: %w", id, name, err)
	}
	canceled := res == 1
	if canceled && s.db != nil && !s.isTransient(ctx, id) {
		rec := &store.NodeRecord{
			ExecutionID: id,
			NodeName:    name,
			Status:      types.NodeStatusCanceled,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "cancel_suspended_node", func(ctx context.Context) error {
			return s.db.UpsertNode(ctx, rec)
		})
	}
	return canceled, nil
}

// cleanupOnCancel removes timeout ZSET entries for all suspended nodes of a
// canceled execution and deletes the suspended_nodes SET itself.
func (s *Store) cleanupOnCancel(ctx context.Context, id types.ExecutionID) {
	t := namespace.FromContext(ctx)
	nodes, err := s.rdb.SMembers(ctx, suspendedNodesKey(t, id)).Result()
	// Resolve the waiter/signal keys for each suspended node BEFORE opening the
	// pipeline: deriving signal-name-keyed keys requires reading each node's
	// stored waiter spec, which cannot be done inside a queued pipeline.
	var waiterKeys []string
	if err == nil {
		for _, name := range nodes {
			waiterKeys = append(waiterKeys, s.suspendedWaiterKeys(ctx, id, name)...)
		}
	}
	pipe := s.rdb.Pipeline()
	if err == nil {
		for _, name := range nodes {
			pipe.ZRem(ctx, timeoutZSetKey(t, id), timeoutMember(id, name))
		}
	}
	// Delete every waiter-related key for the canceled execution's suspended
	// nodes. Without this a signal delivered after cancellation would still find
	// a live waiter (waiterKey) / spec (waiterSpecKey) / batch (signalBatchKey)
	// and drive a ghost resume — writing a new resume outbox for an execution
	// that is already terminal.
	for _, key := range waiterKeys {
		pipe.Del(ctx, key)
	}
	// These indexes are execution-scoped. Once cancellation is authoritative,
	// no worker may recover work from them; removing them prevents stale leases
	// or undelivered outbox tasks from reviving a canceled execution.
	pipe.Del(ctx,
		suspendedNodesKey(t, id),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		remainingNodesKey(t, id),
		failedNodesKey(t, id),
		timeoutZSetKey(t, id),
	)
	_, _ = pipe.Exec(ctx)
}

// suspendedWaiterKeys returns every waiter/signal-related key owned by a single
// suspended node: the per-node waiter spec / signal batch / resume lock, plus
// the signal-name-keyed waiter and signal keys derived from its stored spec.
// nodeMeta and node status/output are intentionally retained so post-cancel
// Inspect can still surface where the execution stopped; none of the retained
// keys can trigger a resume once the waiter keys are gone.
func (s *Store) suspendedWaiterKeys(ctx context.Context, id types.ExecutionID, nodeName string) []string {
	t := namespace.FromContext(ctx)
	keys := []string{
		waiterSpecKey(t, id, nodeName),
		signalBatchKey(t, id, nodeName),
		resumeLockKey(t, id, nodeName),
	}
	if spec, err := s.loadWaiterSpec(ctx, id, nodeName); err == nil && spec != nil {
		for _, sigName := range spec.Signals {
			keys = append(keys, waiterKey(t, id, sigName), signalKey(t, id, sigName))
		}
	}
	return keys
}

// ---------------------------------------------------------------------------
// Output store
// ---------------------------------------------------------------------------
