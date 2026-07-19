package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

func (s *Store) AcquireTaskLease(ctx context.Context, lease *engine.TaskLease) (previous *engine.NodeSnapshot, acquired bool, err error) {
	started := time.Now()
	defer func() {
		result := "acquired"
		if err != nil {
			result = "error"
		} else if !acquired {
			result = "rejected"
		}
		s.observeLeaseAcquire(ctx, result, time.Since(started))
	}()

	ttl := s.getExecTTL(lease.Task.ExecutionID)
	t := tenant.FromContext(ctx)
	payloadJSON := ""
	if lease.Task.Payload != nil {
		encoded, err := json.Marshal(lease.Task.Payload)
		if err != nil {
			return nil, false, fmt.Errorf("marshal lease payload %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		payloadJSON = string(encoded)
	}
	result, err := acquireTaskLeaseLua.Run(ctx, s.rdb,
		[]string{
			nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
			nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
			leaseExpiryZSetKey(t, lease.Task.ExecutionID),
		},
		string(lease.LeaseID), string(lease.LeaseToken), lease.IssuedAt.UnixMilli(), int(ttl.Seconds()), lease.Task.ActivationID, lease.Task.AutoDepth, lease.TTL.Milliseconds(), leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName), int(lease.Task.Type), payloadJSON, lease.Task.NodeIdx,
	).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("acquire task lease %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(result) != 8 {
		return nil, false, fmt.Errorf("acquire task lease %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, result)
	}

	asInt64 := func(v any) int64 {
		switch n := v.(type) {
		case int64:
			return n
		case string:
			var parsed int64
			if _, err := fmt.Sscanf(n, "%d", &parsed); err == nil {
				return parsed
			}
		}
		return 0
	}
	asString := func(v any) string {
		if s, ok := v.(string); ok {
			return s
		}
		return ""
	}

	acquired = asInt64(result[0]) == 1
	prevStatus := asString(result[1])
	var prev *engine.NodeSnapshot
	if prevStatus != "" {
		prev = &engine.NodeSnapshot{
			ExecutionID:  lease.Task.ExecutionID,
			Name:         lease.Task.NodeName,
			NodeIdx:      lease.Task.NodeIdx,
			Status:       types.NodeStatus(prevStatus),
			Attempt:      int(asInt64(result[2])),
			ActivationID: int(asInt64(result[3])),
			AutoDepth:    int(asInt64(result[4])),
			LeaseToken:   engine.LeaseToken(asString(result[5])),
		}
		if ms := asInt64(result[6]); ms > 0 {
			prev.LeaseIssuedAt = time.UnixMilli(ms).UTC()
		}
		if ms := asInt64(result[7]); ms > 0 {
			prev.LeaseTTL = time.Duration(ms) * time.Millisecond
		}
	}
	if !acquired {
		return prev, false, nil
	}

	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
	); err != nil {
		return nil, false, err
	}

	if s.db != nil {
		attempt := 1
		if prev != nil {
			attempt = prev.Attempt + 1
		}
		rec := &store.NodeRecord{
			ExecutionID: lease.Task.ExecutionID,
			NodeName:    lease.Task.NodeName,
			Status:      types.NodeStatusRunning,
			LeaseID:     string(lease.LeaseID),
			LeaseToken:  string(lease.LeaseToken),
			Attempt:     attempt,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "acquire_task_lease", func(ctx context.Context) error {
			return s.db.UpsertNode(ctx, rec)
		})
	}

	return prev, true, nil
}

// leaseIndexBatchLimit caps a single ListExpiredLeases scan. Small enough that
// the sweeper stays quick under heavy backlog; the sweeper re-polls until the
// list drains, so this is not a coverage cap, only a per-call bound.
const leaseIndexBatchLimit = 256

func (s *Store) ListExpiredLeases(ctx context.Context, before time.Time) (expired []engine.ExpiredLease, err error) {
	started := time.Now()
	defer func() {
		s.observeLeaseExpiryScan(ctx, len(expired), time.Since(started), err)
	}()

	const scanCount = int64(128)

	max := fmt.Sprintf("%d", before.UnixMilli())
	out := make([]engine.ExpiredLease, 0, leaseIndexBatchLimit)
	seenIndexes := make(map[string]struct{})

	tenants, err := s.listTenants(ctx)
	if err != nil {
		return out, fmt.Errorf("list tenants for lease scan: %w", err)
	}
	for _, t := range tenants {
		if len(out) >= leaseIndexBatchLimit {
			break
		}
		if err := s.scanExpiredLeasesForTenant(ctx, t, before, max, scanCount, seenIndexes, &out); err != nil {
			return out, err
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *Store) scanExpiredLeasesForTenant(ctx context.Context, t tenant.TenantID, before time.Time, max string, scanCount int64, seenIndexes map[string]struct{}, out *[]engine.ExpiredLease) error {
	var cursor uint64
	for len(*out) < leaseIndexBatchLimit {
		indexKeys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "leases"), scanCount).Result()
		if err != nil {
			return fmt.Errorf("scan lease indexes: %w", err)
		}
		for _, indexKey := range indexKeys {
			if _, seen := seenIndexes[indexKey]; seen {
				continue
			}
			seenIndexes[indexKey] = struct{}{}
			indexTenant, indexExecID, validIndex := parseTenantExecKey(indexKey)
			if !validIndex || indexTenant != t {
				continue
			}

			remaining := leaseIndexBatchLimit - len(*out)
			members, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key: indexKey, Start: "-inf", Stop: max, ByScore: true, Offset: 0, Count: int64(remaining),
			}).Result()
			if err != nil {
				return fmt.Errorf("list expired leases for %q: %w", indexExecID, err)
			}
			for _, member := range members {
				execID, nodeName, ok := splitLeaseMember(member)
				if !ok || execID != indexExecID {
					if err := s.rdb.ZRem(ctx, indexKey, member).Err(); err != nil {
						return fmt.Errorf("prune malformed lease index member %q: %w", member, err)
					}
					continue
				}

				status, err := s.rdb.Get(ctx, nodeStatusKey(t, execID, nodeName)).Result()
				if err == redis.Nil || (err == nil && status != string(types.NodeStatusRunning) && status != string(types.NodeStatusCommitting) && status != string(types.NodeStatusWaiting)) {
					if removeErr := s.rdb.ZRem(ctx, indexKey, member).Err(); removeErr != nil {
						return fmt.Errorf("prune stale lease %q/%q: %w", execID, nodeName, removeErr)
					}
					continue
				}
				if err != nil {
					return fmt.Errorf("read node status %q/%q: %w", execID, nodeName, err)
				}

				meta, err := s.rdb.HGetAll(ctx, nodeMetaKey(t, execID, nodeName)).Result()
				if err != nil {
					return fmt.Errorf("read node meta %q/%q: %w", execID, nodeName, err)
				}
				if meta["lease_token"] == "" {
					if removeErr := s.rdb.ZRem(ctx, indexKey, member).Err(); removeErr != nil {
						return fmt.Errorf("prune tokenless lease %q/%q: %w", execID, nodeName, removeErr)
					}
					continue
				}

				var deadlineMs, issuedAtMs, leaseTTLms int64
				parseInt64(meta["lease_deadline_ms"], func(value int64) { deadlineMs = value })
				parseInt64(meta["lease_issued_at_ms"], func(value int64) { issuedAtMs = value })
				parseInt64(meta["lease_ttl_ms"], func(value int64) { leaseTTLms = value })
				if deadlineMs <= 0 && issuedAtMs > 0 && leaseTTLms > 0 {
					// Compatibility with leases written before lease_deadline_ms was
					// introduced. The repaired index is then based on the same
					// durable metadata, not its prior ZSET score.
					deadlineMs = issuedAtMs + leaseTTLms
				}
				if deadlineMs > before.UnixMilli() {
					if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: float64(deadlineMs), Member: member}).Err(); err != nil {
						return fmt.Errorf("repair lease index %q/%q: %w", execID, nodeName, err)
					}
					continue
				}

				lease := engine.ExpiredLease{
					ExecutionID: execID,
					NodeName:    nodeName,
					LeaseID:     engine.LeaseID(meta["lease_id"]),
					LeaseToken:  engine.LeaseToken(meta["lease_token"]),
				}
				if issuedAtMs > 0 {
					lease.IssuedAt = time.UnixMilli(issuedAtMs).UTC()
				}
				if leaseTTLms > 0 {
					lease.TTL = time.Duration(leaseTTLms) * time.Millisecond
				}
				parseInt64(meta["node_idx"], func(value int64) { lease.NodeIdx = int(value) })
				parseInt64(meta["activation_id"], func(value int64) { lease.ActivationID = int(value) })
				parseInt64(meta["auto_depth"], func(value int64) { lease.AutoDepth = int(value) })
				parseInt64(meta["lease_task_type"], func(value int64) { lease.TaskType = engine.TaskType(value) })
				if rawPayload := meta["lease_payload"]; rawPayload != "" {
					var payload types.SignalPayload
					if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
						return fmt.Errorf("decode expired lease payload %q/%q: %w", execID, nodeName, err)
					}
					lease.Payload = &payload
				}
				*out = append(*out, lease)
				if len(*out) == leaseIndexBatchLimit {
					break
				}
			}
			if len(*out) == leaseIndexBatchLimit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (s *Store) RevokeLease(ctx context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	if token == "" {
		return false, nil
	}
	ttl := s.getExecTTL(id)
	t := tenant.FromContext(ctx)
	result, err := revokeLeaseLua.Run(ctx, s.rdb,
		[]string{nodeStatusKey(t, id, name), nodeMetaKey(t, id, name), leaseExpiryZSetKey(t, id)},
		string(token), int(ttl.Seconds()), leaseExpiryMember(id, name),
	).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease %q/%q: %w", id, name, err)
	}
	if result == 1 {
		if err := s.refreshTransientTTL(ctx, id, nodeStatusKey(t, id, name), nodeMetaKey(t, id, name), leaseExpiryZSetKey(t, id)); err != nil {
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

func (s *Store) ClaimTaskLease(ctx context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	t := tenant.FromContext(ctx)
	if s.transient {
		execStatus, err := s.rdb.Get(ctx, execKey(t, lease.Task.ExecutionID, "status")).Result()
		if err != nil && err != redis.Nil {
			return nil, false, fmt.Errorf("claim task lease %q/%q: get execution status: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		if types.IsTerminalExecutionStatus(types.ExecutionStatus(execStatus)) {
			return &engine.NodeSnapshot{
				ExecutionID:  lease.Task.ExecutionID,
				Name:         lease.Task.NodeName,
				NodeIdx:      lease.Task.NodeIdx,
				Status:       types.NodeStatusCanceled,
				ActivationID: lease.Task.ActivationID,
				AutoDepth:    lease.Task.AutoDepth,
			}, true, nil
		}
	}

	ttl := s.getExecTTL(lease.Task.ExecutionID)
	result, err := claimTaskLeaseLua.Run(ctx, s.rdb,
		[]string{
			nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
			nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
			leaseExpiryZSetKey(t, lease.Task.ExecutionID),
		},
		string(lease.LeaseToken), int(ttl.Seconds()), lease.Task.ActivationID, leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName),
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
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID,
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
	); err != nil {
		return nil, false, err
	}
	return ns, true, nil
}

// SuspendTaskLease atomically converts one committing lease into a suspended
// node while preserving the signal rendezvous semantics for ordinary and
// multi-signal waits. A stale claimant returns committed=false and cannot
// consume signals or overwrite a recovered lease.
func (s *Store) SuspendTaskLease(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (*types.SignalPayload, bool, error) {
	if s.transient {
		// See SuspendOrConsume: transient mode never parks a waiter.
		return nil, false, engine.ErrSuspendUnsupported
	}
	if lease == nil || spec == nil {
		return nil, false, engine.ErrInvalidLeaseToken
	}
	outputJSON := ""
	if storeOutput {
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, false, fmt.Errorf("marshal suspend output %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		outputJSON = string(encoded)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, false, fmt.Errorf("marshal suspend spec %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	multi := 0
	if spec.Mode == types.ModeMultiSignal {
		multi = 1
	}
	oldWaiter := oldSignalName
	if oldWaiter == "" {
		oldWaiter = "__none__"
	}
	t := tenant.FromContext(ctx)
	keys := []string{
		nodeStatusKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		nodeMetaKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		outputKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		leaseExpiryZSetKey(t, lease.Task.ExecutionID),
		suspendedNodesKey(t, lease.Task.ExecutionID),
		resumeLockKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		waiterKey(t, lease.Task.ExecutionID, oldWaiter),
		waiterSpecKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
		signalBatchKey(t, lease.Task.ExecutionID, lease.Task.NodeName),
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, signalKey(t, lease.Task.ExecutionID, signalName))
	}
	for _, signalName := range spec.Signals {
		keys = append(keys, waiterKey(t, lease.Task.ExecutionID, signalName))
	}
	store := 0
	if storeOutput {
		store = 1
	}
	args := []any{
		string(lease.LeaseID), string(lease.LeaseToken), lease.Attempt, lease.Task.ActivationID,
		int(s.getExecTTL(lease.Task.ExecutionID).Seconds()), leaseExpiryMember(lease.Task.ExecutionID, lease.Task.NodeName),
		store, outputJSON, lease.Task.NodeName, multi, signalQuorum(spec), len(spec.Signals), string(specJSON),
	}
	for _, signalName := range spec.Signals {
		args = append(args, signalName)
	}
	result, err := suspendTaskLeaseLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("suspend task lease %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(result) != 4 {
		return nil, false, fmt.Errorf("suspend task lease %q/%q: unexpected result %v", lease.Task.ExecutionID, lease.Task.NodeName, result)
	}
	if redisResultInt(result[0]) != 1 {
		return nil, false, nil
	}
	if err := s.refreshTransientTTL(ctx, lease.Task.ExecutionID, keys...); err != nil {
		return nil, false, err
	}
	if err := s.extendExecTTL(ctx, lease.Task.ExecutionID, lease.Task.NodeName, spec, s.suspendTTL(lease.Task.ExecutionID, spec)); err != nil {
		return nil, false, err
	}
	name := redisResultString(result[1])
	raw := redisResultString(result[2])
	if name == "" || raw == "" {
		return nil, true, nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, false, fmt.Errorf("decode suspend payload %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	payload := &types.SignalPayload{Triggered: types.SignalReceived, Name: name, Data: data}
	if allJSON := redisResultString(result[3]); allJSON != "" {
		var encodedAll map[string]string
		if err := json.Unmarshal([]byte(allJSON), &encodedAll); err != nil {
			return nil, false, fmt.Errorf("decode suspend payload set %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
		}
		payload.All = make(map[string]map[string]any, len(encodedAll))
		for signalName, signalJSON := range encodedAll {
			var signalData map[string]any
			if err := json.Unmarshal([]byte(signalJSON), &signalData); err != nil {
				return nil, false, fmt.Errorf("decode suspend signal %q/%q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, signalName, err)
			}
			payload.All[signalName] = signalData
		}
	}
	return payload, true, nil
}

// ---------------------------------------------------------------------------
// Scheduling counters
// ---------------------------------------------------------------------------
