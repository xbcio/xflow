package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.OutboxFailureRecorder = (*Store)(nil)
var _ engine.OutboxMetricsReader = (*Store)(nil)

type redisOutboxEntry struct {
	ID          string      `json:"id"`
	Task        engine.Task `json:"task"`
	AutoDepth   int         `json:"auto_depth,omitempty"`
	Activation  int         `json:"activation_id,omitempty"`
	UnitIdx     *int        `json:"unit_idx,omitempty"`
	AvailableAt int64       `json:"available_at_ms,omitempty"`
	CreatedAt   int64       `json:"created_at_ms,omitempty"`
}

// ListOutbox returns ready entries for one execution. It leaves entries in
// Redis until AckOutbox so enqueue/ack response loss is retried safely.
func (s *Store) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	t := namespace.FromContext(ctx)
	ids, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: outboxReadyKey(t, id), Start: "-inf", Stop: fmt.Sprintf("%d", before.UnixMilli()), ByScore: true, Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list outbox %q: %w", id, err)
	}
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		raw, err := s.rdb.HGet(ctx, outboxBodyKey(t, id), entryID).Result()
		if err == redis.Nil {
			_ = ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(t, id), outboxBodyKey(t, id), outboxAttemptsKey(t, id)}, entryID).Err()
			continue
		}
		if err != nil {
			return out, fmt.Errorf("read outbox %q/%q: %w", id, entryID, err)
		}
		entry, err := unmarshalRedisOutboxEntry(raw)
		if err != nil {
			return out, fmt.Errorf("decode outbox %q/%q: %w", id, entryID, err)
		}
		out = append(out, entry)
	}
	return out, nil
}

// AckOutbox removes an already-enqueued entry atomically and idempotently.
func (s *Store) AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error {
	t := namespace.FromContext(ctx)
	if err := ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(t, id), outboxBodyKey(t, id), outboxAttemptsKey(t, id)}, entryID).Err(); err != nil {
		return fmt.Errorf("ack outbox %q/%q: %w", id, entryID, err)
	}
	return nil
}

// RecordOutboxFailure records a failed queue handoff and moves an intent to
// execution-scoped dead-letter storage after maxAttempts failures. It also
// writes compact node/activation metadata so later replay can guard against
// stale activations without parsing the entry body.
func (s *Store) RecordOutboxFailure(ctx context.Context, id types.ExecutionID, entry engine.OutboxEntry, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
	if maxAttempts <= 0 {
		maxAttempts = engine.DefaultOutboxMaxDeliveryAttempts
	}
	ttl := s.getExecTTL(id)
	t := namespace.FromContext(ctx)
	result, err := recordOutboxFailureLua.Run(ctx, s.rdb, []string{
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
		outboxReplayEntryIdxKey(t, id), // KEYS[7]: clear stale replay index on re-dead-letter
	}, entry.ID, maxAttempts, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		entry.Task.NodeName, entry.Task.ActivationID,
		deadLetterIntent(entry.ID), int(entry.Task.Type)).Slice()
	if err != nil {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: %w", id, entry.ID, err)
	}
	if len(result) != 2 {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: unexpected result %v", id, entry.ID, result)
	}
	failure := engine.OutboxDeliveryFailure{Attempts: int(redisResultInt(result[0])), DeadLettered: redisResultInt(result[1]) == 1}
	if err := s.refreshTransientTTL(ctx, id,
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
		outboxReplayEntryIdxKey(t, id),
	); err != nil {
		return engine.OutboxDeliveryFailure{}, err
	}
	return failure, nil
}

// OutboxMetrics scans durable pending and dead-letter indexes to provide
// aggregate backlog metrics. It is a recovery/observability path, not part of
// the task-delivery hot path.
func (s *Store) OutboxMetrics(ctx context.Context) (engine.OutboxMetricsSnapshot, error) {
	var snapshot engine.OutboxMetricsSnapshot
	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return engine.OutboxMetricsSnapshot{}, fmt.Errorf("list namespaces for outbox metrics: %w", err)
	}
	for _, t := range namespaces {
		if err := s.scanOutboxMetricsForTenant(ctx, t, &snapshot); err != nil {
			return engine.OutboxMetricsSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (s *Store) scanOutboxMetricsForTenant(ctx context.Context, t namespace.Namespace, snapshot *engine.OutboxMetricsSnapshot) error {
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan pending outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count pending outbox %q: %w", key, err)
			}
			snapshot.Pending += int(count)
			if count == 0 {
				continue
			}
			oldest, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
				Key: key, Start: "-inf", Stop: "+inf", ByScore: true, Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return fmt.Errorf("read pending outbox oldest score %q: %w", key, err)
			}
			if len(oldest) == 0 {
				continue
			}
			oldestAt := time.UnixMilli(int64(oldest[0].Score)).UTC()
			if !oldestAt.IsZero() && (snapshot.OldestPendingAt.IsZero() || oldestAt.Before(snapshot.OldestPendingAt)) {
				snapshot.OldestPendingAt = oldestAt
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	cursor = 0
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:dead"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan dead-letter outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count dead-letter outbox %q: %w", key, err)
			}
			snapshot.DeadLettered += int(count)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// ListOutboxExecutions scans execution-scoped ready indexes. The index is
// authoritative per execution; scanning is only a recovery discovery path.
func (s *Store) ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error) {
	if limit <= 0 {
		return nil, nil
	}
	ids := make(map[types.ExecutionID]struct{})
	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("list namespaces for outbox discovery: %w", err)
	}
	for _, t := range namespaces {
		if len(ids) >= limit {
			break
		}
		if err := s.scanOutboxExecutionsForTenant(ctx, t, limit, ids); err != nil {
			return nil, err
		}
	}
	out := make([]types.ExecutionID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *Store) scanOutboxExecutionsForTenant(ctx context.Context, t namespace.Namespace, limit int, ids map[types.ExecutionID]struct{}) error {
	var cursor uint64
	for len(ids) < limit {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan outbox indexes: %w", err)
		}
		for _, key := range keys {
			id, ok := executionIDFromKey(key)
			if ok {
				ids[id] = struct{}{}
			}
			if len(ids) >= limit {
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

func marshalRedisOutboxEntry(id string, task engine.Task, availableAt time.Time) (string, error) {
	entry := redisOutboxEntry{
		ID:          id,
		Task:        task,
		AutoDepth:   task.AutoDepth,
		Activation:  task.ActivationID,
		UnitIdx:     redisUnitIdxPtr(task.UnitIdx),
		AvailableAt: availableAt.UnixMilli(),
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	if availableAt.IsZero() {
		entry.AvailableAt = 0
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("marshal outbox %q: %w", id, err)
	}
	return string(data), nil
}

// redisUnitIdxPtr omits the wire field when the task's UnitIdx is the
// "unknown" sentinel, so absence on decode is distinguishable from a real
// unit index of 0. See engine.UnitIdxUnknown.
func redisUnitIdxPtr(unitIdx int) *int {
	if unitIdx == engine.UnitIdxUnknown {
		return nil
	}
	v := unitIdx
	return &v
}

func unmarshalRedisOutboxEntry(raw string) (engine.OutboxEntry, error) {
	var encoded redisOutboxEntry
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		return engine.OutboxEntry{}, err
	}
	encoded.Task.AutoDepth = encoded.AutoDepth
	encoded.Task.ActivationID = encoded.Activation
	if encoded.UnitIdx != nil {
		encoded.Task.UnitIdx = *encoded.UnitIdx
	} else {
		encoded.Task.UnitIdx = engine.UnitIdxUnknown
	}
	entry := engine.OutboxEntry{ID: encoded.ID, Task: encoded.Task}
	if encoded.AvailableAt > 0 {
		entry.AvailableAt = time.UnixMilli(encoded.AvailableAt).UTC()
	}
	if encoded.CreatedAt > 0 {
		entry.CreatedAt = time.UnixMilli(encoded.CreatedAt).UTC()
	}
	return entry, nil
}

func redisAdvanceOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("advance/%s/%s/%d", id, name, activationID)
}
func redisExecuteOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("execute/%s/%s/%d", id, name, activationID)
}
func redisSkipOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("skip/%s/%s/%d", id, name, activationID)
}
