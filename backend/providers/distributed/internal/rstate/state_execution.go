package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

func (s *Store) CreateExecution(ctx context.Context, e *engine.ExecutionSnapshot) error {
	return s.createExecution(ctx, e, nil)
}

// CreateExecutionWithOutbox commits execution metadata and its initial durable
// delivery intents in one Redis transaction. The SQL audit projection remains
// a best-effort follow-up and is not part of scheduling correctness.
func (s *Store) CreateExecutionWithOutbox(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	return s.createExecution(ctx, e, entries)
}

// createExecution persists the execution, its outbox entries, and the SQL audit
// projection. The Redis pipeline (including outbox entries scored available-now)
// commits before the SQL write; on SQL failure, cleanupCreatedExecution deletes
// the Redis keys including the outbox. There is a narrow (SQL-call-duration)
// window where the outbox dispatcher could pick up an entry and enqueue an asynq
// task that references an execution about to be cleaned up — that orphan task
// fails on dequeue and is retried/dead-lettered by asynq (self-healing, not
// data corruption). Keeping the outbox in the same pipeline is intentional: it
// is the common (no-SQL-failure) path that must never strand a root task.
func (s *Store) createExecution(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	ttl := s.execTTL

	// Check for per-execution TTL override from context.
	if override, ok := engine.ExecutionTTLFromContext(ctx); ok {
		ttl = override
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = override
		s.ttlMu.Unlock()
	} else if s.transient && s.transientTTL > 0 {
		// In transient mode the structural exec keys (:params/:runtime/:trace_id/
		// :span_id/:graph) are written once here and never re-EXPIREd by per-node
		// Lua. Set them to transientTTL directly so they outlive the run under the
		// documented constraint (transientTTL > max execution wall-clock), instead
		// of relying on a per-mutation refresh (see refreshTransientTTL).
		ttl = s.transientTTL
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

	pipe := s.rdb.TxPipeline()
	t := namespace.FromContext(ctx)
	// Register the namespace in the discovery registry so maintenance loops
	// (sweeper, lease repair, outbox dispatcher, timeout monitor) SCAN its
	// namespace. Skipped in transient mode to preserve the fire-and-forget
	// no-bookkeeping invariant; the default namespace is always scanned anyway.
	if !s.transient {
		// Non-fatal: the namespace is re-registered on the next durable write
		// and listNamespaces always includes the default namespace, so a
		// transient SADD failure cannot strand a namespace's keys outside the
		// sweeper.
		_ = s.registerNamespace(ctx, t)
	}
	keys := []string{execKey(t, e.ID, "status"), execKey(t, e.ID, "graph")}
	pipe.Set(ctx, execKey(t, e.ID, "status"), string(e.Status), ttl)
	pipe.Set(ctx, execKey(t, e.ID, "graph"), string(graphJSON), ttl)
	if e.Params != nil {
		paramsJSON, err := json.Marshal(e.Params)
		if err != nil {
			return fmt.Errorf("marshal execution params for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(t, e.ID, "params"), string(paramsJSON), ttl)
		keys = append(keys, execKey(t, e.ID, "params"))
	}
	if e.Runtime != nil {
		runtimeJSON, err := json.Marshal(e.Runtime)
		if err != nil {
			return fmt.Errorf("marshal execution runtime for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(t, e.ID, "runtime"), string(runtimeJSON), ttl)
		keys = append(keys, execKey(t, e.ID, "runtime"))
	}
	if e.TraceID != "" {
		pipe.Set(ctx, execKey(t, e.ID, "trace_id"), e.TraceID, ttl)
		keys = append(keys, execKey(t, e.ID, "trace_id"))
	}
	if e.SpanID != "" {
		pipe.Set(ctx, execKey(t, e.ID, "span_id"), e.SpanID, ttl)
		keys = append(keys, execKey(t, e.ID, "span_id"))
	}
	if len(e.TraceCarrier) > 0 {
		carrierJSON, err := json.Marshal(e.TraceCarrier)
		if err != nil {
			return fmt.Errorf("marshal execution trace carrier for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(t, e.ID, "trace_carrier"), string(carrierJSON), ttl)
		keys = append(keys, execKey(t, e.ID, "trace_carrier"))
	}
	// Acyclic executions use these counters as the O(1) completion source of
	// truth. Cyclic graphs retain their activation-based completion protocol.
	// UnitCount() is used instead of NodeCount() so that grouped nodes count as
	// a single unit — without this, single-group executions would never reach
	// remaining=0 (P0-2 fix).
	if e.Graph != nil && !e.Graph.AllowCycles() {
		pipe.Set(ctx, remainingNodesKey(t, e.ID), e.Graph.UnitCount(), ttl)
		pipe.Set(ctx, failedNodesKey(t, e.ID), 0, ttl)
		keys = append(keys, remainingNodesKey(t, e.ID), failedNodesKey(t, e.ID))
	}
	// Seed in-degree counters.
	if e.Graph != nil {
		for i := 0; i < e.Graph.NodeCount(); i++ {
			d := e.Graph.InDegreeAt(i)
			if d > 0 {
				pipe.Set(ctx, inDegreeKey(t, e.ID, i), d, ttl)
				keys = append(keys, inDegreeKey(t, e.ID, i))
			}
		}
	}
	if len(entries) > 0 {
		readyKey := outboxReadyKey(t, e.ID)
		bodyKey := outboxBodyKey(t, e.ID)
		availableNow := time.Now().UTC().UnixMilli()
		for _, entry := range entries {
			if entry.ID == "" {
				return fmt.Errorf("create execution %q: empty outbox entry ID", e.ID)
			}
			encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
			if err != nil {
				return fmt.Errorf("create execution %q outbox %q: %w", e.ID, entry.ID, err)
			}
			availableAt := availableNow
			if !entry.AvailableAt.IsZero() {
				availableAt = entry.AvailableAt.UTC().UnixMilli()
			}
			pipe.HSet(ctx, bodyKey, entry.ID, encoded)
			pipe.ZAdd(ctx, readyKey, redis.Z{Score: float64(availableAt), Member: entry.ID})
		}
		pipe.Expire(ctx, readyKey, ttl)
		pipe.Expire(ctx, bodyKey, ttl)
		keys = append(keys, readyKey, bodyKey)
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

func (s *Store) cleanupCreatedExecution(ctx context.Context, e *engine.ExecutionSnapshot) {
	s.ttlMu.Lock()
	delete(s.execTTLs, e.ID)
	s.ttlMu.Unlock()

	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx,
		execKey(t, e.ID, "status"),
		execKey(t, e.ID, "graph"),
		execKey(t, e.ID, "error"),
		execKey(t, e.ID, "params"),
		execKey(t, e.ID, "runtime"),
		execKey(t, e.ID, "trace_id"),
		execKey(t, e.ID, "span_id"),
		execKey(t, e.ID, "trace_carrier"),
		remainingNodesKey(t, e.ID),
		failedNodesKey(t, e.ID),
		leaseExpiryZSetKey(t, e.ID),
		outboxReadyKey(t, e.ID),
		outboxBodyKey(t, e.ID),
		outboxAttemptsKey(t, e.ID),
		outboxDeadKey(t, e.ID),
		outboxDeadBodyKey(t, e.ID),
		executionKeySetKey(t, e.ID),
		timeoutZSetKey(t, e.ID),
	)
	if e.Graph != nil {
		for i := 0; i < e.Graph.NodeCount(); i++ {
			node := e.Graph.NodeAt(i)
			pipe.Del(ctx,
				inDegreeKey(t, e.ID, i),
				activeInputsKey(t, e.ID, i),
				scheduleKey(t, e.ID, i),
				nodeStatusKey(t, e.ID, node.Name),
				nodeMetaKey(t, e.ID, node.Name),
				outputKey(t, e.ID, node.Name),
			)
		}
	}
	_, _ = pipe.Exec(ctx)
	s.mu.Lock()
	delete(s.graphs, e.ID)
	s.mu.Unlock()
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

func (s *Store) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	ttl := s.getExecTTL(id)
	t := namespace.FromContext(ctx)
	// Compare-and-set with cancel-aware fencing: a terminal or canceling status
	// blocks non-canceled overwrites, so a concurrent cyclic completeExecution
	// cannot stomp an in-flight Cancel.
	// Only include the error key in KEYS when non-empty to avoid CROSSSLOT errors
	// in Redis Cluster: an empty-string key lands on slot 0, which differs from
	// the {id}-tagged status key's slot.
	var keys []string
	if errMsg != "" {
		keys = []string{execKey(t, id, "status"), execKey(t, id, "error")}
	} else {
		keys = []string{execKey(t, id, "status")}
	}
	applied, err := updateExecutionStatusLua.Run(ctx, s.rdb,
		keys,
		string(status), errMsg, int(ttl.Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("update execution status %q: %w", id, err)
	}
	// Clean up timeout ZSET entries when execution is canceled.
	if applied == 1 && status == types.ExecutionStatusCanceled {
		s.cleanupOnCancel(ctx, id)
	}
	if applied == 1 && types.IsTerminalExecutionStatus(status) {
		if err := s.shortenTransientCompletionTTL(ctx, id, keys...); err != nil {
			return err
		}
		// Redis persists the graph for later inspection/reload; the in-process
		// cache and per-execution TTL override must not survive a terminal state.
		s.evictExecutionCaches(id)
	} else if applied == 1 {
		if err := s.refreshTransientTTL(ctx, id, keys...); err != nil {
			return err
		}
	}
	if applied == 1 && s.db != nil && !s.transient {
		s.auditWrite(ctx, "update_execution_status", func(ctx context.Context) error {
			return s.db.UpdateExecutionStatus(ctx, id, status, errMsg)
		})
	}
	_ = s.PublishExecutionEvent(ctx, engine.ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

func (s *Store) GetExecution(ctx context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	t := namespace.FromContext(ctx)
	val, err := s.rdb.Get(ctx, execKey(t, id, "status")).Result()
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
	if raw, err := s.rdb.Get(ctx, execKey(t, id, "params")).Bytes(); err == nil {
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, fmt.Errorf("unmarshal execution params %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution params %q: %w", id, err)
	}
	var runtime *types.Runtime
	if raw, err := s.rdb.Get(ctx, execKey(t, id, "runtime")).Bytes(); err == nil {
		var decoded types.Runtime
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("unmarshal execution runtime %q: %w", id, err)
		}
		runtime = &decoded
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution runtime %q: %w", id, err)
	}
	var traceID string
	if raw, err := s.rdb.Get(ctx, execKey(t, id, "trace_id")).Result(); err == nil {
		traceID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution trace ID %q: %w", id, err)
	}
	var spanID string
	if raw, err := s.rdb.Get(ctx, execKey(t, id, "span_id")).Result(); err == nil {
		spanID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution span ID %q: %w", id, err)
	}
	var traceCarrier map[string]string
	if raw, err := s.rdb.Get(ctx, execKey(t, id, "trace_carrier")).Bytes(); err == nil {
		if err := json.Unmarshal(raw, &traceCarrier); err != nil {
			return nil, fmt.Errorf("unmarshal execution trace carrier %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution trace carrier %q: %w", id, err)
	}
	return &engine.ExecutionSnapshot{
		ID:           id,
		Graph:        g,
		Status:       types.ExecutionStatus(val),
		Params:       params,
		Runtime:      runtime,
		TraceID:      traceID,
		SpanID:       spanID,
		TraceCarrier: traceCarrier,
	}, nil
}
