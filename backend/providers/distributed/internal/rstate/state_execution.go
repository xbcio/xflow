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

	// Per-execution transient hint from workflow options (via submission context).
	// This must be checked before the global transient fallback so a per-workflow
	// transient execution gets the correct TTL even when the global mode is off.
	hint, perExecTransient := engine.ExecutionTransientFromContext(ctx)
	if !perExecTransient && e.Graph != nil && e.Graph.Transient() {
		// The graph is the source of truth for transience, exactly as it is on
		// the entry-admission path -- that path's own comment makes the argument
		// ("The graph is the source of truth here, not a context hint"), because
		// a Kafka trigger group's admission goes through neither Submit nor
		// Invoke and therefore carries no hint on its context.
		//
		// Relying on the hint alone left the two paths disagreeing about the
		// same execution: admission skipped the execution row because it read
		// the graph, while createExecution wrote no marker, so every later node
		// commit asked isTransient, found nothing, answered "durable" and
		// projected. That is the observed shape -- xflow_nodes rows with no
		// matching xflow_executions row -- and how transient traffic reached
		// SQL (xflow_nodes.output carries it by construction).
		perExecTransient = true
		hint = engine.TransientHint{
			TTL:           e.Graph.TransientTTL(),
			CompletionTTL: e.Graph.TransientCompletionTTL(),
		}
	}
	if perExecTransient {
		if hint.TTL > 0 {
			ttl = hint.TTL
		} else if s.transientTTL > 0 {
			ttl = s.transientTTL
		}
	}

	// Prime the transient verdict from the decision just made above.
	//
	// createExecution is authoritative for this execution: it knows whether the
	// graph asked to be transient. Without priming, the projection guard below
	// would ask isTransient, find no marker (none is written yet), and fall
	// through to the SQL confirmation -- which would report "no row" because
	// this very call has not created it yet, so every execution would be
	// misread as transient and silently stop being projected.
	//
	// Store-wide transient mode is folded in here: it writes no per-execution
	// marker at all, so priming "durable" for it would override the mode.
	effTransient := perExecTransient || s.transient
	s.rememberTransient(e.ID, transientMark{
		transient:        effTransient,
		ttl:              hint.TTL,
		completionTTL:    hint.CompletionTTL,
		durableConfirmed: !effTransient,
	})

	// Check for per-execution TTL override from context.
	if override, ok := engine.ExecutionTTLFromContext(ctx); ok {
		ttl = override
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = override
		s.ttlMu.Unlock()
	} else if !perExecTransient && s.transient && s.transientTTL > 0 {
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

	pipe := s.rdb.TxPipeline()
	t := namespace.FromContext(ctx)
	// The transient marker joins the same transaction as the structural keys.
	// Written afterwards, it would leave a window in which another replica sees
	// a running execution with no marker and resolves it as durable -- long
	// enough for the first node commit to project its payload into SQL.
	if perExecTransient {
		s.markExecutionTransient(ctx, pipe, e.ID, hint.TTL, hint.CompletionTTL, ttl)
	}
	// Persist the retention this execution was created with so
	// GetExecutionRetention can still answer with the TTL the writes actually
	// used after the process-local override is gone and, more generally, so
	// its absence remains a meaningful signal (see retentionRecordKey). Every
	// execution gets one, not only overridden ones: a record written only for
	// overrides would make an ordinary execution's absent record
	// indistinguishable from an expired override's, and GetExecutionRetention
	// treats absence as "window unknown".
	if recorded := s.recordedRetention(ttl); recorded.Milliseconds() > 0 {
		if recordTTL := s.retentionRecordTTL(recorded); recordTTL > 0 {
			pipe.Set(ctx, retentionRecordKey(t, e.ID), strconv.FormatInt(recorded.Milliseconds(), 10), recordTTL)
		}
	}

	var rec *store.ExecutionRecord
	if s.db != nil && !s.isTransient(ctx, e.ID) {
		now := time.Now()
		var recErr error
		rec, recErr = buildExecutionRecord(ctx, e, now)
		if recErr != nil {
			return recErr
		}
	}

	// Register the namespace in the discovery registry so maintenance loops
	// (sweeper, lease repair, outbox dispatcher, timeout monitor) SCAN its
	// namespace. Skipped in transient mode to preserve the fire-and-forget
	// no-bookkeeping invariant; the default namespace is always scanned anyway.
	if !s.isTransient(ctx, e.ID) {
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
	// Scope round-trips alongside Params and Runtime. No production path writes
	// it against this backend today -- a map body's sub-execution runs on an
	// embedded in-memory backend -- but a snapshot field that persists in one
	// backend and evaporates in the other is exactly the kind of divergence the
	// statestore contract suite exists to catch.
	if len(e.Scope) > 0 {
		scopeJSON, err := json.Marshal(e.Scope)
		if err != nil {
			return fmt.Errorf("marshal execution scope for %q: %w", e.ID, err)
		}
		pipe.Set(ctx, execKey(t, e.ID, "scope"), string(scopeJSON), ttl)
		keys = append(keys, execKey(t, e.ID, "scope"))
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
		for i := 0; i < e.Graph.UnitCount(); i++ {
			d := e.Graph.UnitInDegreeAt(i)
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
			// A root skip intent is fenced on its unit's "skip" scheduling
			// marker; it joins the same transaction so the intent is never
			// deliverable without it (see engine.UnselectedRootSkips).
			if entry.Task.Type == engine.TaskTypeNodeSkip {
				sk := scheduleKey(t, e.ID, entry.Task.UnitIdx)
				pipe.HSet(ctx, sk, "action", "skip")
				pipe.Expire(ctx, sk, ttl)
				keys = append(keys, sk)
			}
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
	if len(entries) > 0 {
		s.markOutboxReadyIndex(ctx, t, e.ID)
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
		transientMarkKey(t, e.ID),
		terminalMarkKey(t, e.ID),
		retentionRecordKey(t, e.ID),
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
		// Same superset argument as the TTL-shortening walk in state.go: the
		// counter keys are unit-indexed while this loop walks node indices, and
		// UnitCount <= NodeCount makes the node range cover every unit key.
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
	// The execution's ready set is gone, so its readiness-index member has to
	// go with it. Leaving it would only cost a no-op drain that repairs it
	// later; removing it here keeps "a member implies a ready set" true.
	s.refreshOutboxReadyIndex(ctx, t, e.ID)
	s.mu.Lock()
	delete(s.graphs, e.ID)
	s.mu.Unlock()
}

func buildExecutionRecord(ctx context.Context, e *engine.ExecutionSnapshot, now time.Time) (*store.ExecutionRecord, error) {
	rec := &store.ExecutionRecord{
		ExecutionID: e.ID,
		Namespace:   string(namespace.FromContext(ctx)),
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
	ttl := s.getExecTTL(ctx, id)
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
		// Order matters: the marker is read by the engine's inactive-execution
		// classifier after the status key is gone, and it must keep the active
		// retention — so it is written before evictExecutionCaches drops the
		// transient decision the marker's TTL resolution needs.
		s.markExecutionTerminalBestEffort(ctx, id, status)
		// Redis persists the graph for later inspection/reload; the in-process
		// cache and per-execution TTL override must not survive a terminal state.
		s.evictExecutionCaches(id)
	} else if applied == 1 {
		if err := s.refreshTransientTTL(ctx, id, keys...); err != nil {
			return err
		}
	}
	if applied == 1 && s.db != nil && !s.isTransient(ctx, id) {
		s.writeExecutionStatusProjection(ctx, id, status, errMsg)
	}
	_ = s.PublishExecutionEvent(ctx, engine.ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

// markExecutionTerminalBestEffort records the terminal marker that lets the
// engine's inactive-execution classifier tell a benign late delivery from lost
// work after the status key is gone.
//
// Only transient executions get one. A durable execution's status key already
// keeps the full active retention — nothing shortens it — so a marker there
// would duplicate, key for key, a record that expires at the same moment. A
// transient execution's status key is shortened to the completion TTL in the
// very transition that terminalizes it, and the marker is then the only record
// of the terminal outcome for the rest of the active retention. That window is
// exactly where the classification matters: a backlogged task can be consumed
// long after the completion TTL lapsed, and without the marker "no status"
// would read as "work never ran" on an execution that completed normally.
//
// Best effort by the same argument as shortenTransientCompletionTTLBestEffort:
// the terminal transition is already durable, so failing the caller would make
// it retry a transition that succeeded. The failure is logged rather than
// swallowed because losing the marker converts benign late deliveries into
// false loss verdicts once the backlog outlives the completion TTL.
func (s *Store) markExecutionTerminalBestEffort(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if !types.IsTerminalExecutionStatus(status) || !s.isTransient(ctx, id) {
		return
	}
	ttl := s.getExecTTL(ctx, id)
	if ttl <= 0 {
		return
	}
	t := namespace.FromContext(ctx)
	if err := s.rdb.Set(ctx, terminalMarkKey(t, id), string(status), ttl).Err(); err != nil && s.logger != nil {
		s.logger.Error("mark_execution_terminal_failed", "execution_id", string(id), "status", string(status), "err", err)
	}
}

// GetExecutionTerminalStatus implements engine.ExecutionTerminalReader: the
// terminal outcome recorded by markExecutionTerminalBestEffort, read after the
// status key itself may already have expired.
func (s *Store) GetExecutionTerminalStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	t := namespace.FromContext(ctx)
	val, err := s.rdb.Get(ctx, terminalMarkKey(t, id)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get execution terminal marker %q: %w", id, err)
	}
	return types.ExecutionStatus(val), true, nil
}

var _ engine.ExecutionTerminalReader = (*Store)(nil)

// retentionRecordTTLFactor is how much longer the persisted retention record
// lives than the longest retention any resolution could report.
//
// The record is written once, at creation, while the execution's evidence
// keys can keep being re-EXPIREd until its last state write, and a delivery
// can still be inside its window when it is consumed up to one retention
// after that write (the status key survives one retention past the write, and
// a task whose age is at most one retention is consumed no later than one
// further retention after that). The margin buys the record that one extra
// retention, on the same argument transientMarkerTTLFactor documents: a
// forgotten or absent refresh defers the record's expiry instead of letting
// it die under evidence it still describes. A longer-lived execution can
// still outlive its record first; absence then answers "unknown" and the
// classifier lands in unattributed, which is the deliberate under-report —
// never an over-stated window.
const retentionRecordTTLFactor = 2

// recordedRetention bounds the value the retention record may store for an
// execution created with `retention`: it is capped by the deployment default
// execTTL.
//
// A terminal write on a replica that does not hold the process-local override
// (or whose transient marker has already lapsed) resolves its TTL through
// getExecTTL and lands on execTTL, so a recorded value above it could name a
// window the terminal evidence never had. Under-stating the window is safe
// here — it only moves drops to unattributed; over-stating it is the
// false-loss shape the record exists to prevent.
func (s *Store) recordedRetention(retention time.Duration) time.Duration {
	if s.execTTL > 0 && s.execTTL < retention {
		return s.execTTL
	}
	return retention
}

// retentionRecordTTL sizes the persisted retention record's lifetime for an
// execution whose resolved retention is `retention`. It takes the longest TTL
// any resolution could report so the record cannot expire while a fallback
// value smaller than itself would still be wanted, then applies
// retentionRecordTTLFactor. A non-positive result means the store has no
// expiry at all and no record is written; absence then reads as "unknown".
func (s *Store) retentionRecordTTL(retention time.Duration) time.Duration {
	ttl := retention
	if s.execTTL > ttl {
		ttl = s.execTTL
	}
	if s.transientTTL > ttl {
		ttl = s.transientTTL
	}
	if ttl <= 0 {
		return 0
	}
	return ttl * retentionRecordTTLFactor
}

// GetExecutionRetention implements engine.ExecutionRetentionReader: the active
// retention that bounds how long any record of the execution — its status key
// or its terminal marker — can survive after its last write. The engine's
// inactive-execution classifier compares a delivery's queue wait against this
// window; beyond it, a task for an execution that finished long ago is
// indistinguishable from one for an execution that expired under its queued
// work, and the verdict becomes "unattributed" rather than a false loss
// report.
//
// Resolution order: the process-local per-execution override, then the
// retention record persisted at creation (written for every execution), then
// engine.ExecutionRetentionUnknown. There is deliberately no value fallback:
// the local map is deleted at terminalization and never survives a restart or
// a replica change, and once the record is gone too the TTL that actually
// bounded the writes cannot be confirmed — getExecTTL could report the longer
// global TTL for an execution whose override (or shorter transient TTL) is
// what its terminal evidence lived by, over-stating the window and turning a
// benign late duplicate into a reported loss. Unknown lands such deliveries
// in unattributed: still counted, still permanent, still logged, but not a
// loss claim. The method must never report a window larger than the one the
// status and marker writes actually used.
func (s *Store) GetExecutionRetention(ctx context.Context, id types.ExecutionID) (time.Duration, error) {
	s.ttlMu.RLock()
	override := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if override > 0 {
		return override, nil
	}
	t := namespace.FromContext(ctx)
	val, err := s.rdb.Get(ctx, retentionRecordKey(t, id)).Result()
	switch {
	case err == nil:
		ms, parseErr := strconv.ParseInt(val, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse persisted retention for %q: %w", id, parseErr)
		}
		if ms <= 0 {
			return 0, fmt.Errorf("persisted retention for %q is %d, want a positive duration", id, ms)
		}
		return time.Duration(ms) * time.Millisecond, nil
	case errors.Is(err, redis.Nil):
		// The record is the only durable statement of what this execution's
		// writes were bounded by, and it is gone (its own TTL passed, or the
		// execution predates the record). The fallback is not a lower bound on
		// that value, so report unknown rather than a window that may exceed
		// the evidence the writes left behind.
		return engine.ExecutionRetentionUnknown, nil
	default:
		return 0, fmt.Errorf("get execution retention %q: %w", id, err)
	}
}

var _ engine.ExecutionRetentionReader = (*Store)(nil)

// projectExecutionStatus mirrors an execution's terminal state onto the SQL
// audit trail. The atomic commit paths (commitNodeLua, commitGroupLua,
// seedExecutionFromEntryLua) finalize an execution inside their Lua script, so
// they never pass through UpdateExecutionStatus and this is their only route to
// the audit store. Best effort by contract (STORAGE-CONTRACT.md): Redis stays
// authoritative and a failed projection never fails the commit.
//
// errMsg is NOT guaranteed to be free of node output. engine/errorpolicy.go
// takes it from sysErr.Error() verbatim, so a node that formats a response body
// or an upstream output into its error writes that text here, and node output
// routinely contains credentials from upstream HTTP responses.
//
// Do not read "the built-in nodes keep payload out of Error()" as a fact about
// this repository. A sweep found three that did not: xflow.http rendered the
// whole request URL, query string included, on every transport failure (fixed);
// db_errors.go passes a raw *mysql.MySQLError through, and its duplicate-key
// text carries the offending column VALUE; grpc.go puts the remote-controlled
// st.Message() in Message rather than Details. The types.Error split (payload
// in Details, which Error() does not render) is the convention, and it is one
// this layer cannot check. The isTransient guard below is the part that is
// enforced.
//
// Details is no longer a write-only field, so the split above is not the escape
// hatch this comment used to imply. Since U-9 the engine projects
// ClassifiedError.Details onto the node snapshot and the inspect API serves it
// as NodeDetail.ErrorDetails — see the "Projection error text" section of
// doc.go. The difference that matters for THIS function is nil: errMsg remains
// the execution-level reason and still reaches the audit store unchanged,
// while the structured detail is withheld from any node whose output policy is
// private.
//
// That guard is load-bearing only on the GROUP commit path: state_commit.go
// already wraps its call in an outer !isTransient block, so a node-commit test
// cannot tell this check from that one. TestPerWorkflowTransient_
// SkipsGroupCommitStatusProjection drives CommitGroup for exactly that reason —
// deleting the check below reds it and nothing else.
func (s *Store) projectExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) {
	if s.db == nil || s.isTransient(ctx, id) || status == "" {
		return
	}
	s.writeExecutionStatusProjection(ctx, id, status, errMsg)
}

// writeExecutionStatusProjection mirrors an execution status onto the SQL audit
// trail, tolerating the one way that write is expected to fail.
//
// Both projection call sites route through here, and that is the point: the
// tolerance was first added to just one of them, and the other kept reporting
// the same divergence — 69 further "store: not found" lines in a run whose
// sibling site had already gone quiet. A shared helper is what keeps a rule
// about a write from having to be rediscovered per writer.
//
// store.ErrNotFound means there is no execution row to update, and for this
// projection that is never a store fault:
//
//   - a transient execution never gets a row at all — CreateExecution skips the
//     audit mirror for it — while a lapsed transient marker makes isTransient
//     answer "durable". This is the common case: transientTTL bounds run time,
//     and a run that outlives it loses its marker first. Measured on a real
//     deployment: 135 and 69 such lines in two runs.
//   - a late projection for an execution whose keys have already gone has
//     nothing left to mirror.
//
// Counting either as an audit failure is worse than useless: the audit trail is
// best-effort by contract (Redis is authoritative), and one error line per
// occurrence makes a healthy pipeline look like a broken store — which is how a
// genuine audit outage would be missed.
//
// Residual, stated rather than hidden: a durable execution whose row was
// genuinely deleted is indistinguishable here and is not reported. Nothing at
// this layer can tell it from the lapsed-marker case, because the marker is
// exactly the evidence that lapsed. Catching that is the job of the audit
// reconcile pass (§4), not of a per-write counter that cannot see the
// difference.
func (s *Store) writeExecutionStatusProjection(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) {
	err := s.db.UpdateExecutionStatus(ctx, id, status, errMsg)
	if errors.Is(err, store.ErrNotFound) {
		// Reported as OK rather than as neither outcome: the observer's question
		// is "did the audit trail diverge from Redis?", and here it did not —
		// there was simply nothing to mirror. Leaving it uncounted would make
		// ok+failed stop equalling the projections attempted, so a skip could
		// not be told from a write that never happened.
		s.audit.OnAuditOK(ctx, "update_execution_status")
		if s.auditCounters != nil {
			s.auditCounters.OnAuditOK(ctx, "update_execution_status")
		}
		if s.logger != nil {
			s.logger.Debug("update_execution_status skipped; no execution row to mirror",
				"execution_id", string(id), "status", string(status))
		}
		return
	}
	s.auditWrite(ctx, "update_execution_status", func(context.Context) error { return err })
}

// terminalExecutionError picks the reason to project for a terminal execution.
// The rule is shared with the local backend, so it lives in engine; this is a
// thin alias kept for call-site brevity.
func terminalExecutionError(status types.ExecutionStatus, nodeErr, cyclicErr string) string {
	return engine.TerminalExecutionError(status, nodeErr, cyclicErr)
}

// GetExecutionStatus reads back only the lifecycle status
// (engine.ExecutionStatusReader). It is the same authoritative key GetExecution
// reads first; everything GetExecution does after that key exists to fill in
// fields the activeness check never looks at.
//
// One GET where GetExecution issues eight. The saving is not the seven extra
// keys alone but the seven extra round trips: they are issued sequentially, not
// pipelined, so on a real network they serialize.
func (s *Store) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	t := namespace.FromContext(ctx)
	val, err := s.rdb.Get(ctx, execKey(t, id, "status")).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get execution status %q: %w", id, err)
	}
	return types.ExecutionStatus(val), true, nil
}

// GetExecutionStatuses answers the activeness question for a whole page of
// executions in one round trip.
//
// The status key is a single GET, so this is a pipeline of GETs rather than N
// sequential ones — the same saving GetExecution made for its eight keys, applied
// to the one key its callers here actually need. Both of them ask it about a page
// of assignments they have already loaded, where all but a few are expected to be
// dead, so answering per-execution charged the caller a full round trip each time
// to be told "gone".
//
// Positionally aligned with ids; an empty status means the execution has no status
// key, which is the absence GetExecutionStatus reports as found=false.
func (s *Store) GetExecutionStatuses(ctx context.Context, ids []types.ExecutionID) ([]types.ExecutionStatus, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, execKey(t, id, "status"))
	}
	// A missing key is the expected answer for a dead execution, not a failure:
	// redis.Nil is per-command and is read off each cmd below.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get execution statuses: %w", err)
	}
	out := make([]types.ExecutionStatus, len(ids))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get execution status %q: %w", ids[i], err)
		}
		out[i] = types.ExecutionStatus(val)
	}
	return out, nil
}

// GetExecution assembles a full snapshot from the eight per-field keys the
// execution is stored across. They are read in one pipeline: they are
// independent GETs of the same execution, so issuing them back to back — as
// this did — cost eight sequential network round trips to answer one call.
//
// It is worth the change because this is per-task traffic, not an occasional
// read: engine's buildInput calls it on every BuildTaskLease and
// RecoverTaskLease, and executionSubmissionContext calls it on every batch body.
//
// The absent-execution path now issues all eight commands where it used to stop
// after the status GET. That is seven extra COMMANDs on a path that was one
// round trip and still is; the returned snapshot is unchanged.
func (s *Store) GetExecution(ctx context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	statusCmd := pipe.Get(ctx, execKey(t, id, "status"))
	paramsCmd := pipe.Get(ctx, execKey(t, id, "params"))
	runtimeCmd := pipe.Get(ctx, execKey(t, id, "runtime"))
	scopeCmd := pipe.Get(ctx, execKey(t, id, "scope"))
	traceIDCmd := pipe.Get(ctx, execKey(t, id, "trace_id"))
	spanIDCmd := pipe.Get(ctx, execKey(t, id, "span_id"))
	carrierCmd := pipe.Get(ctx, execKey(t, id, "trace_carrier"))
	errorCmd := pipe.Get(ctx, execKey(t, id, "error"))
	// Exec surfaces the first command error, and redis.Nil is the expected answer
	// for most of these keys. The per-command results below are authoritative.
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get execution %q: %w", id, err)
	}
	val, err := statusCmd.Result()
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
	if raw, err := paramsCmd.Bytes(); err == nil {
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, fmt.Errorf("unmarshal execution params %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution params %q: %w", id, err)
	}
	var runtime *types.Runtime
	if raw, err := runtimeCmd.Bytes(); err == nil {
		var decoded types.Runtime
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("unmarshal execution runtime %q: %w", id, err)
		}
		runtime = &decoded
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution runtime %q: %w", id, err)
	}
	var scope map[string]any
	if raw, err := scopeCmd.Bytes(); err == nil {
		if err := json.Unmarshal(raw, &scope); err != nil {
			return nil, fmt.Errorf("unmarshal execution scope %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution scope %q: %w", id, err)
	}
	var traceID string
	if raw, err := traceIDCmd.Result(); err == nil {
		traceID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution trace ID %q: %w", id, err)
	}
	var spanID string
	if raw, err := spanIDCmd.Result(); err == nil {
		spanID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution span ID %q: %w", id, err)
	}
	var traceCarrier map[string]string
	if raw, err := carrierCmd.Bytes(); err == nil {
		if err := json.Unmarshal(raw, &traceCarrier); err != nil {
			return nil, fmt.Errorf("unmarshal execution trace carrier %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution trace carrier %q: %w", id, err)
	}
	// The error key is written by UpdateExecutionStatus and by commitNodeLua's
	// CyclicFinalError branch, but was never read back until now. Absent is the
	// normal case (every non-failed execution), hence the redis.Nil tolerance.
	var execErr string
	if raw, err := errorCmd.Result(); err == nil {
		execErr = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution error %q: %w", id, err)
	}
	return &engine.ExecutionSnapshot{
		ID:           id,
		Graph:        g,
		Status:       types.ExecutionStatus(val),
		Params:       params,
		Runtime:      runtime,
		Scope:        scope,
		TraceID:      traceID,
		SpanID:       spanID,
		TraceCarrier: traceCarrier,
		Error:        execErr,
	}, nil
}
