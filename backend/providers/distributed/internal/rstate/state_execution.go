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
	return s.createExecution(ctx, e, nil, time.Time{}, 0)
}

// CreateExecutionWithOutbox commits execution metadata and its initial durable
// delivery intents in one Redis transaction. The SQL audit projection remains
// a best-effort follow-up and is not part of scheduling correctness.
func (s *Store) CreateExecutionWithOutbox(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	return s.createExecution(ctx, e, entries, time.Time{}, 0)
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
//
// holdUntil parks every entry of this execution: the bodies and skip markers are
// written as usual, but the ready ZSET scores them far in the future, so no
// delivery path and no discovery index considers them work. It is how a QUEUE
// mode submission is persisted without being startable, and it is deliberately
// a property of the entry's SCORE rather than of its body: the body's
// availability stays the caller's own, so once promotion scores the entry back
// to now, what the store hands the dispatcher says "available" in both places.
//
// queueWait is the execution's declared queue_timeout, and it is added to — not
// substituted for — whatever retention this execution would otherwise get. A
// parked execution's keys are written once and then sit untouched until
// promotion, so a retention that does not cover the wait expires them mid-queue:
// the execution disappears while its place in line remains, and the line then
// spends a promotion on nothing. The extension is recorded as the execution's
// own retention so later writers, which include the promotion's own status
// refresh, do not shorten it back.
func (s *Store) createExecution(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry, holdUntil time.Time, queueWait time.Duration) error {
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
	// would ask isTransient before this call's pipeline has run -- so Redis has
	// no marker yet -- and fall through to the SQL confirmation, which would
	// report "no row" because this very call has not created it yet. Every
	// execution would then be misread as transient and silently stop being
	// projected.
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

	// The queue wait is added last, after every other rule has had its say, so
	// it lengthens whatever retention the execution actually got — a transient
	// execution's short TTL included. A zero retention means unbounded in this
	// store, and unbounded plus a wait is still unbounded.
	if queueWait > 0 && ttl > 0 {
		ttl += queueWait
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = ttl
		s.ttlMu.Unlock()
	}

	// The time budget is a retention rule too: the keys have to outlive the
	// deadline, or the status expires before the expiry that the deadline exists
	// to trigger can act on it. Applied after the queue wait so it only ever
	// lengthens, and recorded like the wait so a later writer does not shorten it.
	if want := retentionPastDeadline(ttl, e.Deadline); want > ttl {
		ttl = want
		s.ttlMu.Lock()
		s.execTTLs[e.ID] = ttl
		s.ttlMu.Unlock()
	}

	// Serialize graph for persistence (allows recovery after queue consumer restart).
	graphJSON, err := json.Marshal(e.Graph)
	if err != nil {
		return fmt.Errorf("marshal graph for %q: %w", e.ID, err)
	}

	pipe := s.rdb.TxPipeline()
	t := namespace.FromContext(ctx)
	// The mode marker joins the same transaction as the structural keys. Written
	// afterwards, it would leave a window in which another replica sees a
	// running execution with no marker, has to fall back to SQL, and finds no
	// row -- this very call writes that row only after the pipeline commits.
	//
	// Every execution gets a marker, not just transient ones. "No marker" is
	// also the shape of a lapsed transient marker, so it cannot be read as
	// "durable"; and a bare SQL miss cannot be read as "transient", because the
	// projection is a best-effort follow-up, not an admission witness.
	if perExecTransient {
		s.markExecutionTransient(ctx, pipe, e.ID, hint.TTL, hint.CompletionTTL, ttl)
	} else if !s.transient {
		// Store-wide transient mode writes no per-execution marker at all: the
		// flag is the answer on every replica, and a durable marker here would
		// outlive a runtime reconfiguration as a false verdict.
		s.markExecutionDurable(ctx, pipe, e.ID, ttl)
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
	// The execution's own clock. Both keys are written with the execution so a
	// reader can time a run — GET /v1/executions/{id} exposes them as
	// created_at/updated_at, and §6.8 derives a terminal run's duration from
	// them. They live in Redis (not only in the SQL projection) because inspect
	// runs against the state store, including on deployments with no SQL at
	// all. updated_at_ms is rewritten by every accepted status transition.
	createdAtMs := time.Now().UTC().UnixMilli()
	createdKey := execKey(t, e.ID, "created_at_ms")
	updatedKey := execKey(t, e.ID, "updated_at_ms")
	pipe.Set(ctx, createdKey, createdAtMs, ttl)
	pipe.Set(ctx, updatedKey, createdAtMs, ttl)
	keys = append(keys, createdKey, updatedKey)
	// The execution's time budget, written only when it has one: an absent key
	// reads back as a zero Deadline, which is "no budget" (see GetExecution).
	// Immutable like the identity keys below, so it is written once, here.
	if !e.Deadline.IsZero() {
		pipe.Set(ctx, execKey(t, e.ID, "deadline_ms"), e.Deadline.UnixMilli(), ttl)
		keys = append(keys, execKey(t, e.ID, "deadline_ms"))
	}
	// The registration namespace rides with the execution's immutable metadata.
	// Dispatch reads it back because the queue worker's ctx starts from
	// context.Background() and names the deployment, not the registration.
	if e.Namespace != "" {
		pipe.Set(ctx, execKey(t, e.ID, "namespace"), e.Namespace, ttl)
	}
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
	// TestRun gates pin_data under settings.pin_data_mode: test_only, so it must
	// survive the round trip: a lost flag silently runs pinned nodes for real.
	if e.TestRun {
		pipe.Set(ctx, execKey(t, e.ID, "test_run"), "1", ttl)
		keys = append(keys, execKey(t, e.ID, "test_run"))
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
	// The execution this one derives from, and — for a rerun — the node it starts
	// at. Immutable, written once with the execution itself: the link is what
	// makes a derived execution traceable after the execution it derives from
	// has expired, and a reader that found the execution without it could not
	// tell a derived run from an original one.
	if e.ParentID != "" {
		pipe.Set(ctx, execKey(t, e.ID, "parent"), string(e.ParentID), ttl)
		keys = append(keys, execKey(t, e.ID, "parent"))
	}
	// The node of the parent execution that started this child, and the child's
	// nesting depth. Immutable for the same reason the parent link is: they are
	// what makes a sub-workflow child distinguishable from a rerun (which
	// carries ParentID and RerunFrom, never ParentNode) and what the runtime
	// depth bound reads on the next nest level.
	if e.ParentNode != "" {
		pipe.Set(ctx, execKey(t, e.ID, "parent_node"), e.ParentNode, ttl)
		keys = append(keys, execKey(t, e.ID, "parent_node"))
	}
	if e.Depth > 0 {
		pipe.Set(ctx, execKey(t, e.ID, "depth"), strconv.Itoa(e.Depth), ttl)
		keys = append(keys, execKey(t, e.ID, "depth"))
	}
	if e.RerunFrom != "" {
		pipe.Set(ctx, execKey(t, e.ID, "rerun_from"), e.RerunFrom, ttl)
		keys = append(keys, execKey(t, e.ID, "rerun_from"))
	}
	// The workflow identity captured at submission. Immutable, like the links
	// above: a reader that found the execution must be able to name the
	// definition it ran even after that definition (or version) is gone. Empty
	// values write nothing and read back empty, which is the documented
	// unattributed state.
	if e.WorkflowID != "" {
		pipe.Set(ctx, execKey(t, e.ID, "workflow_id"), string(e.WorkflowID), ttl)
		keys = append(keys, execKey(t, e.ID, "workflow_id"))
	}
	if e.WorkflowVersion != "" {
		pipe.Set(ctx, execKey(t, e.ID, "workflow_version"), e.WorkflowVersion, ttl)
		keys = append(keys, execKey(t, e.ID, "workflow_version"))
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
		held := int64(0)
		if !holdUntil.IsZero() {
			held = holdUntil.UTC().UnixMilli()
		}
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
			if held != 0 {
				availableAt = held
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
	// Register the deadline before the transaction makes the execution visible:
	// an execution with a deadline and no index entry is one the sweep can never
	// find. A failure here fails the creation, which nothing has been written for
	// yet (execution_deadline_index.go has the full argument).
	if err := s.registerExecutionDeadline(ctx, t, e.ID, e.Deadline, false); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create execution %q: %w", e.ID, err)
	}
	// The deadline just written is remembered so this replica's first getExecTTL
	// does not read it back; a zero one is remembered too, as "no time budget".
	s.rememberExecDeadline(e.ID, e.Deadline)
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
	s.graphs.Put(e.ID, e.Graph, e.Graph.EstimatedHeapBytes())
	return nil
}

func (s *Store) cleanupCreatedExecution(ctx context.Context, e *engine.ExecutionSnapshot) {
	s.ttlMu.Lock()
	delete(s.execTTLs, e.ID)
	delete(s.execDeadlines, e.ID)
	s.ttlMu.Unlock()

	t := namespace.FromContext(ctx)
	// Every key creation wrote, plus keys later transitions or caches may have
	// added before the SQL failure was observed. All of them are execution-scoped:
	// the namespace registry and the workflow-scoped outbox readiness index are
	// maintained separately (refreshOutboxReadyIndex below), never by this
	// rollback, so a failed create cannot perturb another execution's state.
	keys := createdExecutionKeys(t, e.ID, e.Graph)
	keys = append(keys,
		execKey(t, e.ID, "error"),
		terminalMarkKey(t, e.ID),
		leaseExpiryZSetKey(t, e.ID),
		outboxAttemptsKey(t, e.ID),
		outboxDeadKey(t, e.ID),
		outboxDeadBodyKey(t, e.ID),
		executionKeySetKey(t, e.ID),
		timeoutZSetKey(t, e.ID),
	)
	if e.Graph != nil {
		// Node keys can already exist when the SQL write fails: the outbox
		// dispatcher may have picked an entry up during the SQL-call window. They
		// are execution-scoped like everything above.
		for i := 0; i < e.Graph.NodeCount(); i++ {
			node := e.Graph.NodeAt(i)
			keys = append(keys,
				activeInputsKey(t, e.ID, i),
				nodeStatusKey(t, e.ID, node.Name),
				nodeMetaKey(t, e.ID, node.Name),
				outputKey(t, e.ID, node.Name),
			)
		}
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, keys...)
	_, _ = pipe.Exec(ctx)
	// The execution's ready set is gone, so its readiness-index member has to
	// go with it. Leaving it would only cost a no-op drain that repairs it
	// later; removing it here keeps "a member implies a ready set" true.
	s.refreshOutboxReadyIndex(ctx, t, e.ID)
	s.graphs.Delete(e.ID)
}

func buildExecutionRecord(ctx context.Context, e *engine.ExecutionSnapshot, now time.Time) (*store.ExecutionRecord, error) {
	rec := &store.ExecutionRecord{
		ExecutionID: e.ID,
		Namespace:   string(namespace.FromContext(ctx)),
		Status:      e.Status,
		TriggerKind: engine.ExecutionTriggerKind(e),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	// NodeTotal is the compiled graph's size at creation — the denominator of a
	// list row's node progress. A nil graph (a seed with no compiled body) stays
	// 0 = UNKNOWN rather than "zero nodes".
	if e.Graph != nil {
		rec.NodeTotal = e.Graph.NodeCount()
	}
	// A creation whose status is already terminal (an admission that completed
	// a trivial workflow inline) records the same instant as its finish: the
	// row's first and only terminal observation.
	if store.IsTerminalExecutionStatus(e.Status) {
		finished := now
		rec.FinishedAt = &finished
	}
	if def, ok := engine.WorkflowDefFromContext(ctx); ok {
		rec.WorkflowName = def.Name
		// The workflow identity, not the display name: an empty def.ID means the
		// submission carried an inline definition with no server id, and the row
		// stays unattributed (invisible to every workflow filter) rather than
		// borrowing a name that is not an identity.
		rec.WorkflowID = types.WorkflowID(def.ID)
		defJSON, err := json.Marshal(def)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow definition for %q: %w", e.ID, err)
		}
		rec.WorkflowDef = defJSON
	}
	// The snapshot is the durable identity the creation captured. The context
	// definition is only the projection's input, and it is absent on a direct
	// rerun seed (the engine attaches none there); without this fallback the
	// list row would read unattributed while GetExecution names the parent's
	// workflow. Submit/Invoke set both from the same definition, so preferring
	// the context changes nothing on those paths.
	if rec.WorkflowID == "" {
		rec.WorkflowID = e.WorkflowID
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
	// Register a canceling transition BEFORE writing the status. The index is
	// namespace-scoped and cannot share the status key's slot, so the two can
	// never be one atomic step; ordering them registration-first is what makes
	// the index complete. A failed registration fails the transition here,
	// while the execution is still in its previous status and Cancel can simply
	// be retried; the reverse order would leave a canceling execution the
	// reaper can never discover. See canceling_index.go.
	if status == types.ExecutionStatusCanceling {
		if err := s.markExecutionCanceling(ctx, t, id); err != nil {
			return err
		}
	}
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
	if applied == 1 {
		// The status key carries the transition; the timestamp is its wall
		// clock for the inspect payload (§6.8 duration = updated − created).
		s.touchExecutionUpdatedAt(ctx, id)
	}
	// Clean up timeout ZSET entries when execution is canceled or timed out.
	if applied == 1 && status == types.ExecutionStatusCanceled {
		s.cleanupOnCancel(ctx, id)
	}
	if applied == 1 && status == types.ExecutionStatusTimeout {
		s.cleanupOnTimeout(ctx, id)
	}
	if applied == 1 && types.IsTerminalExecutionStatus(status) {
		if err := s.shortenTransientCompletionTTL(ctx, id, keys...); err != nil {
			return err
		}
		// Group-key pass, best-effort by construction: this path is a cancel or
		// timeout, and a retention miss on a group's artifacts must not fail a
		// cancellation the caller asked for and Redis has already applied.
		s.shortenTransientGroupCompletionTTLBestEffort(ctx, id)
		// Order matters: the marker is read by the engine's inactive-execution
		// classifier after the status key is gone, and it must keep the active
		// retention — so it is written before evictExecutionCaches drops the
		// transient decision the marker's TTL resolution needs.
		s.markExecutionTerminalBestEffort(ctx, id, status)
		// Redis persists the graph for later inspection/reload; the in-process
		// cache and per-execution TTL override must not survive a terminal state.
		s.evictExecutionCaches(id)
		// The workflow's slot is released after the terminal write is durable,
		// never before: a slot freed while the execution is still winding down
		// would admit a replacement that overlaps it, which is exactly what the
		// limit exists to prevent.
		s.afterExecutionTerminal(ctx, id, status)
		// The execution can no longer be stuck in canceling, so its registration
		// is retired. Best-effort: a leftover member is repaired by the next
		// ListCancelingExecutions that reads it (canceling_index.go).
		s.clearExecutionCanceling(ctx, t, id)
	} else if applied == 1 {
		// Re-assert the registration now that the canceling status is durable.
		// A retried Cancel reuses the member its failed first attempt left
		// behind (the ZADD NX above is a no-op), and List can read the still
		// running status and prune that member in the window between the two
		// steps. ZADD NX keeps the score of an existing member, so the age is
		// not reset; and since the status is already persisted, a failure here
		// is logged rather than returned — the next List's re-check repairs the
		// missing member no later than the following sweep. Placed before
		// refreshTransientTTL so a retry after that fails still finds the
		// member present. Rebuilding on applied == 0 would resurrect an entry
		// for a transition the status fencing refused (e.g. a terminal status).
		if status == types.ExecutionStatusCanceling {
			if err := s.markExecutionCanceling(ctx, t, id); err != nil && s.logger != nil {
				s.logger.Error("re-register canceling execution in the canceling index failed",
					"namespace", string(t), "execution_id", string(id), "err", err)
			}
		}
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

// pauseExecutionLua suspends an execution's dispatch by moving its status to
// paused, with the compare-and-set discipline of updateExecutionStatusLua and
// one addition: it reports WHY it refused, so a caller retrying a pause whose
// response was lost gets a success while a pause of a queued or finished
// execution is refused.
//
// The status write IS the whole pause. Nothing in the outbox is moved, because
// nothing needs to be: leaseOutboxLua and readOutboxLua refuse to name an
// intent while this status stands, which is also what holds the work a task
// already in flight appends after the pause landed — the case a "park the
// intents I can see" pause would miss. A resume therefore reconstructs nothing,
// and a delayed intent keeps the score it was written with. See
// engine.ExecutionPauser for why the enforcement point is the store.
//
// The reply is {code, current}:
//
//	1  applied (running -> paused)
//	2  already paused: a no-op success, not a conflict
//	0  no status key: there is no execution to suspend
//	-1 some other status, reported in current
//
// The TTL is refreshed rather than kept, matching every other status write: the
// status key's expiry is the execution's lifetime, and a pause does not shorten
// it.
//
// KEYS[1] = status key
// ARGV[1] = status TTL in seconds
var pauseExecutionLua = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
    return {0, ''}
end
if current == 'paused' then
    return {2, current}
end
if current ~= 'running' then
    return {-1, current}
end
redis.call('SET', KEYS[1], 'paused', 'EX', tonumber(ARGV[1]))
return {1, current}
`)

// resumeExecutionLua returns a paused execution to dispatch. Every other status
// is refused — including the terminal one an execution can reach while paused,
// because a task leased before the pause is allowed to finish it. A resume that
// reported success there would promise work that is never going to run.
//
// Like the pause, this writes the status and nothing else. The ready set was
// never moved, so the held work is due again the instant this lands; delivering
// it is FlushOutbox's job, which the engine runs after this returns.
//
// The reply is {code, current}: 1 applied, 0 refused with the current status
// ("" when there is no status key at all).
//
// KEYS[1] = status key
// ARGV[1] = status TTL in seconds
var resumeExecutionLua = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current ~= 'paused' then
    return {0, current or ''}
end
redis.call('SET', KEYS[1], 'running', 'EX', tonumber(ARGV[1]))
return {1, current}
`)

var _ engine.ExecutionPauser = (*Store)(nil)

// PauseExecution suspends dispatch of a running execution. See
// engine.ExecutionPauser for the contract, pauseExecutionLua for where the hold
// lives, and leaseOutboxLua for the read that enforces it.
//
// The transition is mirrored onto the SQL audit row for the same reason
// UpdateExecutionStatus mirrors a terminal one: the operator's listing reads
// that row, so without it a paused execution would read as running in every list
// view and a filter for paused would return nothing — the one place an operator
// looks to find what they just paused.
func (s *Store) PauseExecution(ctx context.Context, id types.ExecutionID) error {
	code, current, err := s.runPauseTransition(ctx, id, pauseExecutionLua)
	if err != nil {
		return err
	}
	switch {
	case code == pauseTransitionApplied || code == pauseTransitionAlreadyPaused:
		if code == pauseTransitionApplied {
			s.touchExecutionUpdatedAt(ctx, id)
		}
		s.projectExecutionStatus(ctx, id, types.ExecutionStatusPaused, "")
		return nil
	case current == "":
		return fmt.Errorf("pause execution %q: %w", id, engine.ErrExecutionNotPausable)
	default:
		return fmt.Errorf("pause execution %q in status %q: %w", id, current, engine.ErrExecutionNotPausable)
	}
}

// ResumeExecution returns a paused execution to dispatch. Delivering the work
// the pause held is not part of this call: the engine flushes the execution
// after it returns, and the status is what makes that flush find anything.
func (s *Store) ResumeExecution(ctx context.Context, id types.ExecutionID) error {
	code, current, err := s.runPauseTransition(ctx, id, resumeExecutionLua)
	if err != nil {
		return err
	}
	if code != pauseTransitionApplied {
		return fmt.Errorf("resume execution %q in status %q: %w", id, current, engine.ErrExecutionNotPaused)
	}
	s.touchExecutionUpdatedAt(ctx, id)
	s.projectExecutionStatus(ctx, id, types.ExecutionStatusRunning, "")
	return nil
}

// The transition codes the two scripts share. They are named rather than
// written as literals at each comparison so the pause's "already paused is a
// success" cannot drift from the resume's "only paused is a success".
const (
	// pauseTransitionRefused is not read: a refusal is whatever is left after
	// the two accepted codes, and the scripts report its reason in the status.
	pauseTransitionApplied       = 1
	pauseTransitionAlreadyPaused = 2
)

// runPauseTransition runs one of the two status transitions and decodes its
// {code, current} reply. The two share this tail because they are one
// mechanism — a status compare-and-set that reports why it refused — and a
// backend that handled the reply differently per direction would answer a
// retried pause and a retried resume with different verdicts.
func (s *Store) runPauseTransition(ctx context.Context, id types.ExecutionID, script *redis.Script) (int64, string, error) {
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(ctx, id)
	reply, err := script.Run(ctx, s.rdb, []string{execKey(t, id, "status")}, int(ttl.Seconds())).Slice()
	if err != nil && err != redis.Nil {
		return 0, "", fmt.Errorf("pause transition for %q: %w", id, err)
	}
	if len(reply) < 2 {
		return 0, "", nil
	}
	code, _ := reply[0].(int64)
	current, _ := reply[1].(string)
	return code, current, nil
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

// touchExecutionUpdatedAt records the wall clock of the latest accepted status
// transition on the execution's updated_at key, which GetExecution exposes as
// UpdatedAt. It is best effort by design: the transition itself is already
// durable and is the answer to "what happened", while this timestamp is a
// convenience a reader must treat as unknown when absent. Failing the caller
// would make it retry a transition Redis has already applied.
func (s *Store) touchExecutionUpdatedAt(ctx context.Context, id types.ExecutionID) {
	ttl := s.getExecTTL(ctx, id)
	t := namespace.FromContext(ctx)
	// A suspended execution's keys may have been renewed past the base
	// retention (suspendTTL can exceed it; see extendExecTTL). The clock must
	// live at least as long as the status key it annotates, or a resumed run
	// outliving the base window loses updated_at while the status still reads.
	if remaining, err := s.rdb.PTTL(ctx, execKey(t, id, "status")).Result(); err == nil && remaining > ttl {
		ttl = remaining
	}
	if err := s.rdb.Set(ctx, execKey(t, id, "updated_at_ms"), time.Now().UTC().UnixMilli(), ttl).Err(); err != nil && s.logger != nil {
		s.logger.Debug("touch_execution_updated_at_failed", "execution_id", string(id), "err", err)
	}
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

// GetExecution assembles a full snapshot from the per-field keys the execution
// is stored across. They are read in one pipeline: they are independent GETs of
// the same execution, so issuing them back to back — as this did — cost a
// network round trip each to answer one call.
//
// It is worth the change because this is per-task traffic, not an occasional
// read: engine's buildInput calls it on every BuildTaskLease and
// RecoverTaskLease, and executionSubmissionContext calls it on every batch body.
//
// The absent-execution path now issues every command where it used to stop
// after the status GET. That is a handful of extra COMMANDs on a path that was
// one round trip and still is; the returned snapshot is unchanged.
func (s *Store) GetExecution(ctx context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	statusCmd := pipe.Get(ctx, execKey(t, id, "status"))
	paramsCmd := pipe.Get(ctx, execKey(t, id, "params"))
	runtimeCmd := pipe.Get(ctx, execKey(t, id, "runtime"))
	scopeCmd := pipe.Get(ctx, execKey(t, id, "scope"))
	testRunCmd := pipe.Get(ctx, execKey(t, id, "test_run"))
	traceIDCmd := pipe.Get(ctx, execKey(t, id, "trace_id"))
	spanIDCmd := pipe.Get(ctx, execKey(t, id, "span_id"))
	carrierCmd := pipe.Get(ctx, execKey(t, id, "trace_carrier"))
	errorCmd := pipe.Get(ctx, execKey(t, id, "error"))
	parentCmd := pipe.Get(ctx, execKey(t, id, "parent"))
	rerunFromCmd := pipe.Get(ctx, execKey(t, id, "rerun_from"))
	parentNodeCmd := pipe.Get(ctx, execKey(t, id, "parent_node"))
	depthCmd := pipe.Get(ctx, execKey(t, id, "depth"))
	workflowIDCmd := pipe.Get(ctx, execKey(t, id, "workflow_id"))
	workflowVersionCmd := pipe.Get(ctx, execKey(t, id, "workflow_version"))
	createdAtCmd := pipe.Get(ctx, execKey(t, id, "created_at_ms"))
	updatedAtCmd := pipe.Get(ctx, execKey(t, id, "updated_at_ms"))
	deadlineCmd := pipe.Get(ctx, execKey(t, id, "deadline_ms"))
	namespaceCmd := pipe.Get(ctx, execKey(t, id, "namespace"))
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
	g, _ := s.graphs.Get(id)
	if g == nil {
		// Cold cache — a fresh process, or an entry the idle sweep dropped.
		// Re-read the persisted graph so the returned snapshot keeps carrying
		// it; LoadGraph also re-seeds the cache.
		if g, err = s.LoadGraph(ctx, id); err != nil {
			return nil, err
		}
	}
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
	var testRun bool
	if raw, err := testRunCmd.Result(); err == nil {
		testRun = raw == "1"
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution test_run %q: %w", id, err)
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
	// The execution this one was derived from, and -- for a rerun -- the node it
	// starts at. Absent for every execution an engine started itself, which is
	// why both tolerate redis.Nil.
	var parentID string
	if raw, err := parentCmd.Result(); err == nil {
		parentID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution parent %q: %w", id, err)
	}
	var rerunFrom string
	if raw, err := rerunFromCmd.Result(); err == nil {
		rerunFrom = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution rerun origin %q: %w", id, err)
	}
	// The sub-workflow link: which parent node started this child, and how deep
	// it nests. Absent on every execution an engine started itself, hence the
	// same redis.Nil tolerance as the origin above.
	var parentNode string
	if raw, err := parentNodeCmd.Result(); err == nil {
		parentNode = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution parent node %q: %w", id, err)
	}
	var depth int
	if raw, err := depthCmd.Result(); err == nil {
		depth, err = strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("decode execution depth %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution depth %q: %w", id, err)
	}
	// The workflow identity is immutable metadata written with the execution;
	// absent on every execution recorded without one, hence the same redis.Nil
	// tolerance as the links above.
	var workflowID string
	if raw, err := workflowIDCmd.Result(); err == nil {
		workflowID = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution workflow id %q: %w", id, err)
	}
	var workflowVersion string
	if raw, err := workflowVersionCmd.Result(); err == nil {
		workflowVersion = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution workflow version %q: %w", id, err)
	}
	// The execution's clock. Absent for executions created before these keys
	// existed, which reads back as a zero time and is omitted from the inspect
	// payload rather than guessed.
	var createdAt, updatedAt time.Time
	if raw, err := createdAtCmd.Int64(); err == nil {
		createdAt = time.UnixMilli(raw).UTC()
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution created_at %q: %w", id, err)
	}
	if raw, err := updatedAtCmd.Int64(); err == nil {
		updatedAt = time.UnixMilli(raw).UTC()
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution updated_at %q: %w", id, err)
	}
	// The time budget's absolute deadline. Absent when the execution has none --
	// including every execution created before settings.timeout existed -- which
	// reads back as a zero time, i.e. "no budget".
	var deadline time.Time
	if raw, err := deadlineCmd.Int64(); err == nil {
		deadline = time.UnixMilli(raw).UTC()
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution deadline %q: %w", id, err)
	}
	// The registration namespace; absent on executions created before it was
	// persisted, which readers must treat as "fall back to the ctx namespace".
	var namespaceVal string
	if raw, err := namespaceCmd.Result(); err == nil {
		namespaceVal = raw
	} else if err != redis.Nil {
		return nil, fmt.Errorf("get execution namespace %q: %w", id, err)
	}
	return &engine.ExecutionSnapshot{
		ID:              id,
		Graph:           g,
		Status:          types.ExecutionStatus(val),
		Params:          params,
		Runtime:         runtime,
		Scope:           scope,
		WorkflowID:      types.WorkflowID(workflowID),
		WorkflowVersion: workflowVersion,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		Deadline:        deadline,
		TestRun:         testRun,
		TraceID:         traceID,
		SpanID:          spanID,
		TraceCarrier:    traceCarrier,
		Error:           execErr,
		ParentID:        types.ExecutionID(parentID),
		ParentNode:      parentNode,
		Depth:           depth,
		RerunFrom:       rerunFrom,
		Namespace:       namespaceVal,
	}, nil
}

// GetExecutionInputContext implements engine.ExecutionInputContextReader.
//
// buildInput runs on every task and reads only the lifecycle status, the
// submission params, the runtime, the execution scope and the trace identity.
// GetExecution assembles those from the same keys but also decodes the parent
// and rerun links, the terminal error, the test-run flag and the registration
// namespace, none of which reach a node's input. This issues the six GETs it
// needs in one pipeline — one round trip, as GetExecution already was — and
// decodes only those six values.
//
// Decoding is deliberately the same per field as GetExecution: params and scope
// as map[string]any, runtime as types.Runtime, trace ids as plain strings, with
// redis.Nil tolerated for every optional key. found=false means the status key
// is absent, the same absence GetExecution reports as a nil snapshot.
func (s *Store) GetExecutionInputContext(ctx context.Context, id types.ExecutionID) (*engine.ExecutionInputContext, bool, error) {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	statusCmd := pipe.Get(ctx, execKey(t, id, "status"))
	paramsCmd := pipe.Get(ctx, execKey(t, id, "params"))
	runtimeCmd := pipe.Get(ctx, execKey(t, id, "runtime"))
	scopeCmd := pipe.Get(ctx, execKey(t, id, "scope"))
	traceIDCmd := pipe.Get(ctx, execKey(t, id, "trace_id"))
	spanIDCmd := pipe.Get(ctx, execKey(t, id, "span_id"))
	// Exec surfaces the first command error, and redis.Nil is the expected answer
	// for most of these keys. The per-command results below are authoritative.
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, false, fmt.Errorf("get execution input context %q: %w", id, err)
	}
	val, err := statusCmd.Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get execution input context %q: %w", id, err)
	}
	var params map[string]any
	if raw, err := paramsCmd.Bytes(); err == nil {
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, false, fmt.Errorf("unmarshal execution params %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, false, fmt.Errorf("get execution params %q: %w", id, err)
	}
	var runtime *types.Runtime
	if raw, err := runtimeCmd.Bytes(); err == nil {
		var decoded types.Runtime
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, false, fmt.Errorf("unmarshal execution runtime %q: %w", id, err)
		}
		runtime = &decoded
	} else if err != redis.Nil {
		return nil, false, fmt.Errorf("get execution runtime %q: %w", id, err)
	}
	var scope map[string]any
	if raw, err := scopeCmd.Bytes(); err == nil {
		if err := json.Unmarshal(raw, &scope); err != nil {
			return nil, false, fmt.Errorf("unmarshal execution scope %q: %w", id, err)
		}
	} else if err != redis.Nil {
		return nil, false, fmt.Errorf("get execution scope %q: %w", id, err)
	}
	var traceID string
	if raw, err := traceIDCmd.Result(); err == nil {
		traceID = raw
	} else if err != redis.Nil {
		return nil, false, fmt.Errorf("get execution trace ID %q: %w", id, err)
	}
	var spanID string
	if raw, err := spanIDCmd.Result(); err == nil {
		spanID = raw
	} else if err != redis.Nil {
		return nil, false, fmt.Errorf("get execution span ID %q: %w", id, err)
	}
	return &engine.ExecutionInputContext{
		Status:  types.ExecutionStatus(val),
		Params:  params,
		Runtime: runtime,
		Scope:   scope,
		TraceID: traceID,
		SpanID:  spanID,
	}, true, nil
}

// GetExecutionTraceCarrier implements engine.ExecutionTraceCarrierReader: a
// single GET of the carrier captured at submission.
//
// found=false covers both "no such execution" and "submitted with tracing
// disabled"; the carrier key is written only when a carrier exists, so the two
// are the same absence here and the engine treats both as nil. A corrupt value
// is an error rather than an empty carrier: the control plane would silently
// detach the dispatch span from the workflow trace.
func (s *Store) GetExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, bool, error) {
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, execKey(t, id, "trace_carrier")).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get execution trace carrier %q: %w", id, err)
	}
	var carrier map[string]string
	if err := json.Unmarshal(raw, &carrier); err != nil {
		return nil, false, fmt.Errorf("unmarshal execution trace carrier %q: %w", id, err)
	}
	return carrier, true, nil
}

var _ engine.ExecutionInputContextReader = (*Store)(nil)
var _ engine.ExecutionTraceCarrierReader = (*Store)(nil)
