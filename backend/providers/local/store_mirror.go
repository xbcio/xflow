package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// This file is the local backend's half of the storage contract the Redis
// backend implements in rstate (see backend/providers/distributed/internal/
// rstate/doc.go): the in-memory maps are the system of record, and the
// optional store.Store is a durable projection of them.
//
// The split follows rstate exactly:
//
//   - CreateExecution is a required mirror. rstate cleans up the Redis keys and
//     fails the create when the SQL insert fails; here the equivalent is
//     rollbackCreatedExecution followed by the same error return. An execution
//     the engine was told exists must be listable in the store, or a restart
//     would leave no trace of it at all.
//   - Everything after creation is best-effort. rstate routes those writes
//     through auditWrite, which never returns an error; here they go through
//     the serialized projection helpers below, which log and drop failures.
//   - Transient executions are never projected at all. rstate gates every
//     write point on isTransient because the mode exists to keep payloads out
//     of the platform database; the local mirror records the same verdict per
//     execution at create/seed time and skips every projection for it.
//
// Ordering and convergence
//
// Every store write for one execution is serialized by that execution's own
// mutex (mirrorLocks), never by the state mutex. Inside the serialized
// section the helper reads the CURRENT in-memory state and builds the record
// from it, rather than projecting the value captured at transition time. That
// is what makes the mirror convergent:
//
//   - Creation commits in memory, then takes the mirror lock. A workflow that
//     runs to completion in the meantime commits its status first; the insert
//     either reads the terminal status directly or is followed by the
//     reconciliation update, so the row can never be left at "running".
//   - A delayed status projection cannot overwrite a newer status: it reads
//     the status in force when the store UPDATE runs, not the status that
//     triggered it. The last writer writes the latest state.
//   - Node and signal projections are state-based for the same reason: they
//     read the current snapshot / signal table under s.mu.
//
// The per-execution mutex is never acquired while s.mu is held, so the lock
// order is always mirrorLock -> s.mu. Its map entry is refcounted and dropped
// only when its last holder releases and the execution is terminal, skipped, or
// rolled back, so a retire-then-recreate can never put two lock instances on
// one execution.
//
// Create-pending gate
//
// The required create/seed insert opens mirrorCreatePending for its execution.
// While it is open, ordinary status/node/signal projections are dropped (the
// insert reconciles status and pre-delivered signals from live memory before
// closing the gate), promotions flip the status but keep intents held, and
// seeds hold theirs. That is what stops a dispatcher, or an operator cancel,
// from racing side effects or an old process's row against an insert that may
// still be refused.

// mirrorWriteTimeout bounds one store call made after the scheduling
// transition has already committed. The caller's context may be cancelled the
// moment its transition returns (engine stop cancels task contexts), so the
// write is detached from cancellation but kept bounded: a hung store must not
// pin a worker goroutine forever.
//
// 2s is deliberately below the SQLite driver's busy_timeout (5s in
// store/sqlstore/sqlitestore): a mirror write waiting behind a contended
// database lock must give up before the driver does, because this write is
// best-effort and must not stall the scheduling path. It is still far above a
// healthy local write (microseconds for memstore, low milliseconds for SQLite).
// Projections stay synchronous; there is no async queue.
const mirrorWriteTimeout = 2 * time.Second

// ErrStoreExecutionExists reports that the durable store already holds a row
// for the execution ID this create tried to persist. It is the restart case:
// deterministic IDs (idempotency keys, entry seeds) are stable across process
// lifetimes, but the in-memory maps that remember them are not.
var ErrStoreExecutionExists = errors.New("local: execution already exists in the durable store")

// execMirrorLock serializes every store write for one execution. The pointer
// is stable while the execution is live; refs counts the goroutines holding or
// waiting for it, so the map entry is dropped only when nobody can still be
// serialized through it (see finishMirror). refs is guarded by memoryState.mu.
type execMirrorLock struct {
	mu   sync.Mutex
	refs int
}

// mirrorCtx detaches a projection write from the caller's cancellation (the
// transition that scheduled it has already committed, so a cancelled engine
// must still be able to record the terminal state) and bounds it so a hung
// store cannot block indefinitely.
func mirrorCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), mirrorWriteTimeout)
}

// beginMirror returns the serialization lock for one of an execution's
// ordinary projections, or nil when there is nothing to project.
//
// s.mu is never held by the caller. The returned lock is held; callers must
// release it with finishMirror. A caller that waited while the execution was
// rolled back will find no in-memory state and project nothing — every
// projection re-reads the state under s.mu.
func (s *memoryState) beginMirror(id types.ExecutionID) *execMirrorLock {
	return s.beginMirrorMode(id, false)
}

// beginCreateMirror is beginMirror for the required create/seed insert. It is
// the one projection allowed while the create-pending gate is closed: the
// insert IS the pending work.
func (s *memoryState) beginCreateMirror(id types.ExecutionID) *execMirrorLock {
	return s.beginMirrorMode(id, true)
}

func (s *memoryState) beginMirrorMode(id types.ExecutionID, create bool) *execMirrorLock {
	// db is immutable after construction, so this check needs no lock. A nil
	// store must leave no observable trace at all.
	if s.db == nil {
		return nil
	}
	s.mu.Lock()
	// While the required create/seed insert is in flight, every other
	// projection is dropped: it may still be reading a row that belongs to a
	// previous process, and the create reconciliation re-reads live state once
	// the insert lands. The same gate keeps promotions and seeds from making
	// intents deliverable before the row exists.
	if s.mirrorSkip[id] || s.rolledBack[id] || (!create && s.mirrorCreatePending[id]) {
		s.mu.Unlock()
		return nil
	}
	m := s.mirrorLocks[id]
	if m == nil {
		m = &execMirrorLock{}
		s.mirrorLocks[id] = m
	}
	m.refs++
	s.mu.Unlock()
	m.mu.Lock()
	return m
}

// finishMirror releases one projection's serialization lock and retires the
// map entry when nobody is holding or waiting for it and the execution is
// over. Keeping the entry while refs > 0 is what stops a terminal projection
// from dropping the lock another projection is queued on: that would let two
// lock instances serialize the same execution concurrently.
func (s *memoryState) finishMirror(id types.ExecutionID, m *execMirrorLock) {
	m.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	m.refs--
	if m.refs != 0 || s.mirrorLocks[id] != m {
		return
	}
	entry := s.executions[id]
	if entry == nil || isTerminalStatus(entry.snap.Status) || s.mirrorSkip[id] || s.rolledBack[id] {
		delete(s.mirrorLocks, id)
	}
	s.pruneMirrorStateLocked(id)
}

// noteMirrorModeLocked records the transient verdict for one execution at
// create/seed time. Transient graphs and per-execution transient hints promise
// that no SQL row or payload exists; the flag is what every projection checks.
// Callers must hold s.mu.
//
// With no store attached there is nothing to skip and the maps stay empty:
// the nil-store path must be byte-for-byte the pre-option behavior.
func (s *memoryState) noteMirrorModeLocked(ctx context.Context, e *engine.ExecutionSnapshot) {
	if s.db == nil {
		return
	}
	transient := e.Graph != nil && e.Graph.Transient()
	if _, ok := engine.ExecutionTransientFromContext(ctx); ok {
		transient = true
	}
	if transient {
		s.mirrorSkip[e.ID] = true
	} else {
		delete(s.mirrorSkip, e.ID)
	}
}

// markMirrorCreatePendingLocked opens the create-pending gate for one
// execution and reports whether a projection will follow. Callers must hold
// s.mu and have called noteMirrorModeLocked already, so a transient execution
// never opens it.
func (s *memoryState) markMirrorCreatePendingLocked(id types.ExecutionID) bool {
	if s.db == nil || s.mirrorSkip[id] {
		return false
	}
	s.mirrorCreatePending[id] = true
	return true
}

// markMirrorConflict disables all further projections for one execution. Used
// when a seed finds a durable row left by a previous process: the memory state
// is still authoritative for this run, but updating the old row would corrupt a
// different execution's history.
//
// Unlike a transient skip, a conflict skip is kept for the lifetime of the
// in-memory execution, even after it turns terminal: it is the only thing
// preventing a late projection from writing into the old run's row. The set is
// bounded by colliding deterministic seed IDs, which are rare by construction.
func (s *memoryState) markMirrorConflict(id types.ExecutionID) {
	s.mu.Lock()
	s.mirrorSkip[id] = true
	s.mirrorConflict[id] = true
	s.mu.Unlock()
}

// pruneMirrorStateLocked drops the transient skip once the execution is over
// and no mirror write for it can still be in flight. A conflict skip is kept
// (see markMirrorConflict). The rolledBack tombstone is deliberately not
// pruned: a dispatcher may still hold a task for the rolled-back execution, and
// the tombstone is what refuses its late lease. It is cleared when the ID is
// created again, so its size is bounded by failed creates that are never
// retried. Callers must hold s.mu.
func (s *memoryState) pruneMirrorStateLocked(id types.ExecutionID) {
	entry := s.executions[id]
	if entry == nil || !isTerminalStatus(entry.snap.Status) || s.mirrorCreatePending[id] {
		return
	}
	if _, active := s.mirrorLocks[id]; active {
		return
	}
	if !s.mirrorConflict[id] {
		delete(s.mirrorSkip, id)
	}
}

// pruneTransientSkipAfterTerminal is the nil-result hook for a status
// projection that found the mirror skipped: a terminal transition is the last
// projection of an execution's life, so an idle transient verdict can go. A
// conflict verdict stays.
func (s *memoryState) pruneTransientSkipAfterTerminal(id types.ExecutionID) {
	if s.db == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneMirrorStateLocked(id)
}

// mirrorBestEffort runs one post-commit projection write. By contract the
// in-memory transition has already landed, so an error here is a reporting gap,
// not a scheduling failure: it is logged and dropped, never returned.
func (s *memoryState) mirrorBestEffort(op string, fn func() error) {
	if err := fn(); err != nil {
		if s.logger != nil {
			s.logger.Error("store_mirror_write_failed", "op", op, "err", err)
		}
	}
}

// projectExecutionStatus writes the status currently in force in memory.
// Callers must hold the execution's mirror lock; the state mutex is taken only
// to read the snapshot.
//
// store.ErrNotFound is not an audit failure for this projection: it means
// there is no execution row to update, which happens when the create
// projection was lost (creation is required, so this is rare). rstate treats
// the same error as OK (writeExecutionStatusProjection).
func (s *memoryState) projectExecutionStatus(ctx context.Context, id types.ExecutionID) {
	s.mu.Lock()
	entry := s.executions[id]
	if entry == nil {
		s.mu.Unlock()
		return
	}
	status, errMsg := entry.snap.Status, entry.snap.Error
	s.mu.Unlock()
	if status == "" {
		return
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	err := s.db.UpdateExecutionStatus(wctx, id, status, errMsg)
	if errors.Is(err, store.ErrNotFound) {
		if s.logger != nil {
			s.logger.Debug("store_mirror_status_skipped", "execution_id", string(id), "status", string(status))
		}
		return
	}
	if err != nil {
		if s.logger != nil {
			s.logger.Error("store_mirror_write_failed", "op", "update_execution_status", "err", err)
		}
	}
}

// mirrorExecutionStatus projects whatever execution status is current when it
// reaches the store. The transition's own value is deliberately not passed in:
// a delayed projection must not resurrect a status a newer transition already
// replaced.
func (s *memoryState) mirrorExecutionStatus(ctx context.Context, id types.ExecutionID) {
	m := s.beginMirror(id)
	if m == nil {
		// A terminal status is the last projection an execution gets; if the
		// skip was only the transient verdict, let it go.
		s.pruneTransientSkipAfterTerminal(id)
		return
	}
	defer s.finishMirror(id, m)
	s.projectExecutionStatus(ctx, id)
}

// mirrorNode projects the current snapshot of one node. Only the state in
// force is projected: a stale transition that lost its CAS has no mirror call
// at all, and the snapshot is read (and the SQL record built) under s.mu so no
// map is read while another transition mutates it.
func (s *memoryState) mirrorNode(ctx context.Context, id types.ExecutionID, name string) {
	m := s.beginMirror(id)
	if m == nil {
		return
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	ns := s.nodes[memoryNodeKey(id, name)]
	var rec *store.NodeRecord
	if ns != nil {
		rec = buildMirrorNodeRecord(ns, time.Now())
	}
	s.mu.Unlock()
	if rec == nil {
		return
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	s.mirrorBestEffort("upsert_node", func() error { return s.db.UpsertNode(wctx, rec) })
}

// buildMirrorNodeRecord turns a node snapshot into its durable row. Callers
// must hold s.mu (or otherwise own the snapshot exclusively). The output is
// marshalled only when the snapshot still carries it — a private output is nil
// here, the same rule rstate applies in state_node.go/state_commit.go.
func buildMirrorNodeRecord(n *engine.NodeSnapshot, now time.Time) *store.NodeRecord {
	var output []byte
	if n.Output != nil {
		output, _ = json.Marshal(n.Output) // json.Marshal of map[string]any cannot fail
	}
	return &store.NodeRecord{
		ExecutionID: n.ExecutionID,
		NodeName:    n.Name,
		Status:      n.Status,
		LeaseID:     string(n.LeaseID),
		LeaseToken:  string(n.LeaseToken),
		Attempt:     n.Attempt,
		Output:      output,
		Port:        n.Port,
		StartedAt:   timePtr(n.StartedAt),
		FinishedAt:  timePtr(n.FinishedAt),
		UpdatedAt:   now,
	}
}

// mirrorSaveSignal projects a signal that is still waiting for consumption.
// A signal that resumed a node, or that was consumed or revoked before this
// projection ran, is not written: rstate writes a signal row only on the
// pre-delivery path, and the projection is state-based so it follows the
// memory table.
func (s *memoryState) mirrorSaveSignal(ctx context.Context, id types.ExecutionID, signalName string) {
	m := s.beginMirror(id)
	if m == nil {
		return
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	data, present := s.signals[string(id)+"/"+signalName]
	if present {
		data = cloneData(data)
	}
	s.mu.Unlock()
	if !present {
		return
	}
	dataJSON, _ := json.Marshal(data) // json.Marshal of map[string]any cannot fail
	rec := &store.SignalRecord{
		ExecutionID: id,
		SignalName:  signalName,
		Payload:     dataJSON,
		CreatedAt:   time.Now(),
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	s.mirrorBestEffort("save_signal", func() error { return s.db.SaveSignal(wctx, rec) })
}

// mirrorRevokeSignal projects a revocation only while the signal is absent
// from memory. If it was re-delivered after the revoke, the row must stay.
// rstate ignores the bool returned by RevokeSignal; the durable row is a
// projection, not the authority for whether the revoke succeeded.
func (s *memoryState) mirrorRevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) {
	m := s.beginMirror(id)
	if m == nil {
		return
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	_, present := s.signals[string(id)+"/"+signalName]
	s.mu.Unlock()
	if present {
		return
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	s.mirrorBestEffort("revoke_signal", func() error {
		_, err := s.db.RevokeSignal(wctx, id, signalName)
		return err
	})
}

// mirrorCreateExecution is the required creation projection. It mirrors
// rstate.StateStore.CreateExecution (state_execution.go): the caller has
// already committed the in-memory execution, and a store failure rolls that
// commit back and fails the call.
//
// The record is built from the snapshot current when the serialized section
// runs, and the recorded status is reconciled immediately after the insert.
// That closes the window in which the dispatcher completes a short workflow
// before the insert lands: the row is either inserted terminal or updated to
// terminal before the create returns.
//
// If the store already holds the execution ID — the restart case for
// deterministic IDs — ErrStoreExecutionExists is returned so the caller can
// distinguish it from a transport failure.
func (s *memoryState) mirrorCreateExecution(ctx context.Context, id types.ExecutionID) error {
	m := s.beginCreateMirror(id)
	if m == nil {
		return nil
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	entry := s.executions[id]
	if entry == nil {
		// Rolled back before the projection ran; nothing to persist.
		s.mu.Unlock()
		return nil
	}
	rec, err := buildMirrorExecutionRecord(ctx, &entry.snap, entry.snap.Status, time.Now())
	s.mu.Unlock()
	if err != nil {
		return err
	}

	wctx, cancel := mirrorCtx(ctx)
	err = s.db.CreateExecution(wctx, rec)
	cancel()
	if err != nil {
		if s.durableExecutionExists(ctx, id) {
			return fmt.Errorf("store create execution: execution %q already exists in the durable store: %w", id, ErrStoreExecutionExists)
		}
		return err
	}
	// The row exists, so the gate can open before the reconciliation below:
	// projections that raced the insert now queue on this same mirror lock and
	// run, state-based, right after it. That is how a revoke delivered while
	// the gate was closed reaches the store (its own projection is ordered
	// after the reconciliation instead of being dropped).
	s.clearMirrorCreatePending(id)
	// Reconcile: the execution may have moved on while the insert was in flight.
	s.projectExecutionStatus(ctx, id)
	s.reconcilePreDeliveredSignals(ctx, id)
	return nil
}

// clearMirrorCreatePending opens the create-pending gate once the required row
// exists (or the seed has resolved its conflict).
func (s *memoryState) clearMirrorCreatePending(id types.ExecutionID) {
	s.mu.Lock()
	delete(s.mirrorCreatePending, id)
	s.mu.Unlock()
}

// reconcilePreDeliveredSignals rewrites signal rows that the create-pending
// gate dropped. Status converges through projectExecutionStatus, but a signal
// delivered from outside the engine while the insert was in flight is not part
// of the execution row, so it is rebuilt from live memory here. This runs only
// after a successful insert; a seed conflict must leave the old run's rows
// alone. Callers hold the execution's mirror lock.
func (s *memoryState) reconcilePreDeliveredSignals(ctx context.Context, id types.ExecutionID) {
	prefix := string(id) + "/"
	type pendingSignal struct {
		name    string
		payload []byte
	}
	s.mu.Lock()
	signals := make([]pendingSignal, 0, len(s.signals))
	for key, data := range s.signals {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		payload, _ := json.Marshal(cloneData(data)) // json.Marshal of map[string]any cannot fail
		signals = append(signals, pendingSignal{
			name:    strings.TrimPrefix(key, prefix),
			payload: payload,
		})
	}
	s.mu.Unlock()
	if len(signals) == 0 {
		return
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	for _, sig := range signals {
		rec := &store.SignalRecord{
			ExecutionID: id,
			SignalName:  sig.name,
			Payload:     sig.payload,
			CreatedAt:   time.Now(),
		}
		signal := rec
		s.mirrorBestEffort("reconcile_signal", func() error { return s.db.SaveSignal(wctx, signal) })
	}
}

// durableExecutionExists classifies a failed create without inspecting driver
// error text. A row for the ID means the insert lost to a row that is already
// there (a deterministic ID surviving a process restart); anything else is a
// genuine write failure.
func (s *memoryState) durableExecutionExists(ctx context.Context, id types.ExecutionID) bool {
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	rec, err := s.db.GetExecution(wctx, id)
	return err == nil && rec != nil
}

// buildMirrorExecutionRecord is the local copy of
// rstate.buildExecutionRecord. The definition attached to the submission
// context supplies the workflow name and definition payload; the snapshot
// supplies the identity captured at submission when the context has none.
//
// The namespace comes from the snapshot when it carries one and otherwise from
// the context, which is the inverse of rstate's preference. Local creation
// ignores the context for scheduling, and the snapshot is the only place the
// registration namespace survives a submission whose ctx names the deployment
// rather than the workflow. Both answer namespace.Default on an unstamped
// context, so a plain embedded engine records exactly what rstate would.
func buildMirrorExecutionRecord(ctx context.Context, e *engine.ExecutionSnapshot, status types.ExecutionStatus, now time.Time) (*store.ExecutionRecord, error) {
	ns := e.Namespace
	if ns == "" {
		ns = string(namespace.FromContext(ctx))
	}
	rec := &store.ExecutionRecord{
		ExecutionID: e.ID,
		Namespace:   ns,
		Status:      status,
		TriggerKind: engine.ExecutionTriggerKind(e),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	// The compiled graph's size is the node-progress denominator; a nil graph is
	// UNKNOWN (0), not "zero nodes".
	if e.Graph != nil {
		rec.NodeTotal = e.Graph.NodeCount()
	}
	// A creation that is already terminal records its finish at the same
	// instant; the store's UpdateExecutionStatus then keeps it write-once.
	if store.IsTerminalExecutionStatus(status) {
		finished := now
		rec.FinishedAt = &finished
	}
	if def, ok := engine.WorkflowDefFromContext(ctx); ok {
		rec.WorkflowName = def.Name
		// An empty def.ID means the submission carried an inline definition with
		// no server id; the row stays unattributed rather than borrowing a name
		// that is not an identity.
		rec.WorkflowID = types.WorkflowID(def.ID)
		defJSON, err := json.Marshal(def)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow definition for %q: %w", e.ID, err)
		}
		rec.WorkflowDef = defJSON
	}
	// The snapshot is the durable identity the creation captured (see
	// rstate.buildExecutionRecord for why it wins over a context with no
	// definition).
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

// mirrorEmptyWorkflowDef satisfies the schema's NOT NULL workflow_def column
// for a seed, which carries a compiled graph rather than a serialized
// definition. It is the local counterpart of rstate.emptyJSONObject.
var mirrorEmptyWorkflowDef = []byte(`{}`)

// mirrorSeededExecution projects the audit row for an execution created by
// SeedExecution (the rerun/seed path). Best effort, matching
// rstate.projectSeededRerun: this path never calls CreateExecution, so without
// it a seeded execution would have no row at all once the process exits.
//
// If the row already exists (a deterministic seed ID surviving a restart), the
// run is left unprojected: updating the old row would merge two executions'
// histories. The warning is the only signal, and every later projection for
// this execution is disabled.
func (s *memoryState) mirrorSeededExecution(ctx context.Context, req engine.SeedExecutionRequest) {
	id := req.Execution.ID
	m := s.beginCreateMirror(id)
	if m == nil {
		return
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	entry := s.executions[id]
	if entry == nil {
		s.mu.Unlock()
		return
	}
	snap := entry.snap
	rec := s.buildSeededExecutionRecord(ctx, snap.Status, &snap)
	s.mu.Unlock()

	s.projectSeededRow(ctx, rec)
}

// mirrorSeededEntryExecution is the entry-admission sibling of
// mirrorSeededExecution, matching rstate.projectSeededExecution. The admission
// request, not the snapshot, carries the params/runtime/trace fields on this
// path.
func (s *memoryState) mirrorSeededEntryExecution(ctx context.Context, req engine.SeedExecutionFromEntryRequest) {
	id := engine.DeterministicExecutionID(req.AdmissionKey)
	m := s.beginCreateMirror(id)
	if m == nil {
		return
	}
	defer s.finishMirror(id, m)

	s.mu.Lock()
	entry := s.executions[id]
	if entry == nil {
		s.mu.Unlock()
		return
	}
	status := entry.snap.Status
	rec := s.buildSeededEntryExecutionRecord(ctx, req, status)
	s.mu.Unlock()

	s.projectSeededRow(ctx, rec)
}

// buildSeededExecutionRecord assembles a rerun seed's row. The snapshot is the
// admitted execution and is current for identity fields; only the status must
// come from the live transition. Callers hold s.mu.
func (s *memoryState) buildSeededExecutionRecord(ctx context.Context, status types.ExecutionStatus, snap *engine.ExecutionSnapshot) *store.ExecutionRecord {
	now := time.Now()
	rec := &store.ExecutionRecord{
		ExecutionID: snap.ID,
		Status:      status,
		TriggerKind: engine.ExecutionTriggerKind(snap),
		TraceID:     snap.TraceID,
		SpanID:      snap.SpanID,
		CreatedAt:   now,
		UpdatedAt:   now,
		WorkflowDef: mirrorEmptyWorkflowDef,
	}
	rec.Namespace = snap.Namespace
	if rec.Namespace == "" {
		rec.Namespace = string(namespace.FromContext(ctx))
	}
	if snap.Graph != nil {
		rec.WorkflowName = snap.Graph.Name()
		rec.NodeTotal = snap.Graph.NodeCount()
	}
	if store.IsTerminalExecutionStatus(status) {
		finished := now
		rec.FinishedAt = &finished
	}
	if def, ok := engine.WorkflowDefFromContext(ctx); ok && def.ID != "" {
		rec.WorkflowID = types.WorkflowID(def.ID)
	}
	if rec.WorkflowID == "" {
		rec.WorkflowID = snap.WorkflowID
	}
	// An unencodable field drops only that field, never the row: this projection
	// is the execution's last surviving record. Both JSON columns are nullable.
	if snap.Params != nil {
		if paramsJSON, err := json.Marshal(snap.Params); err != nil {
			s.logMirrorEncodeFailure(snap.ID, "params", err)
		} else {
			rec.Params = paramsJSON
		}
	}
	if snap.Runtime != nil {
		if runtimeJSON, err := json.Marshal(snap.Runtime); err != nil {
			s.logMirrorEncodeFailure(snap.ID, "runtime", err)
		} else {
			rec.Runtime = runtimeJSON
		}
	}
	return rec
}

// buildSeededEntryExecutionRecord assembles an entry admission's row. Callers
// hold s.mu.
func (s *memoryState) buildSeededEntryExecutionRecord(ctx context.Context, req engine.SeedExecutionFromEntryRequest, status types.ExecutionStatus) *store.ExecutionRecord {
	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	now := time.Now()
	rec := &store.ExecutionRecord{
		ExecutionID: execID,
		Status:      status,
		TriggerKind: req.TriggerKind,
		TraceID:     req.TraceID,
		SpanID:      req.SpanID,
		CreatedAt:   now,
		UpdatedAt:   now,
		WorkflowDef: mirrorEmptyWorkflowDef,
	}
	rec.Namespace = string(req.Namespace)
	if rec.Namespace == "" {
		rec.Namespace = string(namespace.FromContext(ctx))
	}
	if req.Graph != nil {
		rec.WorkflowName = req.Graph.Name()
		rec.NodeTotal = req.Graph.NodeCount()
	}
	if store.IsTerminalExecutionStatus(status) {
		finished := now
		rec.FinishedAt = &finished
	}
	rec.WorkflowID = req.WorkflowID
	if def, ok := engine.WorkflowDefFromContext(ctx); ok && def.ID != "" {
		rec.WorkflowID = types.WorkflowID(def.ID)
	}
	if req.Params != nil {
		if paramsJSON, err := json.Marshal(req.Params); err != nil {
			s.logMirrorEncodeFailure(execID, "params", err)
		} else {
			rec.Params = paramsJSON
		}
	}
	if req.Runtime != nil {
		if runtimeJSON, err := json.Marshal(req.Runtime); err != nil {
			s.logMirrorEncodeFailure(execID, "runtime", err)
		} else {
			rec.Runtime = runtimeJSON
		}
	}
	return rec
}

// projectSeededRow inserts one seeded row and reconciles its status. Callers
// hold the execution's mirror lock.
func (s *memoryState) projectSeededRow(ctx context.Context, rec *store.ExecutionRecord) {
	wctx, cancel := mirrorCtx(ctx)
	err := s.db.CreateExecution(wctx, rec)
	cancel()
	if err != nil {
		if s.durableExecutionExists(ctx, rec.ExecutionID) {
			if s.logger != nil {
				s.logger.Warn("store_mirror_seed_conflict", "execution_id", string(rec.ExecutionID))
			}
			s.markMirrorConflict(rec.ExecutionID)
			return
		}
		s.mirrorBestEffort("create_seeded_execution", func() error { return err })
		return
	}
	// The seeded execution may already have advanced (or finalized) while the
	// insert was in flight; converge on the live status, then restore any
	// pre-delivered signal the pending gate dropped. The gate opens first for
	// the same reason as on the required-create path: raced projections queue
	// behind this lock and apply their current state after the reconciliation.
	s.clearMirrorCreatePending(rec.ExecutionID)
	s.projectExecutionStatus(ctx, rec.ExecutionID)
	s.reconcilePreDeliveredSignals(ctx, rec.ExecutionID)
}

// logMirrorEncodeFailure reports a request field that could not be encoded for
// the SQL projection. It is deliberately not routed through mirrorBestEffort:
// an encode failure is not a store failure, and counting it as one would report
// a phantom outage while the row is still written. It is called while s.mu is
// held; reading s.logger needs no lock.
func (s *memoryState) logMirrorEncodeFailure(id types.ExecutionID, field string, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Error("store_mirror_encode_failed", "execution_id", string(id), "field", field, "err", err)
}

// rollbackCreatedExecution undoes an in-memory create whose required store
// projection failed. It is the local counterpart of
// rstate.cleanupCreatedExecution: every piece of execution-scoped state the
// create wrote is removed, a charged concurrency slot (or queue position) is
// released — which may promote the next waiting execution — and waiters are
// unblocked rather than left on channels nothing will close.
func (s *memoryState) rollbackCreatedExecution(id types.ExecutionID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Before deleting the execution: the release path inspects executions and
	// promotes the head of the workflow's line.
	s.releaseWorkflowSlotLocked(id)

	entry := s.executions[id]
	delete(s.executions, id)
	if done, ok := s.doneCh[id]; ok {
		if entry == nil || !entry.closed {
			close(done)
		}
		delete(s.doneCh, id)
	}
	// Waiters observe the removal as a terminal event; leaving the channels
	// open would strand every Stream caller on a channel nothing will close.
	if watchers, ok := s.eventWatchers[id]; ok {
		for _, watcher := range watchers {
			close(watcher)
		}
		delete(s.eventWatchers, id)
	}

	prefix := string(id) + "/"
	for key := range s.nodes {
		if strings.HasPrefix(key, prefix) {
			delete(s.nodes, key)
		}
	}
	for key := range s.inDegrees {
		if strings.HasPrefix(key, prefix) {
			delete(s.inDegrees, key)
		}
	}
	for key := range s.activeIns {
		if strings.HasPrefix(key, prefix) {
			delete(s.activeIns, key)
		}
	}
	deleteKeysWithPrefix(s.advanced, prefix)
	deleteKeysWithPrefix(s.scheduled, prefix)
	for key := range s.outputs {
		if strings.HasPrefix(key, prefix) {
			delete(s.outputs, key)
		}
	}
	deleteKeysWithPrefix(s.privateOutputs, prefix)
	for key := range s.suspended {
		if strings.HasPrefix(key, prefix) {
			delete(s.suspended, key)
		}
	}
	for key := range s.signals {
		if strings.HasPrefix(key, prefix) {
			delete(s.signals, key)
		}
	}
	for key := range s.signalSets {
		if strings.HasPrefix(key, prefix) {
			delete(s.signalSets, key)
		}
	}
	deleteKeysWithPrefix(s.resumed, prefix)
	for key := range s.subExecs {
		if strings.HasPrefix(key, prefix) {
			delete(s.subExecs, key)
		}
	}
	for key := range s.subWorkflowLinks {
		if strings.HasPrefix(key, prefix) {
			delete(s.subWorkflowLinks, key)
		}
	}
	for key := range s.subWorkflowConclusions {
		if strings.HasPrefix(key, prefix) {
			delete(s.subWorkflowConclusions, key)
		}
	}
	for key := range s.groupUnits {
		if strings.HasPrefix(key, prefix) {
			delete(s.groupUnits, key)
		}
	}
	delete(s.outbox, id)
	delete(s.deadOutbox, id)
	delete(s.replayReceipts, id)
	delete(s.replayEntryIdx, id)
	delete(s.remaining, id)
	delete(s.failed, id)
	delete(s.idempotency, id)
	delete(s.cancelingSince, id)
	delete(s.mirrorLocks, id)
	delete(s.mirrorSkip, id)
	delete(s.mirrorCreatePending, id)
	delete(s.mirrorConflict, id)
	// Late tasks may still reference this execution; the marker makes the
	// lease path refuse them instead of recreating node state.
	s.rolledBack[id] = true
	for key, admission := range s.admissions {
		if admission.executionID == id {
			delete(s.admissions, key)
		}
	}
}

// timePtr turns a snapshot's timing fact into the nullable form NodeRecord
// uses. A zero time means "the fact never happened".
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

// deleteKeysWithPrefix removes every key of m that belongs to one execution's
// key space. rollbackCreatedExecution uses it across the maps whose keys carry
// the "<execID>/" convention.
func deleteKeysWithPrefix[V any](m map[string]V, prefix string) {
	for key := range m {
		if strings.HasPrefix(key, prefix) {
			delete(m, key)
		}
	}
}
