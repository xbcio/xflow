package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// laggingProjectionStore models the SQL audit store while an execution's
// projection has not landed yet: GetExecution reports no row, which is exactly
// what a lagging (or failed) best-effort projection looks like. The row can be
// switched on to model the projection arriving later.
type laggingProjectionStore struct {
	store.Store
	rowPresent bool
	getCalls   int
	nodeWrites int
}

func (s *laggingProjectionStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	return nil
}

func (s *laggingProjectionStore) GetExecution(_ context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	s.getCalls++
	if !s.rowPresent {
		return nil, store.ErrNotFound
	}
	return &store.ExecutionRecord{ExecutionID: id}, nil
}

func (s *laggingProjectionStore) UpsertNode(context.Context, *store.NodeRecord) error {
	s.nodeWrites++
	return nil
}

func (s *laggingProjectionStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	return nil
}

// TestDurableMarkerOutrunsSQLProjectionAcrossReplicas is the regression test for
// the cross-replica misclassification.
//
// Creation commits the execution to Redis first and projects it to SQL as a
// best-effort follow-up. A second replica can therefore see a live execution
// while SQL still reports no row. That miss used to be read as evidence of
// transience and cached with no expiry, so once the projection did land this
// replica kept skipping the execution's audit writes forever.
//
// The explicit durable marker, written by the creation that makes the execution
// visible, removes the guess: replica B must classify the execution as durable
// without consulting SQL at all.
func TestDurableMarkerOutrunsSQLProjectionAcrossReplicas(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	ctx := context.Background()
	id := types.ExecutionID("exec-cross-replica-durable")

	// Replica A admits the execution. No SQL store: the projection has not
	// landed (and may still be in flight).
	replicaA := New(rdb, nil, time.Hour)
	replicaA.transient = false
	if err := replicaA.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution on replica A: %v", err)
	}

	marker := transientMarkKey(namespace.FromContext(ctx), id)
	fields, err := rdb.HGetAll(ctx, marker).Result()
	if err != nil {
		t.Fatalf("read mode marker at %s: %v", marker, err)
	}
	if fields["durable"] != "1" {
		t.Fatalf("creation wrote mode marker %v, want durable=1: the verdict must "+
			"not depend on the SQL projection", fields)
	}
	if _, isTransient := fields["transient"]; isTransient {
		t.Fatalf("durable execution carries the transient field: %v", fields)
	}

	// Replica B sees Redis but its SQL store reports no row: the projection is
	// still lagging.
	lagging := &laggingProjectionStore{}
	replicaB := New(rdb, lagging, time.Hour)
	replicaB.transient = false

	if replicaB.isTransient(ctx, id) {
		t.Fatalf("replica B classified %s as transient while its SQL projection "+
			"was lagging; it will skip audit/projection for this execution", id)
	}
	if lagging.getCalls != 0 {
		t.Fatalf("replica B asked SQL %d time(s) for a marker-carrying execution, "+
			"want 0: the explicit marker is the verdict", lagging.getCalls)
	}
	if mark, ok := replicaB.execTransient[id]; !ok || !mark.durableConfirmed {
		t.Fatalf("replica B cached verdict %+v (ok=%v), want a confirmed durable verdict",
			mark, ok)
	}

	// The projection lands. The verdict must stay durable, and audit writes must
	// actually run afterwards.
	lagging.rowPresent = true
	replicaB.evictExecutionCaches(id)
	if replicaB.isTransient(ctx, id) {
		t.Fatalf("replica B reads %s as transient after the SQL row appeared", id)
	}
	if err := replicaB.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}
	if lagging.nodeWrites != 1 {
		t.Fatalf("node projections = %d, want 1: a durable execution's audit trail "+
			"must not stay dark", lagging.nodeWrites)
	}
}

// TestSQLMissDoesNotPinTransientVerdict covers the markerless fallback: an
// execution created before the durable marker existed, or one whose transient
// marker lapsed. SQL is consulted, but a missing row is a projection lag, not
// evidence -- so the fail-closed transient answer must not be cached. Once the
// row appears the verdict must repair itself without a process restart.
func TestSQLMissDoesNotPinTransientVerdict(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	ctx := context.Background()
	id := types.ExecutionID("exec-markerless-lagging-row")

	// Create through the normal path, then remove the marker: the shape of an
	// execution admitted before explicit durable markers existed.
	replicaA := New(rdb, nil, time.Hour)
	replicaA.transient = false
	if err := replicaA.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution on replica A: %v", err)
	}
	deleteTransientMarker(t, rdb, ctx, id)

	lagging := &laggingProjectionStore{}
	replicaB := New(rdb, lagging, time.Hour)
	replicaB.transient = false

	if !replicaB.isTransient(ctx, id) {
		t.Fatal("a markerless execution with no SQL row must fail closed as transient")
	}
	if _, cached := replicaB.execTransient[id]; cached {
		t.Fatal("the SQL-miss verdict was cached; the row may simply not have " +
			"landed yet, and a cached miss pins the wrong verdict forever")
	}
	if lagging.getCalls != 1 {
		t.Fatalf("SQL lookups = %d, want 1", lagging.getCalls)
	}

	// The projection lands.
	lagging.rowPresent = true
	if replicaB.isTransient(ctx, id) {
		t.Fatal("replica B still reads the execution as transient after the SQL " +
			"row appeared; the SQL-miss verdict was pinned")
	}
	if lagging.getCalls != 2 {
		t.Fatalf("SQL lookups after the row appeared = %d, want 2: the verdict "+
			"must be re-derived, not reused", lagging.getCalls)
	}
	if mark, ok := replicaB.execTransient[id]; !ok || !mark.durableConfirmed {
		t.Fatalf("repaired verdict = %+v (ok=%v), want a confirmed durable verdict",
			mark, ok)
	}
}

// TestGlobalTransientModeWritesNoDurableMarker keeps the store-wide mode intact:
// it is resolved from the flag, never from a per-execution marker, and creation
// must not leave a durable marker that would outlive a reconfiguration.
func TestGlobalTransientModeWritesNoDurableMarker(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	ctx := context.Background()
	id := types.ExecutionID("exec-global-transient-mode")

	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	fields, err := rdb.HGetAll(ctx, transientMarkKey(namespace.FromContext(ctx), id)).Result()
	if err != nil {
		t.Fatalf("read mode marker: %v", err)
	}
	if len(fields) != 0 {
		t.Fatalf("global transient mode wrote a per-execution marker %v; the mode "+
			"must stay a property of the store", fields)
	}
	if !state.isTransient(ctx, id) {
		t.Fatal("global transient mode stopped classifying its executions as transient")
	}
}

// TestEntrySeedWritesDurableMarker extends the explicit marker to the seed
// creation path, which never calls CreateExecution: without it, a replica other
// than the admitting one would fall back to SQL for an execution whose row this
// path projects only after the Lua.
func TestEntrySeedWritesDurableMarker(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	ctx := context.Background()
	state := New(rdb, nil, time.Hour)
	state.transient = false

	req := seedRequestFor(t, "entry-seed-durable-marker", "entry-seed-durable-marker-key")
	resp, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("admission state = %q, want accepted", resp.State)
	}

	fields, err := rdb.HGetAll(ctx, transientMarkKey(namespace.FromContext(ctx), resp.ExecutionID)).Result()
	if err != nil {
		t.Fatalf("read mode marker: %v", err)
	}
	if fields["durable"] != "1" {
		t.Fatalf("entry seed wrote mode marker %v, want durable=1", fields)
	}
	if _, isTransient := fields["transient"]; isTransient {
		t.Fatalf("durable entry seed carries the transient field: %v", fields)
	}
}

// TestSeedExecutionWritesDurableMarker covers the rerun seed path, the third
// creation path that never calls CreateExecution.
func TestSeedExecutionWritesDurableMarker(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	ctx := context.Background()
	state := New(rdb, nil, time.Hour)
	state.transient = false

	id := types.ExecutionID("exec-seed-durable-marker")
	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{
			ID:     id,
			Status: types.ExecutionStatusRunning,
			Graph:  testDurableGraph(),
		},
	}); err != nil {
		t.Fatalf("SeedExecution: %v", err)
	}

	fields, err := rdb.HGetAll(ctx, transientMarkKey(namespace.FromContext(ctx), id)).Result()
	if err != nil {
		t.Fatalf("read mode marker: %v", err)
	}
	if fields["durable"] != "1" {
		t.Fatalf("rerun seed wrote mode marker %v, want durable=1", fields)
	}
	if _, isTransient := fields["transient"]; isTransient {
		t.Fatalf("durable rerun seed carries the transient field: %v", fields)
	}
}
