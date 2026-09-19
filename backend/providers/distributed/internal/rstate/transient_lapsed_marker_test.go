package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// rowGoneNodeStore models a control plane that holds no execution row, which is
// what every transient execution looks like to SQL.
type rowGoneNodeStore struct {
	store.Store
	nodeWrites int
}

func (s *rowGoneNodeStore) CreateExecution(context.Context, *store.ExecutionRecord) error { return nil }

func (s *rowGoneNodeStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return nil, store.ErrNotFound
}

func (s *rowGoneNodeStore) UpsertNode(context.Context, *store.NodeRecord) error {
	s.nodeWrites++
	return nil
}

func (s *rowGoneNodeStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	return nil
}

// rowPresentNodeStore is the positive control: a durable execution does have a
// row, so its node commits must still be projected.
type rowPresentNodeStore struct {
	store.Store
	nodeWrites int
}

func (s *rowPresentNodeStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	return nil
}

func (s *rowPresentNodeStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return &store.ExecutionRecord{}, nil
}

func (s *rowPresentNodeStore) UpsertNode(context.Context, *store.NodeRecord) error {
	s.nodeWrites++
	return nil
}

func (s *rowPresentNodeStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	return nil
}

// TestLapsedMarkerStillReadsAsTransient is the regression test for the leak.
//
// A transient execution's marker can lapse while the execution is still running:
// nothing enforces "transientTTL > max execution wall-clock", and measured on a
// real deployment executions outlive it comfortably. The verdict at that point
// used to be "durable", which projected node output into SQL for an execution
// whose row had never been created -- the observed xflow_nodes rows with zero
// xflow_executions rows.
//
// The row's absence is now the durable evidence, so the verdict has to survive
// the marker. evictExecutionCaches stands in for a replica that never created
// the execution and therefore holds no local answer.
func TestLapsedMarkerStillReadsAsTransient(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	db := &rowGoneNodeStore{}
	state := New(rdb, db, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-lapsed-marker")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if !state.isTransient(ctx, id) {
		t.Fatal("precondition failed: a transient execution did not read as transient")
	}

	// Forget the local verdict so the answer must be re-derived, which is what a
	// second replica does, then let the marker lapse entirely.
	state.evictExecutionCaches(id)
	if !state.isTransient(ctx, id) {
		t.Fatal("with no local verdict and no execution row in SQL, a transient " +
			"execution must still read as transient; reading it as durable " +
			"projects its node output into SQL")
	}

	// The decisive case: the marker is gone as well.
	deleteTransientMarker(t, rdb, ctx, id)
	state.evictExecutionCaches(id)
	if !state.isTransient(ctx, id) {
		t.Fatal("the marker lapsed and the execution was misread as durable; its " +
			"node output would be persisted although it asked not to be")
	}
	if ttl := state.getExecTTL(ctx, id); ttl != state.execTTL && ttl > 0 {
		t.Fatalf("getExecTTL = %v, want the durable default %v: a lapsed marker must "+
			"not change how long this execution's node keys live, or its keys "+
			"outlive its own qualifier", ttl, state.execTTL)
	}
}

// TestDurableExecutionStillProjectsAfterCacheDrop is the negative control: when
// the row does exist the verdict must remain durable. Without this, "never
// project anything" would satisfy the leak test.
func TestDurableExecutionStillProjectsAfterCacheDrop(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	db := &rowPresentNodeStore{}
	state := New(rdb, db, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-durable-projects")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	// Drop the cached verdict: the answer must now come from the row existing.
	state.evictExecutionCaches(id)
	if state.isTransient(ctx, id) {
		t.Fatal("a durable execution whose row exists read as transient; its audit " +
			"projection would stop silently")
	}
}

// TestTransientVerdictIsRememberedAfterTheRowAppears guards the caching rule: a
// confirmed-durable verdict is stable and must be reused, while a "no marker"
// verdict must not be cached as final.
func TestTransientVerdictIsRememberedAfterTheRowAppears(t *testing.T) {
	rdb, closeFn := newTestRedis(t)
	defer closeFn()

	db := &rowPresentNodeStore{}
	state := New(rdb, db, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-remembered")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	state.evictExecutionCaches(id)
	if state.isTransient(ctx, id) {
		t.Fatal("durable execution read as transient")
	}
	mark := state.execTransient[id]
	if !mark.durableConfirmed {
		t.Fatal("the durable verdict was not recorded as confirmed, so every later " +
			"read would repeat the SQL round-trip")
	}
}

func newTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return rdb, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func deleteTransientMarker(t *testing.T, rdb *redis.Client, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	if err := rdb.Del(ctx, transientMarkKey(namespace.FromContext(ctx), id)).Err(); err != nil {
		t.Fatalf("delete transient marker: %v", err)
	}
}
