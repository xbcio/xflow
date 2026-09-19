package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// goneExecutionStore reports store.ErrNotFound for every status projection,
// which is what a MySQL row that was never created (transient) or has already
// gone produces.
type goneExecutionStore struct {
	store.Store
	calls int
}

func (g *goneExecutionStore) CreateExecution(_ context.Context, _ *store.ExecutionRecord) error {
	return nil
}

func (g *goneExecutionStore) UpsertNode(_ context.Context, _ *store.NodeRecord) error { return nil }

func (g *goneExecutionStore) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, _ types.ExecutionStatus, _ string) error {
	g.calls++
	return fmt.Errorf("update execution status %q: %w", id, store.ErrNotFound)
}

// TestProjectionForGoneExecutionIsNotAnAuditFailure pins the classification of
// a benign divergence.
//
// A transient execution whose marker expired (transientTTL bounds run time, and
// a run that outlives it takes the marker with it) makes isTransient read
// "marker absent" as "durable", so the projection runs against a row a
// transient execution never created. The write correctly fails; counting that
// as an audit failure is what is wrong, because the audit trail is best-effort
// by contract and at one ERROR per occurrence a healthy pipeline reads as a
// broken store. A durable execution that Redis has genuinely forgotten is the
// same shape and equally unprojectable.
func TestProjectionForGoneExecutionIsNotAnAuditFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &goneExecutionStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false

	observer := newRecordingObserver()
	state.SetAuditObserver(observer)

	ctx := namespace.WithNamespace(context.Background(), "default")
	id := types.ExecutionID("exec-gone")

	// No Redis state at all for this execution: the marker and :status are both
	// absent, exactly as they are once the transient TTL has lapsed.
	state.projectExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "")

	if fakeDB.calls != 1 {
		t.Fatalf("UpdateExecutionStatus called %d times, want 1: the projection "+
			"must still attempt the write, and only its classification is under test",
			fakeDB.calls)
	}
	if n := observer.failed["update_execution_status"]; n != 0 {
		t.Fatalf("audit failures = %d, want 0: a projection for an execution Redis "+
			"no longer holds is a declined write, not a store fault", n)
	}
	if n := observer.ok["update_execution_status"]; n != 1 {
		t.Fatalf("audit successes = %d, want 1", n)
	}
}

// TestProjectionStillFailsWhenTheExecutionOutlivedTheWrite is the guard against
// over-applying the skip: with the execution still present in Redis, a
// not-found write is a real divergence and must keep being reported.
func TestProjectionStillFailsWhenTheExecutionOutlivedTheWrite(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &goneExecutionStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false

	observer := newRecordingObserver()
	state.SetAuditObserver(observer)

	ctx := namespace.WithNamespace(context.Background(), "default")
	id := types.ExecutionID("exec-present")
	// :status present is what "this execution still exists" means; it is
	// written at CreateExecution and re-EXPIREd by every committed mutation.
	if err := rdb.Set(ctx, execKey("default", id, "status"), "running", time.Hour).Err(); err != nil {
		t.Fatalf("seed status key: %v", err)
	}

	state.projectExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "")

	if n := observer.failed["update_execution_status"]; n != 1 {
		t.Fatalf("audit failures = %d, want 1: the execution is still in Redis, so a "+
			"missing audit row is a genuine divergence and must stay visible", n)
	}
}
