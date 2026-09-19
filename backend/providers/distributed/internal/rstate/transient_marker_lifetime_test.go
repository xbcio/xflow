package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestTransientMarkerOutlivesTheExecutionItDescribes pins the invariant that
// makes isTransient trustworthy for a long-running execution.
//
// The marker is what makes a transient execution read as transient on every
// replica, and it decides whether node output may be projected into SQL. It is
// written once at CreateExecution, while the node keys are re-EXPIREd by every
// commit -- so without a refresh the marker lapses first and isTransient
// answers "durable" for the most dangerous case there is: an execution still
// running long enough to have output worth leaking.
//
// Measured on a real deployment before the fix: 187 rows in xflow_nodes with no
// matching row in xflow_executions, i.e. raw traffic persisted from transient
// executions that had outlived their markers.
//
// The test advances the clock past the marker's original deadline rather than
// asserting a TTL number, so it fails if the refresh is dropped however the
// refresh is implemented.
func TestTransientMarkerOutlivesTheExecutionItDescribes(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-marker-refresh")
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
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	marker := transientMarkKey(namespace.FromContext(tctx), id)
	if mr.Exists(marker) == false {
		t.Fatalf("no transient marker at %s after CreateExecution", marker)
	}

	// The lease must carry the same ID/token the commit will present, or the
	// script returns StaleToken before reaching the refresh and the test would
	// pass for the wrong reason.
	lease := &engine.TaskLease{
		LeaseID:    "lease",
		LeaseToken: "token",
		Task:       engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}

	// Commit while the marker is still alive, most of the way through its TTL.
	// This is the ordinary long-run shape: the execution keeps working, so it
	// keeps committing.
	mr.FastForward(tg.TransientTTL() - 10*time.Second)
	if err := commitStartNode(ctx, state, id); err != nil {
		t.Fatalf("CommitNode: %v", err)
	}

	// Now step past the deadline the marker ORIGINALLY had. Only a refresh
	// performed by the commit keeps it alive here.
	mr.FastForward(20 * time.Second)

	if !mr.Exists(marker) {
		t.Fatalf("the transient marker at %s expired while the execution was "+
			"still committing; isTransient now answers \"durable\" and this "+
			"execution's node output would be projected into SQL", marker)
	}
	if !state.isTransient(namespace.WithNamespace(context.Background(), namespace.FromContext(tctx)), id) {
		t.Fatal("isTransient returned false for a still-running transient execution; " +
			"the marker did not survive, so the payload would be projected")
	}
}

// TestDurableExecutionGetsNoMarker is the negative control: the refresh is
// EXISTS-guarded, so a durable execution must not acquire a transient marker
// through this path. If it did, the execution would silently stop projecting
// its node output and its audit trail would go dark.
func TestDurableExecutionGetsNoMarker(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-durable-no-marker")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	// The lease must carry the same ID/token the commit will present, or the
	// script returns StaleToken before reaching the refresh and the test would
	// pass for the wrong reason.
	lease := &engine.TaskLease{
		LeaseID:    "lease",
		LeaseToken: "token",
		Task:       engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	if err := commitStartNode(ctx, state, id); err != nil {
		t.Fatalf("CommitNode: %v", err)
	}

	marker := transientMarkKey(namespace.FromContext(ctx), id)
	if mr.Exists(marker) {
		t.Fatalf("a durable execution acquired a transient marker at %s; it would "+
			"stop projecting node output and its audit trail would go dark", marker)
	}
	if state.isTransient(ctx, id) {
		t.Fatal("isTransient returned true for a durable execution")
	}
}

func commitStartNode(ctx context.Context, state *Store, id types.ExecutionID) error {
	advance := &engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeAdvance}
	_, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     0,
		LeaseID:     "lease",
		LeaseToken:  "token",
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"ok": true},
		StoreOutput: true,
		Port:        "main",
		AdvanceTask: advance,
	})
	return err
}
