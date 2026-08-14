package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// TestPerWorkflowTransient_MarkVisibleAcrossReplicas pins the requirement that
// the per-execution transient marker lives in Redis, not in a process-local map.
//
// A control plane runs as several replicas against one Redis. The replica that
// admits the execution is not the one that runs every later mutation: the
// outbox dispatcher, the lease sweeper, and the timeout monitor all pick up
// work by scanning Redis, from whichever replica happens to hold the loop. If
// the transient marker only exists on the admitting replica's heap, every other
// replica reads the global flag instead -- which is OFF on a control plane that
// also hosts durable workflows -- and projects the node payload into SQL. That
// is the exact leak per-workflow transient mode exists to prevent, and it
// reappears silently the moment the deployment scales past one replica.
//
// The two Stores here share one Redis, which is what makes them replicas.
func TestPerWorkflowTransient_MarkVisibleAcrossReplicas(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()

	// Replica A admits the execution.
	dbA := &fakeNodeStore{}
	replicaA := New(rdb, dbA, time.Hour)
	replicaA.transient = false

	// Replica B never saw the submission; it only sees Redis.
	dbB := &fakeNodeStore{}
	replicaB := New(rdb, dbB, time.Hour)
	replicaB.transient = false

	execID := types.ExecutionID("exec-cross-replica")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := replicaA.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution on replica A: %v", err)
	}

	if !replicaA.isTransient(ctx, execID) {
		t.Fatalf("replica A does not see its own transient marker -- the probe " +
			"is broken, not the code under test")
	}
	if !replicaB.isTransient(ctx, execID) {
		t.Errorf("replica B does not see the transient marker for %s: the marker "+
			"is process-local, so every replica that did not admit the execution "+
			"treats it as durable", execID)
	}

	const secret = "ghp_000000000000000000000000000000000000"
	if err := replicaB.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: execID,
		Name:        "start",
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"authorization": "Bearer " + secret},
	}); err != nil {
		t.Fatalf("UpsertNode on replica B: %v", err)
	}

	for _, rec := range dbB.nodes {
		// Scan the value, not the key.
		if strings.Contains(string(rec.Output), secret) {
			t.Errorf("replica B projected the transient execution's node output "+
				"into SQL carrying the payload verbatim: %s", rec.Output)
		}
	}
	if len(dbB.nodes) != 0 {
		t.Fatalf("replica B wrote %d node rows for a transient execution, want 0",
			len(dbB.nodes))
	}
}
