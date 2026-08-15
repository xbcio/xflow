package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// fakeNodeStore records the node rows a Store would project into SQL. It keeps
// the serialized Output because that column (xflow_nodes.output) is where a
// node's payload lands -- for a traffic-collection workflow, the raw request and
// response bodies, credentials included.
type fakeNodeStore struct {
	store.Store
	nodes []*store.NodeRecord
}

func (f *fakeNodeStore) CreateExecution(_ context.Context, _ *store.ExecutionRecord) error {
	return nil
}

func (f *fakeNodeStore) UpsertNode(_ context.Context, rec *store.NodeRecord) error {
	f.nodes = append(f.nodes, rec)
	return nil
}

// TestPerWorkflowTransient_SkipsNodeOutputProjection is the load-bearing
// assertion for per-workflow transient mode: a workflow that declared itself
// transient must leave NO node output in SQL.
//
// Skipping the audit mirror is not enough. The audit trail is a side record;
// xflow_nodes.output is the payload itself. A workflow carries raw traffic
// through its nodes precisely because the cleansing rules drop whole messages
// rather than redact fields -- a surviving message's body is byte-for-byte
// intact. If that row is written, the credentials the tagging rules exist to
// find are persisted in the platform database, which is the entire reason
// transient mode is a prerequisite for connecting real traffic.
//
// The durable execution in the same Store is the positive control: it proves
// the projection path is live and that a skip is a decision, not a no-op.
func TestPerWorkflowTransient_SkipsNodeOutputProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	// Global transient is OFF: the control plane hosts durable workflows too,
	// which is why the per-workflow switch has to exist at all.
	state.transient = false

	ctx := context.Background()

	transientID := types.ExecutionID("exec-transient-node")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     transientID,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	durableID := types.ExecutionID("exec-durable-node")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     durableID,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	// The secret stands in for what a real payload carries through this node.
	const secret = "ghp_000000000000000000000000000000000000"
	for _, id := range []types.ExecutionID{transientID, durableID} {
		if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID: id,
			Name:        "start",
			Status:      types.NodeStatusSuccess,
			Output:      map[string]any{"authorization": "Bearer " + secret},
		}); err != nil {
			t.Fatalf("UpsertNode (%s): %v", id, err)
		}
	}

	var transientRows, durableRows int
	for _, rec := range fakeDB.nodes {
		switch rec.ExecutionID {
		case transientID:
			transientRows++
		case durableID:
			durableRows++
		}
		// Scan the value, not the key: a key-absence check would pass while the
		// secret rode along inside a nested copy of the payload.
		if strings.Contains(string(rec.Output), secret) && rec.ExecutionID == transientID {
			t.Errorf("transient execution's node output was projected into SQL "+
				"carrying the payload verbatim: %s", rec.Output)
		}
	}

	if durableRows != 1 {
		t.Fatalf("durable node rows = %d, want 1 -- the positive control did not "+
			"reach the projection path, so the transient assertion proves nothing",
			durableRows)
	}
	if transientRows != 0 {
		t.Fatalf("transient node rows = %d, want 0 -- a per-workflow transient "+
			"execution persisted node output to SQL", transientRows)
	}
}

// TestPerWorkflowTransient_SkipsLeaseNodeProjection covers the lease path,
// which the test above does not reach.
//
// Four sites project into xflow_nodes (state_node, state_commit, state_suspend
// and this one), and each carries its own isTransient guard -- an invariant
// maintained by repetition, so a single omission disables it silently. This one
// was omitted: AcquireTaskLease gated only on `s.db != nil`.
//
// The row it writes carries no Output, but calling it harmless misses how the
// table works. xflow_nodes is unique on (execution_id, node_name), so the row
// created here is the row a later commit updates in place -- the commit-side
// guard cannot withdraw a record this side already opened. And lease_id,
// lease_token, attempt and the node name of an execution declared ephemeral
// outliving the Redis TTL is itself the opposite of what transient promises.
func TestPerWorkflowTransient_SkipsLeaseNodeProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false // global transient off; only the workflow declares it

	ctx := context.Background()

	transientID := types.ExecutionID("exec-transient-lease")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID: transientID, Status: types.ExecutionStatusRunning, Graph: tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	durableID := types.ExecutionID("exec-durable-lease")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: durableID, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	for i, id := range []types.ExecutionID{transientID, durableID} {
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID("L" + string(rune('0'+i))),
			LeaseToken: engine.LeaseToken("T" + string(rune('0'+i))),
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID: id, NodeName: "start", NodeIdx: 0,
				Type: engine.TaskTypeNodeExec, ActivationID: 1,
			},
		}
		_, acquired, err := state.AcquireTaskLease(ctx, lease)
		if err != nil || !acquired {
			t.Fatalf("AcquireTaskLease (%s): acquired=%v err=%v", id, acquired, err)
		}
	}

	var transientRows, durableRows int
	for _, rec := range fakeDB.nodes {
		switch rec.ExecutionID {
		case transientID:
			transientRows++
		case durableID:
			durableRows++
		}
	}
	if durableRows != 1 {
		t.Fatalf("durable lease rows = %d, want 1 -- the positive control never "+
			"reached the projection, so the transient assertion below would pass "+
			"even with the projection deleted entirely", durableRows)
	}
	if transientRows != 0 {
		t.Fatalf("transient lease rows = %d, want 0 -- AcquireTaskLease projected "+
			"a node row for an execution the workflow declared ephemeral, opening "+
			"the (execution_id, node_name) record that a later commit fills with "+
			"the node's payload", transientRows)
	}
}
