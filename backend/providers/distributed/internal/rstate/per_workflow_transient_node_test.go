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
