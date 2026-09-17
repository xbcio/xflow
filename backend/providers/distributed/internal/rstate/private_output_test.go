package rstate

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestRedisPrivateCommitKeepsRuntimeOutputAndMarksSnapshot(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("redis-private-commit")
	g := privateRedisOutputGraph(t, "secret")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	idx, ok := g.NodeIndex("secret")
	if !ok {
		t.Fatal("secret node missing from graph")
	}
	lease := privateRedisLease(id, "secret", idx, "lease-secret", "token-secret")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	secret := map[string]any{"token": "do-not-project"}
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:   id,
		NodeName:      "secret",
		NodeIdx:       idx,
		ActivationID:  0,
		LeaseID:       lease.LeaseID,
		LeaseToken:    lease.LeaseToken,
		Attempt:       lease.Attempt,
		Status:        types.NodeStatusSuccess,
		Output:        secret,
		StoreOutput:   true,
		PrivateOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !result.Applied || !result.ExecutionDone {
		t.Fatalf("CommitNode() = %+v, want applied terminal commit", result)
	}

	assertRedisRuntimeOutput(t, state, ctx, id, "secret", secret)
	assertRedisPrivateMarker(t, rdb, ctx, id, "secret")
	node, err := state.GetNode(ctx, id, "secret")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || !node.PrivateOutput {
		t.Fatalf("GetNode() = %+v, want private marker", node)
	}
	if node.Output != nil {
		t.Fatalf("GetNode().Output = %#v, want nil", node.Output)
	}
}

func TestRedisPrivateMarkerIsMonotonicAndRenewsWithOutput(t *testing.T) {
	state, mr, rdb := newTestRedisState(t)
	state.execTTL = 2 * time.Second
	ctx := context.Background()
	id := types.ExecutionID("redis-private-marker-ttl")

	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   id,
		Name:          "secret",
		Status:        types.NodeStatusPending,
		Output:        map[string]any{"version": "private"},
		PrivateOutput: true,
	}); err != nil {
		t.Fatalf("UpsertNode(private) error = %v", err)
	}
	assertRedisPrivateMarker(t, rdb, ctx, id, "secret")

	// A later snapshot with a false/zero privacy flag refreshes the runtime
	// output but must neither clear nor let the older private marker expire.
	mr.FastForward(1500 * time.Millisecond)
	publicUpdate := map[string]any{"version": "refresh"}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "secret",
		Status:      types.NodeStatusPending,
		Output:      publicUpdate,
	}); err != nil {
		t.Fatalf("UpsertNode(public) error = %v", err)
	}
	mr.FastForward(750 * time.Millisecond)
	assertRedisPrivateMarker(t, rdb, ctx, id, "secret")
	assertRedisRuntimeOutput(t, state, ctx, id, "secret", publicUpdate)

	// Generic runtime writes do not carry a policy bit, but they can extend an
	// existing output's lifetime. Their write must extend the private marker too.
	mr.FastForward(500 * time.Millisecond)
	putUpdate := map[string]any{"version": "put-output"}
	if err := state.PutOutput(ctx, id, "secret", putUpdate); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}
	mr.FastForward(1250 * time.Millisecond)
	assertRedisPrivateMarker(t, rdb, ctx, id, "secret")
	assertRedisRuntimeOutput(t, state, ctx, id, "secret", putUpdate)
}

func TestRedisPrivateSuspendKeepsRuntimeOutputAndMarksSnapshot(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("redis-private-suspend")
	g := privateRedisOutputGraph(t, "wait")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	lease := privateRedisLease(id, "wait", 0, "lease-wait", "token-wait")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v, want true/nil", claimed, err)
	}

	secret := map[string]any{"approval": "sensitive"}
	if _, committed, err := state.SuspendTaskLease(ctx, lease, secret, true, true,
		&types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, ""); err != nil || !committed {
		t.Fatalf("SuspendTaskLease() committed=%v err=%v, want true/nil", committed, err)
	}
	assertRedisRuntimeOutput(t, state, ctx, id, "wait", secret)
	assertRedisPrivateMarker(t, rdb, ctx, id, "wait")
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusSuspended || !node.PrivateOutput || node.Output != nil {
		t.Fatalf("GetNode() = %+v, want private suspended snapshot without output", node)
	}
}

func TestRedisPrivateEntryExitKeepsRuntimeOutputAndReturnsSyntheticSnapshot(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	g := privateRedisOutputGraph(t, "entry")
	secret := map[string]any{"credential": "entry-only"}
	exits := []engine.BoundaryExit{{
		NodeName: "entry", Port: "main", Data: secret, PrivateOutput: true,
	}}
	response, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "redis-private-entry-exit",
		WorkflowID:      "private-output-workflow",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		EntryUnitIdx:    0,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry() error = %v", err)
	}
	if response.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry() state = %q, want accepted", response.State)
	}

	assertRedisRuntimeOutput(t, state, ctx, response.ExecutionID, "entry", secret)
	assertRedisPrivateMarker(t, rdb, ctx, response.ExecutionID, "entry")
	node, err := state.GetNode(ctx, response.ExecutionID, "entry")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || !node.PrivateOutput || node.Output != nil {
		t.Fatalf("GetNode() = %+v, want synthetic private snapshot without output", node)
	}
}

func TestRedisPrivateCommitRedactsSQLProjection(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()
	id := types.ExecutionID("redis-private-sql-projection")
	g := privateRedisOutputGraph(t, "secret")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	lease := privateRedisLease(id, "secret", 0, "lease-sql", "token-sql")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	secret := map[string]any{"token": "not-for-sql"}
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:   id,
		NodeName:      "secret",
		NodeIdx:       0,
		LeaseID:       lease.LeaseID,
		LeaseToken:    lease.LeaseToken,
		Attempt:       lease.Attempt,
		Status:        types.NodeStatusSuccess,
		Output:        secret,
		StoreOutput:   true,
		PrivateOutput: true,
	}); err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	assertRedisRuntimeOutput(t, state, ctx, id, "secret", secret)

	record, err := db.GetNode(ctx, id, "secret")
	if err != nil {
		t.Fatalf("SQL GetNode() error = %v", err)
	}
	if record.Output != nil {
		t.Fatalf("SQL NodeRecord.Output = %s, want nil", record.Output)
	}
}

func privateRedisOutputGraph(t *testing.T, nodeName string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "redis-private-output",
		Nodes: []types.NodeDef{{
			Name:   nodeName,
			Type:   "test.secret",
			Output: &types.NodeOutputPolicy{Private: true},
		}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func privateRedisLease(id types.ExecutionID, name string, idx int, leaseID engine.LeaseID, token engine.LeaseToken) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID: leaseID, LeaseToken: token, IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: name, NodeIdx: idx, Type: engine.TaskTypeNodeExec},
	}
}

func assertRedisPrivateMarker(t *testing.T, rdb interface {
	HGet(context.Context, string, string) *redis.StringCmd
}, ctx context.Context, id types.ExecutionID, name string) {
	t.Helper()
	marker, err := rdb.HGet(ctx, nodeMetaKey(namespace.FromContext(ctx), id, name), "private_output").Result()
	if err != nil || marker != "1" {
		t.Fatalf("private marker = %q, %v; want 1, nil", marker, err)
	}
}

func assertRedisRuntimeOutput(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, name string, want map[string]any) {
	t.Helper()
	got, err := state.GetOutput(ctx, id, name)
	if err != nil {
		t.Fatalf("GetOutput(%q) error = %v", name, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetOutput(%q) = %#v, want %#v", name, got, want)
	}
}
