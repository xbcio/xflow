package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newMiniredisStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, *Store) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return srv, rdb, New(rdb, nil, time.Minute)
}

func sweepGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

func sweepAdmission(g *graph.Graph, limit int) engine.ConcurrencyAdmission {
	return engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     limit,
		Mode:      types.ConcurrencyModeReject,
	}
}

// admitForSweep charges a slot through the real admission path and returns the
// execution's ID, so the sweep cases exercise the same state production writes.
func admitForSweep(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, limit int) types.ExecutionID {
	t.Helper()
	id := engine.NewExecutionID()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	entries := []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}}
	if _, err := state.CreateExecutionWithConcurrency(ctx, snap, entries, sweepAdmission(g, limit)); err != nil {
		t.Fatalf("CreateExecutionWithConcurrency(%q) error = %v", id, err)
	}
	return id
}

// TestSweepConcurrencySlotsRemovesLeakedCharge pins the leak the sweep exists
// for: a slot charged for an execution that never landed. Without the sweep the
// workflow runs one short of its limit forever, and nothing in the engine can
// see it — the member is a real execution ID, it just names nothing.
func TestSweepConcurrencySlotsRemovesLeakedCharge(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-leak")
	tag := workflowDigestTag(namespace.Default, g.Name())
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	// Charge a slot by hand, exactly as admission would, and never create the
	// execution. The score is old so the sweep treats it as past the create
	// window rather than as a submission in flight.
	leaked := types.ExecutionID("leaked-execution")
	old := time.Now().UTC().Add(-10 * time.Minute).Unix()
	if err := workflowAdmitLua.Run(ctx, rdb, []string{slotKey, concurrencyWorkflowIndexKey(namespace.Default)},
		concurrencySlotMember(leaked), 1, old, tag, 3600).Err(); err != nil {
		t.Fatalf("admit leaked slot error = %v", err)
	}

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 1 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want 1", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size after sweep = %d (err %v), want 0 entries", n, err)
	}
	// The drained set and its index entry go together: an index that keeps
	// naming a deleted set sends every later sweep to a key that does not exist.
	if exists, err := rdb.Exists(ctx, slotKey).Result(); err != nil || exists != 0 {
		t.Fatalf("slot key after sweep exists = %d (err %v), want deleted", exists, err)
	}
	if n, err := rdb.SCard(ctx, concurrencyWorkflowIndexKey(namespace.Default)).Result(); err != nil || n != 0 {
		t.Fatalf("workflow index size after sweep = %d (err %v), want 0", n, err)
	}
}

// TestSweepConcurrencySlotsKeepsLiveHolder is the safety half: a slot whose
// execution is still running must survive the sweep. A sweep that removed live
// slots would raise the workflow's effective limit above the declared one.
func TestSweepConcurrencySlotsKeepsLiveHolder(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-live")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := admitForSweep(t, ctx, state, g, 1)

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 0 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want the live slot kept", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size = %d (err %v), want the live holder", n, err)
	}
	if _, err := rdb.Get(ctx, concurrencyTagKey(namespace.Default, id)).Result(); err != nil {
		t.Fatalf("concurrency back-reference for %q error = %v, want it recorded", id, err)
	}
}

// TestSweepConcurrencySlotsRemovesLostRelease covers the other leak: a terminal
// execution whose release never landed. Unlike the phantom-charge case the
// execution record is real and says terminal, so the sweep does not have to
// wait out the create window to be sure.
//
// The state is built by hand rather than through UpdateExecutionStatus: the
// store's own terminal writer releases the slot, so driving the writer would
// test the writer instead of the sweep. What a lost release leaves behind is
// exactly this — a terminal status key and a slot member pointing at it.
func TestSweepConcurrencySlotsRemovesLostRelease(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-terminal")
	tag := workflowDigestTag(namespace.Default, g.Name())
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := engine.NewExecutionID()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := rdb.Set(ctx, execKey(namespace.Default, id, "status"),
		string(types.ExecutionStatusCanceled), 0).Err(); err != nil {
		t.Fatalf("write terminal status error = %v", err)
	}
	if err := workflowAdmitLua.Run(ctx, rdb,
		[]string{slotKey, concurrencyWorkflowIndexKey(namespace.Default)},
		concurrencySlotMember(id), 1, time.Now().UTC().Unix(), tag, 3600).Err(); err != nil {
		t.Fatalf("charge lost-release slot error = %v", err)
	}

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 1 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want the terminal member removed", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size = %d (err %v), want empty", n, err)
	}
}

// TestSweepConcurrencySlotsKeepsFreshPhantom pins the grace window: a member
// whose execution is not there YET is the normal state of a submission between
// charging its slot and writing the execution. Sweeping it would release live
// slots under load, which is a worse failure than the leak it repairs.
func TestSweepConcurrencySlotsKeepsFreshPhantom(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-grace")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())
	tag := workflowDigestTag(namespace.Default, g.Name())

	fresh := types.ExecutionID("fresh-execution")
	if err := workflowAdmitLua.Run(ctx, rdb, []string{slotKey, concurrencyWorkflowIndexKey(namespace.Default)},
		concurrencySlotMember(fresh), 1, time.Now().UTC().Unix(), tag, 3600).Err(); err != nil {
		t.Fatalf("admit fresh slot error = %v", err)
	}

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 0 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want a within-grace member kept", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size = %d (err %v), want the fresh member kept", n, err)
	}
}

// TestSweepConcurrencySlotsStopsAtLimit keeps the pass bounded: the sweeper
// calls it from a maintenance loop, and an unbounded sweep would turn one
// workflow's backlog into a stall for every namespace behind it.
func TestSweepConcurrencySlotsStopsAtLimit(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-limit")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())
	tag := workflowDigestTag(namespace.Default, g.Name())
	old := time.Now().UTC().Add(-10 * time.Minute).Unix()

	for i := 0; i < 5; i++ {
		id := types.ExecutionID("leaked-" + string(rune('a'+i)))
		if err := workflowAdmitLua.Run(ctx, rdb, []string{slotKey, concurrencyWorkflowIndexKey(namespace.Default)},
			concurrencySlotMember(id), 8, old, tag, 3600).Err(); err != nil {
			t.Fatalf("admit leaked %d error = %v", i, err)
		}
	}

	swept, err := state.ReconcileConcurrencySlots(ctx, 2)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 2 {
		t.Fatalf("ReconcileConcurrencySlots(limit 2) swept = %d, want 2", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 3 {
		t.Fatalf("slot ZSET size = %d (err %v), want 3 left for the next pass", n, err)
	}
}

// TestReleaseWorkflowSlotIsIdempotent pins the property every terminal writer
// depends on: calling the release twice, or on an execution that never held a
// slot, must release nothing and report no error.
func TestReleaseWorkflowSlotIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-release-idempotent")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := admitForSweep(t, ctx, state, g, 1)
	if err := state.releaseWorkflowSlot(ctx, id, namespace.Default, ""); err != nil {
		t.Fatalf("first release error = %v", err)
	}
	if err := state.releaseWorkflowSlot(ctx, id, namespace.Default, ""); err != nil {
		t.Fatalf("second release error = %v", err)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size = %d (err %v), want empty", n, err)
	}

	// An execution that never held a slot: no back-reference, nothing to free.
	unlimited := engine.NewExecutionID()
	if err := state.releaseWorkflowSlot(ctx, unlimited, namespace.Default, ""); err != nil {
		t.Fatalf("release for a slotless execution error = %v, want nil", err)
	}
}
